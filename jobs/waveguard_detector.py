# Etablir la connexion Kafka --> Spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, sum as spark_sum,
    current_timestamp, lit, to_json, struct
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, BooleanType
)

# Configuration spark
spark = SparkSession.builder \
    .appName('WaveGuard_FraudDetector') \
    .config('spark.jars.packages',
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0') \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# Schema JSON des transactions
schema = StructType([
    StructField('transaction_id', StringType(), True),
    StructField('timestamp', TimestampType(), True),
    StructField('sender_id', StringType(), True),
    StructField('receiver_id', StringType(), True),
    StructField('amount_fcfa', DoubleType(), True),
    StructField('transaction_type', StringType(), True),
    StructField('location', StringType(), True),
    StructField('is_flagged', BooleanType(), True),
])

# Lecture du flux Kafka
raw_stream = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'kafka:9093') \
    .option('subscribe', 'transactions') \
    .option("kafka.group.id", "waveguard_consumer_group") \
    .option('startingOffsets', 'latest') \
    .load()

# Parsing et extraction
parsed_df = raw_stream \
    .selectExpr('CAST(value AS STRING) as json_str', 'timestamp as kafka_ts') \
    .select(from_json(col('json_str'), schema).alias('data'), col('kafka_ts')) \
    .select('data.*', 'kafka_ts')

# Tolerance aux messages en retard
df_with_watermark = parsed_df.withWatermark('timestamp', '2 minutes')

# Détection basée sur la frequence de transactions dans une fenêtre glissante
# > 5 transactions depuis un même compte en 5 minutes (slide: 1 min)
velocity_fraud = df_with_watermark \
    .groupBy(
        # Fenêtre glissante de 5 minutes, glissant toutes les 1 minute
        window(col('timestamp'), '5 minutes', '1 minute'),
        col('sender_id')
    ) \
    .agg(count('*').alias('tx_count')) \
    .filter(col('tx_count') > 5) \
    .select(
        col('sender_id'),
        col('window.start').alias('window_start'),
        col('window.end').alias('window_end'),
        col('tx_count'),
        lit('VELOCITY_FRAUD').alias('fraud_type'),
        current_timestamp().alias('detected_at')
    )

# Détection basée sur le volume 
# > 500 000 FCFA de transactions depuis un même compte en 10 minutes (slide: 2 min)
# La fenêtre de 10 minutes permet de voir l'accumulation d'argent sur un temps long tandis que le "slide" de 2 minutes recalcule cette somme fréquemment pour détecter la fraude le plus vite possible
volume_fraud = df_with_watermark \
    .groupBy(
        window(col('timestamp'), '10 minutes', '2 minutes'),
        col('sender_id')
    ) \
    .agg(spark_sum('amount_fcfa').alias('total_amount')) \
    .filter(col('total_amount') > 500_000) \
    .select(
        col('sender_id'),
        col('window.start').alias('window_start'),
        col('window.end').alias('window_end'),
        col('total_amount'),
        lit('VOLUME_FRAUD').alias('fraud_type'),
        current_timestamp().alias('detected_at')
    )

# SINK 1 : Écriture vers le topic Kafka 'fraud-alerts'
def write_to_kafka(df, fraud_type_label):
    return df \
        .select(to_json(struct('*')).alias('value')) \
        .writeStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'kafka:9093') \
        .option('topic', 'fraud-alerts') \
        .option('checkpointLocation', f'/tmp/waveguard_checkpoint_{fraud_type_label}') \
        .outputMode('update') \
        .start()

# SINK 2 : Écriture dans le Data Lake local (Parquet)
def write_to_datalake(df, fraud_type_label):
    return df \
        .writeStream \
        .format('parquet') \
        .option('path', f'/tmp/waveguard_lake/{fraud_type_label}') \
        .option('checkpointLocation', f'/tmp/waveguard_checkpoint/lake_{fraud_type_label}') \
        .outputMode('append') \
        .trigger(processingTime='30 seconds') \
        .start()

# Lancement des queries
print('=== Lancement des Sinks WaveGuard (Kafka + Data Lake) ===')
q1_kafka = write_to_kafka(velocity_fraud, 'velocity')
q2_kafka = write_to_kafka(volume_fraud, 'volume')

q1_lake = write_to_datalake(velocity_fraud, 'velocity')
q2_lake = write_to_datalake(volume_fraud, 'volume')

# À ajouter pour débugger la consommation
def process_row(batch_df, batch_id):
    print(f"Batch {batch_id} reçu avec {batch_df.count()} lignes")

q_debug = df_with_watermark.writeStream \
    .foreachBatch(process_row) \
    .start()

spark.streams.awaitAnyTermination()