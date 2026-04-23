import time, json, os
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('MetricsExporter') \
    .master("local[*]") \
    .getOrCreate()

METRICS_FILE = '/tmp/waveguard_metrics.json'

while True:
    try:
        # Lire les alertes générées
        df_velocity = spark.read.parquet('/tmp/waveguard_lake/velocity')
        df_volume = spark.read.parquet('/tmp/waveguard_lake/volume')
        metrics = {
            'timestamp': time.time(),
            'velocity_alerts': df_velocity.count(),
            'volume_alerts': df_volume.count(),
            'top_fraudster': df_velocity.groupBy('sender_id').count()
                .orderBy('count', ascending=False)
                .first()['sender_id'] if df_velocity.count() > 0 else 'N/A'
        }
        with open(METRICS_FILE, 'w') as f:
            json.dump(metrics, f)
        print(f'Métriques exportées : {metrics}')
    except Exception as e:
        print(f'Erreur : {e}')
    time.sleep(15) 