# import time, json, os
# from pyspark.sql import SparkSession

# spark = SparkSession.builder \
#     .appName('MetricsExporter') \
#     .master("local[*]") \
#     .getOrCreate()

# METRICS_FILE = '/tmp/waveguard_metrics.json'

# while True:
#     try:
#         # Lire les alertes générées
#         df_velocity = spark.read.parquet('/tmp/waveguard_lake/velocity')
#         df_volume = spark.read.parquet('/tmp/waveguard_lake/volume')
#         metrics = {
#             'timestamp': time.time(),
#             'velocity_alerts': df_velocity.count(),
#             'volume_alerts': df_volume.count(),
#             'top_fraudster': df_velocity.groupBy('sender_id').count()
#                 .orderBy('count', ascending=False)
#                 .first()['sender_id'] if df_velocity.count() > 0 else 'N/A'
#         }
#         with open(METRICS_FILE, 'w') as f:
#             json.dump(metrics, f)
#         print(f'Métriques exportées : {metrics}')
#     except Exception as e:
#         print(f'Erreur : {e}')
#     time.sleep(15) 

import time, json, os, glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, window
from datetime import datetime

spark = SparkSession.builder \
    .appName('MetricsExporter') \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

METRICS_FILE = '/tmp/waveguard_metrics.json'
HISTORY_FILE = '/tmp/waveguard_history.json'

# Charger l'historique existant
def load_history():
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'r') as f:
            return json.load(f)
    return []

while True:
    try:
        df_velocity = spark.read.parquet('/tmp/waveguard_lake/velocity')
        df_volume = spark.read.parquet('/tmp/waveguard_lake/volume')

        velocity_count = df_velocity.count()
        volume_count = df_volume.count()

        top_fraudster = 'N/A'
        if velocity_count > 0:
            top_fraudster = df_velocity.groupBy('sender_id').count() \
                .orderBy('count', ascending=False) \
                .first()['sender_id']

        # Compter les transactions de la dernière minute depuis le datalake
        tx_per_min = velocity_count + volume_count 

        now = time.time()

        metrics = {
            'timestamp': now,
            'velocity_alerts': velocity_count,
            'volume_alerts': volume_count,
            'top_fraudster': top_fraudster,
            'tx_per_min': tx_per_min
        }
        with open(METRICS_FILE, 'w') as f:
            json.dump(metrics, f)

        # Historique pour Time Series
        history = load_history()
        history.append({
            'timestamp': now,
            'timestamp_iso': datetime.utcfromtimestamp(now).isoformat() + 'Z',
            'velocity_alerts': velocity_count,
            'volume_alerts': volume_count,
            'tx_per_min': tx_per_min
        })
        # Garder seulement les 60 derniers points (15 minutes)
        history = history[-60:]
        with open(HISTORY_FILE, 'w') as f:
            json.dump(history, f)

        print(f'[{datetime.utcnow().isoformat()}] Métriques exportées : {metrics}')

    except Exception as e:
        print(f'Erreur : {e}')

    time.sleep(15)