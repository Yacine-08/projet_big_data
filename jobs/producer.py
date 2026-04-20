
# producer.py permet de simuler des transactions Mobile Money WaveGuard
from confluent_kafka import Producer
from faker import Faker
import json, time, random, uuid
from datetime import datetime, timezone

# on utilise le topic "transactions" pour envoyer les données de transaction
fake = Faker('fr_FR')
BROKER = 'localhost:9092'
TOPIC = 'transactions'

# Liste de comptes (certains seront des fraudeurs)
ACCOUNTS = [f'SN_{i:04d}' for i in range(1, 51)]
FRAUD_ACCOUNTS = ['SN_0042', 'SN_0007', 'SN_0013'] # comptes suspects
conf = {'bootstrap.servers': BROKER}
producer = Producer(conf)

# Callback pour confirmer la livraison des messages
def delivery_report(err, msg):
    if err:
        print(f'[ERREUR] Livraison échouée : {err}')
    else:
        print(f'[OK] Topic={msg.topic()} | Partition={msg.partition()} | Offset={msg.offset()}')

# Génère une transaction aléatoire avec une chance de fraude
def generate_transaction(sender_id=None, fraud=False):

    # Si on passe un sender_id, on l'utilise (pour le burst frauduleux), sinon on choisit aléatoirement
    sender = sender_id if sender_id else random.choice(FRAUD_ACCOUNTS if fraud else ACCOUNTS)

    # Montant plus élevé pour les transactions frauduleuses
    amount = random.randint(800_000, 2_000_000) if fraud else random.randint(500, 150_000)

    # les champs de la transaction 
    return {
        'transaction_id': str(uuid.uuid4()),
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'sender_id': sender,
        'receiver_id': random.choice(ACCOUNTS),
        'amount_fcfa': amount,
        'transaction_type': random.choice(['P2P', 'PAIEMENT_MARCHAND', 'RETRAIT']),
        'location': random.choice(['Dakar', 'Thiès', 'Saint-Louis', 'Ziguinchor', 'Kaolack']),
        'is_flagged': fraud
    }

print('=== WaveGuard Producer démarré === Ctrl+C pour arrêter')

try:
    while True:
        # Injection de fraude : 10% de chance d'envoyer une transaction suspecte
        is_fraud = random.random() < 0.10

        # Implementation du burst
        if is_fraud:
            target_account = random.choice(FRAUD_ACCOUNTS)
            print(f"!!! Déclenchement d'un BURST de fraude pour {target_account} !!!")

            for _ in range(8):
                tx = generate_transaction(sender_id=target_account, fraud=True)
                # Envoi en rafale
                producer.produce(
                    TOPIC,
                    # Clé pour le partitionnement
                    key=tx['sender_id'].encode('utf-8'), 
                    value=json.dumps(tx).encode('utf-8'),
                    callback=delivery_report
                )
                # Espacement de 50ms (rafale)
                time.sleep(0.05) 
        else:
            # Transaction normale
            tx = generate_transaction(fraud=False)
            producer.produce(
                TOPIC,
                key=tx['sender_id'].encode('utf-8'), # Clé pour le partitionnement
                value=json.dumps(tx).encode('utf-8'),
                callback=delivery_report
            ) 
        
        producer.poll(0)
        # Espacement entre les transactions 
        time.sleep(random.uniform(0.1, 0.5)) # ~5 à 20 tx/sec
except KeyboardInterrupt:
    print('Arrêt du producer...')
    producer.flush()