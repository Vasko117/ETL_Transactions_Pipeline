import json
import csv
import os
import time
from confluent_kafka import Producer, KafkaException


KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
if os.path.exists('/opt/airflow'):  
    KAFKA_BOOTSTRAP = 'kafka:19092'

print(f"📡 Connecting to Kafka at {KAFKA_BOOTSTRAP}")

producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'acks': 'all',
    'retries': 3
}

p = Producer(producer_conf)

topic = 'raw-data'
print(f"🔍 Verifying topic '{topic}' exists...")
for i in range(10):
    try:
        metadata = p.list_topics(timeout=5)
        if topic in metadata.topics:
            print(f"✅ Topic '{topic}' is available")
            break
        else:
            print(f"⚠️ Topic '{topic}' not found, waiting... (attempt {i+1}/10)")
            time.sleep(1)
    except Exception as e:
        if i < 9:
            print(f"⏳ Waiting for Kafka connection... (attempt {i+1}/10)")
            time.sleep(1)
        else:
            print(f"⚠️ Could not verify topic, proceeding anyway: {e}")

def delivery_report(err, msg):
    if err:
        print("❌ Delivery failed:", err)
    else:
        print(f"✅ Delivered to topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")


csv_path = 'transactions.csv'
if os.path.exists('/opt/airflow/scripts/transactions.csv'):
    csv_path = '/opt/airflow/scripts/transactions.csv'
elif os.path.exists('scripts/transactions.csv'):
    csv_path = 'scripts/transactions.csv'

print(f"📄 Reading CSV from: {csv_path}")

with open(csv_path, mode='r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        value = json.dumps(row).encode('utf-8')
        p.produce(topic, value=value, on_delivery=delivery_report)
        p.poll(0)

p.flush()
print("🚀 Finished producing all records to Kafka topic:", topic)