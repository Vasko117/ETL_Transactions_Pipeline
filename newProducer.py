#!/usr/bin/env python3
"""
Real-Time Transaction Producer
Simulates a live transaction source by streaming transactions to Kafka
This demonstrates Kafka's value: decoupling producers from consumers, real-time streaming
"""
import json
import csv
import os
import time
import random
from datetime import datetime, timedelta
from confluent_kafka import Producer, KafkaException

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
if os.path.exists('/opt/airflow'):  
    KAFKA_BOOTSTRAP = 'kafka:19092'

print(f"📡 Connecting to Kafka at {KAFKA_BOOTSTRAP}")

producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'acks': 'all',  # Wait for all replicas
    'retries': 3,
    'compression.type': 'snappy',  # Compress messages for efficiency
    'batch.size': 100,  # Batch messages for better throughput
    'linger.ms': 10,  # Wait up to 10ms to fill batch
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
    """Callback for message delivery confirmation"""
    if err:
        print(f"❌ Delivery failed: {err}")
    # Only log errors to avoid spam - successful deliveries are silent

# Load transaction templates from CSV
csv_path = 'transactions.csv'
if os.path.exists('/opt/airflow/scripts/transactions.csv'):
    csv_path = '/opt/airflow/scripts/transactions.csv'
elif os.path.exists('scripts/transactions.csv'):
    csv_path = 'scripts/transactions.csv'

print(f"📄 Loading transaction templates from: {csv_path}")

# Load all transactions into memory (as templates)
transaction_templates = []
try:
    with open(csv_path, mode='r') as f:
        reader = csv.DictReader(f)
        transaction_templates = list(reader)
    print(f"✅ Loaded {len(transaction_templates)} transaction templates")
except Exception as e:
    print(f"❌ Error loading CSV: {e}")
    exit(1)

# Kafka's value: Stream transactions in real-time (simulating live payment system)
# This decouples the data source from consumers and provides buffering
print("\n🚀 Starting REAL-TIME transaction stream to Kafka...")
print("   This simulates a live payment system sending transactions continuously")
print("   Kafka provides: buffering, decoupling, and real-time processing\n")

# Stream transactions with realistic timing
# In production, this would be a real payment gateway/API
STREAM_DURATION = int(os.getenv('STREAM_DURATION', '60'))  # Stream for 60 seconds by default
MESSAGES_PER_SECOND = float(os.getenv('MESSAGES_PER_SECOND', '5'))  # 5 transactions/second

start_time = time.time()
messages_sent = 0
batch_count = 0

print(f"📊 Streaming configuration:")
print(f"   Duration: {STREAM_DURATION} seconds")
print(f"   Rate: ~{MESSAGES_PER_SECOND} transactions/second")
print(f"   Total: ~{int(STREAM_DURATION * MESSAGES_PER_SECOND)} transactions\n")

try:
    while (time.time() - start_time) < STREAM_DURATION:
        # Select random transaction template
        template = random.choice(transaction_templates)
        
        # Update timestamp to current time (make it "live")
        template['transaction_date'] = datetime.now().isoformat()
        
        # Add some variation to amounts (simulate real transactions)
        if 'amount' in template:
            base_amount = float(template.get('amount', 0) or 0)
            variation = random.uniform(0.9, 1.1)  # ±10% variation
            template['amount'] = str(round(base_amount * variation, 2))
        
        # Produce to Kafka (this is where Kafka shines - async, buffered, decoupled)
        value = json.dumps(template).encode('utf-8')
        p.produce(
            topic, 
            value=value, 
            on_delivery=delivery_report,
            key=template.get('transaction_id', '').encode('utf-8')  # Use transaction_id as key for partitioning
        )
        
        messages_sent += 1
        
        # Poll to trigger delivery callbacks
        p.poll(0)
        
        # Log progress every 50 messages
        if messages_sent % 50 == 0:
            elapsed = time.time() - start_time
            rate = messages_sent / elapsed if elapsed > 0 else 0
            print(f"📊 Streamed {messages_sent} transactions ({rate:.1f} msg/sec, {elapsed:.1f}s elapsed)")
        
        # Realistic timing: transactions arrive at variable intervals
        # Kafka handles buffering and backpressure automatically
        time.sleep(1.0 / MESSAGES_PER_SECOND + random.uniform(-0.1, 0.1))
    
    # Flush any remaining messages
    remaining = p.flush(timeout=10)
    if remaining > 0:
        print(f"⚠️ {remaining} messages not delivered within timeout")
    
    elapsed = time.time() - start_time
    rate = messages_sent / elapsed if elapsed > 0 else 0
    
    print(f"\n✅ Streaming complete!")
    print(f"   Total messages sent: {messages_sent}")
    print(f"   Duration: {elapsed:.1f} seconds")
    print(f"   Average rate: {rate:.1f} messages/second")
    print(f"\n💡 Kafka Value Demonstrated:")
    print(f"   • Decoupling: Producer and consumer run independently")
    print(f"   • Buffering: Messages stored even if consumer is slow")
    print(f"   • Scalability: Multiple consumers can read same data")
    print(f"   • Real-time: Low latency message delivery")
    
except KeyboardInterrupt:
    print("\n⚠️ Streaming interrupted by user")
    p.flush(timeout=5)
except Exception as e:
    print(f"\n❌ Error during streaming: {e}")
    p.flush(timeout=5)
    raise
