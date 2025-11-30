import os
import json
import uuid
import time
import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from confluent_kafka import Consumer


KAFKA_BOOTSTRAP = "kafka:19092"
MINIO_ENDPOINT = "http://minio:9000"

print(f"Using Kafka bootstrap: {KAFKA_BOOTSTRAP}")
print(f"Using MinIO endpoint: {MINIO_ENDPOINT}")


consumer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'group.id': 'bronze-writer',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

consumer = Consumer(consumer_conf)
consumer.subscribe(['raw-data'])


s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id="minio",
    aws_secret_access_key="minio123",
    region_name="us-east-1",
)

bucket_name = "datalake"
bronze_prefix = "bronze/"


def ensure_bucket():
    """Wait for MinIO to be ready."""
    for _ in range(10):
        try:
            s3.head_bucket(Bucket=bucket_name)
            return
        except Exception:
            try:
                s3.create_bucket(Bucket=bucket_name)
                return
            except:
                pass
        print("Waiting for MinIO...")
        time.sleep(1)

ensure_bucket()


print("📡 Reading Kafka messages (will wait up to 30 seconds)...")
records = []
start = time.time()
max_wait_time = 30
min_messages = 1
last_message_time = start

while (time.time() - start < max_wait_time):
    msg = consumer.poll(1.0)
    
    if msg is None:
        if len(records) >= min_messages and (time.time() - last_message_time) > 5:
            print(f"✅ Collected {len(records)} messages, no new messages for 5 seconds. Proceeding...")
            break
        continue
    
    if msg.error():
        print(f"⚠️ Consumer error: {msg.error()}")
        continue

    try:
        value = json.loads(msg.value().decode("utf-8"))
        records.append(value)
        last_message_time = time.time()
        if len(records) % 100 == 0:
            print(f"📊 Collected {len(records)} messages so far...")
    except Exception as e:
        print(f"⚠️ Error parsing message: {e}")
        continue

consumer.close()
print(f"📡 Finished reading. Total messages collected: {len(records)}")

if len(records) == 0:
    print("⚠️ No new messages. Exiting.")
    exit(0)

df = pd.DataFrame(records)
table = pa.Table.from_pandas(df)

ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
file_name = f"{bronze_prefix}transactions_{ts}_{uuid.uuid4().hex}.parquet"

buf = pa.BufferOutputStream()
pq.write_table(table, buf)

s3.put_object(
    Bucket=bucket_name,
    Key=file_name,
    Body=buf.getvalue().to_pybytes()
)

print(f"🟤 Bronze written: {file_name} ({len(records)} records)")
print("Bronze ETL complete.")
