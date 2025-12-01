#!/usr/bin/env python3
"""
Real-Time Bronze Layer Writer
Consumes from Kafka and writes to MinIO incrementally (true streaming)
This demonstrates Kafka's value: real-time processing, incremental writes, decoupling
"""
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

print(f"📡 Kafka Consumer: {KAFKA_BOOTSTRAP}")
print(f"💾 MinIO Endpoint: {MINIO_ENDPOINT}")

# Kafka Consumer Configuration
# Kafka's value: Consumer groups allow multiple consumers, offset management, parallel processing
consumer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'group.id': 'bronze-writer',  # Consumer group for parallel processing
    'auto.offset.reset': 'earliest',  # Start from beginning if no offset
    'enable.auto.commit': False,  # Manual commit for reliability
    'session.timeout.ms': 30000,
    'max.poll.interval.ms': 300000,
}

consumer = Consumer(consumer_conf)
consumer.subscribe(['raw-data'])

# MinIO S3 Client
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

# Streaming Configuration
BATCH_SIZE = int(os.getenv('BRONZE_BATCH_SIZE', '100'))  # Write file every 100 records
MAX_WAIT_TIME = int(os.getenv('BRONZE_MAX_WAIT', '30'))  # Or after 30 seconds of no messages
MIN_RECORDS_FOR_FILE = 10  # Minimum records before writing a file

print(f"\n🚀 Starting REAL-TIME Kafka consumer (Bronze Writer)")
print(f"   Batch size: {BATCH_SIZE} records per file")
print(f"   Max wait: {MAX_WAIT_TIME} seconds")
print(f"   Kafka provides: real-time processing, incremental writes, decoupling\n")

records = []
batch_start_time = time.time()
files_written = 0
total_records = 0
last_message_time = time.time()

def write_bronze_batch(records_batch, reason=""):
    """Write a batch of records to Bronze layer (incremental write)"""
    global files_written, total_records
    
    if len(records_batch) < MIN_RECORDS_FOR_FILE:
        return False
    
    try:
        df = pd.DataFrame(records_batch)
        table = pa.Table.from_pandas(df)
        
        ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
        file_name = f"{bronze_prefix}transactions_{ts}_{uuid.uuid4().hex[:8]}.parquet"
        
        buf = pa.BufferOutputStream()
        pq.write_table(table, buf)
        
        s3.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=buf.getvalue().to_pybytes()
        )
        
        files_written += 1
        total_records += len(records_batch)
        
        print(f"🟤 Bronze batch written: {file_name} ({len(records_batch)} records) {reason}")
        return True
    except Exception as e:
        print(f"❌ Error writing bronze batch: {e}")
        return False

# Real-time consumption loop
# Kafka's value: Process messages as they arrive, not collect-all-then-process
print("📡 Consuming messages from Kafka (real-time streaming)...\n")

try:
    while True:
        # Poll for messages (non-blocking with timeout)
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            # No message received
            elapsed_since_last = time.time() - last_message_time
            
            # If we have records and haven't received messages for a while, flush
            if len(records) > 0 and elapsed_since_last > MAX_WAIT_TIME:
                print(f"⏰ No messages for {elapsed_since_last:.1f}s, flushing {len(records)} records...")
                write_bronze_batch(records, "(timeout flush)")
                records = []
                batch_start_time = time.time()
            
            # If no records and long wait, check if we should exit
            elif len(records) == 0 and elapsed_since_last > MAX_WAIT_TIME * 2:
                print(f"⏰ No messages for {MAX_WAIT_TIME * 2}s, exiting...")
                break
            
            continue
        
        if msg.error():
            if msg.error().code() == -191:  # PARTITION_EOF (end of partition)
                # This is normal - we've read all available messages
                if len(records) > 0:
                    write_bronze_batch(records, "(partition EOF)")
                    records = []
                print("📭 Reached end of partition, waiting for new messages...")
                continue
            else:
                print(f"⚠️ Consumer error: {msg.error()}")
                continue
        
        # Process message (Kafka provides real-time delivery)
        try:
            value = json.loads(msg.value().decode("utf-8"))
            records.append(value)
            last_message_time = time.time()
            
            # Log progress
            if len(records) % 50 == 0:
                print(f"📊 Buffered {len(records)} records (Kafka offset: {msg.offset()})")
            
            # Write batch when threshold reached (incremental processing)
            if len(records) >= BATCH_SIZE:
                write_bronze_batch(records, f"(batch size: {BATCH_SIZE})")
                records = []
                batch_start_time = time.time()
                
                # Commit offset after successful write
                consumer.commit(asynchronous=False)
                
        except Exception as e:
            print(f"⚠️ Error parsing message: {e}")
            continue

except KeyboardInterrupt:
    print("\n⚠️ Consumer interrupted by user")
except Exception as e:
    print(f"\n❌ Consumer error: {e}")
    import traceback
    traceback.print_exc()
finally:
    # Write any remaining records
    if len(records) > 0:
        write_bronze_batch(records, "(final flush)")
        consumer.commit(asynchronous=False)
    
    consumer.close()
    
    print(f"\n✅ Bronze ETL complete!")
    print(f"   Total files written: {files_written}")
    print(f"   Total records processed: {total_records}")
    print(f"\n💡 Kafka Value Demonstrated:")
    print(f"   • Real-time processing: Messages processed as they arrive")
    print(f"   • Incremental writes: Files written in batches, not all-at-once")
    print(f"   • Decoupling: Producer and consumer run independently")
    print(f"   • Reliability: Offset management ensures no data loss")
