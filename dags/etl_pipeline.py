"""
Real-Time ETL Pipeline DAG
Demonstrates Kafka's value in a streaming data pipeline:
1. Create Kafka topic
2. Stream transactions to Kafka (real-time simulation)
3. Bronze: Consume from Kafka incrementally and write to MinIO (true streaming)
4. Silver: Transform Bronze data
5. Gold: Transform Silver data into Data Warehouse

Kafka provides: Decoupling, buffering, real-time processing, scalability
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os

def create_kafka_topic_if_not_exists():
    """Create Kafka topic if it doesn't exist"""
    import subprocess
    import time
    
    kafka_host = 'kafka' if os.path.exists('/opt/airflow') else 'localhost'
    kafka_port = '19092' if os.path.exists('/opt/airflow') else '9092'
    
    print(f"🔍 Checking if Kafka topic 'raw-data' exists...")
    
    try:
        from confluent_kafka.admin import AdminClient, NewTopic
        
        bootstrap_servers = f'{kafka_host}:{kafka_port}'
        print(f"📡 Connecting to Kafka at {bootstrap_servers}")
        
        admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
        
        for i in range(10):
            try:
                metadata = admin_client.list_topics(timeout=5)
                print(f"✅ Connected to Kafka")
                break
            except Exception as e:
                if i < 9:
                    print(f"⏳ Waiting for Kafka to be ready... (attempt {i+1}/10)")
                    time.sleep(2)
                else:
                    raise
        
        try:
            metadata = admin_client.list_topics(timeout=10)
            if 'raw-data' in metadata.topics:
                print(f"ℹ️ Topic 'raw-data' already exists")
                return
        except:
            pass
        
        topic_list = [NewTopic('raw-data', num_partitions=1, replication_factor=1)]
        futures = admin_client.create_topics(topic_list)
        
        for topic, future in futures.items():
            try:
                future.result(timeout=10) 
                print(f"✅ Topic '{topic}' created successfully")
            except Exception as e:
                error_str = str(e).lower()
                if "already exists" in error_str or "topicexists" in error_str:
                    print(f"ℹ️ Topic '{topic}' already exists")
                else:
                    print(f"⚠️ Error creating topic '{topic}': {e}")
                    raise
    except Exception as e:
        print(f"⚠️ Could not create topic using AdminClient: {e}")
        print(f"🔄 Falling back to kafka-topics.sh command...")
        
        try:
            result = subprocess.run(
                ['docker', 'ps', '--filter', 'name=kafka', '--format', '{{.Names}}'],
                capture_output=True,
                text=True,
                timeout=5
            )
            kafka_container = result.stdout.strip()
            
            if kafka_container:
                print(f"📦 Found Kafka container: {kafka_container}")
                cmd = [
                    'docker', 'exec', kafka_container,
                    'kafka-topics.sh', '--create',
                    '--topic', 'raw-data',
                    '--bootstrap-server', 'localhost:9092',
                    '--partitions', '1',
                    '--replication-factor', '1',
                    '--if-not-exists'
                ]
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
                if result.returncode == 0:
                    print(f"✅ Topic 'raw-data' created using kafka-topics.sh")
                else:
                    if "already exists" in result.stderr.lower():
                        print(f"ℹ️ Topic 'raw-data' already exists")
                    else:
                        print(f"⚠️ Error: {result.stderr}")
                        raise Exception(result.stderr)
            else:
                raise Exception("Could not find Kafka container")
        except Exception as e2:
            print(f"❌ Failed to create topic: {e2}")
            raise
    
    print("⏳ Waiting 2 seconds for topic to be fully ready...")
    time.sleep(2)
    print("✅ Topic creation complete")

default_args = {
    'owner': 'etl_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='Real-time streaming ETL pipeline demonstrating Kafka value',
    schedule_interval='@once',  # Run once when triggered
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'kafka', 'streaming', 'datawarehouse'],
)

create_kafka_topic = PythonOperator(
    task_id='create_kafka_topic',
    python_callable=create_kafka_topic_if_not_exists,
    dag=dag,
)

produce_to_kafka = BashOperator(
    task_id='produce_to_kafka',
    bash_command="""
    cd /opt/airflow/workspace && \
    python3 newProducer.py
    """,
    dag=dag,
)

bronze_write = BashOperator(
    task_id='bronze_write',
    bash_command="""
    cd /opt/airflow/scripts && \
    python3 bronze_write.py
    """,
    dag=dag,
)

bronze_to_silver = BashOperator(
    task_id='bronze_to_silver',
    bash_command="""
    cd /opt/airflow/scripts && \
    python3 bronze_to_silver.py
    """,
    dag=dag,
)

silver_to_gold = BashOperator(
    task_id='silver_to_gold',
    bash_command="""
    cd /opt/airflow/scripts && \
    python3 silver_to_gold.py
    """,
    dag=dag,
)

final_query = BashOperator(
    task_id='final_query',
    bash_command="""
    cd /opt/airflow/scripts && \
    python3 finalquery.py
    """,
    dag=dag,
)

create_kafka_topic >> produce_to_kafka >> bronze_write >> bronze_to_silver >> silver_to_gold >> final_query

