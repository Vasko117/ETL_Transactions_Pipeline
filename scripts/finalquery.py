#!/usr/bin/env python3
"""
Final Query Script - Shows top 5 records from each layer (Bronze, Silver, Gold)
This script runs at the end of the ETL pipeline to verify data quality.
"""
import duckdb
import boto3
import os
import sys

# Configuration
DUCKDB_FILE = "etl.duckdb"
MINIO_ENDPOINT = "http://minio:9000"
if not os.path.exists('/opt/airflow'):
    MINIO_ENDPOINT = "http://localhost:9000"

AWS_KEY = "minio"
AWS_SECRET = "minio123"
AWS_REGION = "us-east-1"
BUCKET_NAME = "datalake"

print("=" * 80)
print("🔍 FINAL QUERY - Data Quality Verification")
print("=" * 80)
print()

# Connect to DuckDB
con = duckdb.connect(DUCKDB_FILE)

# Configure MinIO/S3 access
s3_host = MINIO_ENDPOINT.replace('http://', '').replace('https://', '')
con.execute(f"""
    SET s3_endpoint='{s3_host}';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
    SET s3_access_key_id='{AWS_KEY}';
    SET s3_secret_access_key='{AWS_SECRET}';
    SET s3_region='{AWS_REGION}';
""")

s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET,
    region_name=AWS_REGION
)

print("✅ Connected to DuckDB and MinIO")
print()

# ============================================================
# 1. BRONZE LAYER - Top 5 records
# ============================================================
print("🟤 BRONZE LAYER - Top 5 Records")
print("-" * 80)
try:
    # List bronze files
    resp = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix="bronze/")
    bronze_files = [
        f"s3://{BUCKET_NAME}/{obj['Key']}"
        for obj in resp.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]
    
    if bronze_files:
        # Use the first bronze file
        bronze_file = bronze_files[0]
        df_bronze = con.execute(f"""
            SELECT * 
            FROM read_parquet('{bronze_file}')
            LIMIT 5
        """).fetchdf()
        print(df_bronze.to_string())
        print(f"\n📊 Total Bronze files: {len(bronze_files)}")
    else:
        print("⚠️ No Bronze files found")
except Exception as e:
    print(f"❌ Error querying Bronze: {e}")

print()
print()

# ============================================================
# 2. SILVER LAYER - Top 5 records
# ============================================================
print("🧊 SILVER LAYER - Top 5 Records")
print("-" * 80)
try:
    df_silver = con.execute(f"""
        SELECT * 
        FROM read_parquet('s3://{BUCKET_NAME}/silver/*.parquet')
        LIMIT 5
    """).fetchdf()
    print(df_silver.to_string())
    
    # Count total records
    count_silver = con.execute(f"""
        SELECT COUNT(*) as total
        FROM read_parquet('s3://{BUCKET_NAME}/silver/*.parquet')
    """).fetchone()[0]
    print(f"\n📊 Total Silver records: {count_silver}")
except Exception as e:
    print(f"❌ Error querying Silver: {e}")

print()
print()

# ============================================================
# 3. GOLD LAYER - Top 5 records from each table
# ============================================================
print("🟡 GOLD LAYER - Top 5 Records from Data Warehouse Tables")
print("-" * 80)

# List gold files
try:
    resp = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix="gold/")
    gold_files = [
        obj['Key']
        for obj in resp.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]
    
    if gold_files:
        print(f"📦 Found {len(gold_files)} Gold parquet files")
        print()
        
        # Get unique table names (assuming format: gold_<hash>.parquet or <table_name>.parquet)
        # Try to query the most recent or first few files
        for gold_file in gold_files[:5]:  # Show first 5 gold files
            try:
                file_path = f"s3://{BUCKET_NAME}/{gold_file}"
                df_gold = con.execute(f"""
                    SELECT * 
                    FROM read_parquet('{file_path}')
                    LIMIT 5
                """).fetchdf()
                
                # Get count
                count_gold = con.execute(f"""
                    SELECT COUNT(*) as total
                    FROM read_parquet('{file_path}')
                """).fetchone()[0]
                
                print(f"📄 File: {gold_file}")
                print(f"📊 Total records: {count_gold}")
                print(df_gold.to_string())
                print()
            except Exception as e:
                print(f"⚠️ Could not read {gold_file}: {e}")
                print()
    else:
        # Try querying from DuckDB tables if they exist
        try:
            tables = con.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'main'
                ORDER BY table_name
            """).fetchall()
            
            if tables:
                print("📊 Querying from DuckDB tables:")
                print()
                for (table_name,) in tables:
                    try:
                        df_table = con.execute(f"""
                            SELECT * FROM {table_name} LIMIT 5
                        """).fetchdf()
                        count_table = con.execute(f"""
                            SELECT COUNT(*) FROM {table_name}
                        """).fetchone()[0]
                        
                        print(f"📄 Table: {table_name}")
                        print(f"📊 Total records: {count_table}")
                        print(df_table.to_string())
                        print()
                    except Exception as e:
                        print(f"⚠️ Could not query {table_name}: {e}")
                        print()
            else:
                print("⚠️ No Gold files or tables found")
        except Exception as e:
            print(f"⚠️ Could not query Gold layer: {e}")
except Exception as e:
    print(f"❌ Error querying Gold: {e}")

print()
print("=" * 80)
print("✅ Final Query Complete - ETL Pipeline Verification Done")
print("=" * 80)

con.close()

