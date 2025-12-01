import duckdb
import boto3
import uuid
import os
import sys


con = duckdb.connect("etl.duckdb")


MINIO_ENDPOINT = "http://localhost:9000"
if os.path.exists('/opt/airflow'):
    MINIO_ENDPOINT = "http://minio:9000"

s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id="minio",
    aws_secret_access_key="minio123",
    region_name="us-east-1"
)


s3_host = MINIO_ENDPOINT.replace('http://', '').replace('https://', '')
con.execute(f"""
    SET s3_endpoint='{s3_host}';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
    SET s3_access_key_id='minio';
    SET s3_secret_access_key='minio123';
    SET s3_region='us-east-1';
""")

bucket_name = "datalake"
bronze_prefix = "bronze"
silver_prefix = "silver"

print("✅ Connected to DuckDB and configured MinIO access")

# Create state table to track processed Bronze files (incremental processing)
con.execute("""
    CREATE TABLE IF NOT EXISTS processed_bronze_files (
        file_path VARCHAR PRIMARY KEY,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")

# Get list of already processed files
processed_files_result = con.execute("SELECT file_path FROM processed_bronze_files").fetchall()
processed_files_set = {row[0] for row in processed_files_result}
print(f"📋 Found {len(processed_files_set)} already processed Bronze files")

resp = s3.list_objects_v2(
    Bucket=bucket_name,
    Prefix=f"{bronze_prefix}/"
)

if "Contents" not in resp:
    print("⚠️ No objects found inside Bronze folder!")
    sys.exit(0)

all_parquet_files = [
    f"s3://{bucket_name}/{obj['Key']}"
    for obj in resp["Contents"]
    if obj["Key"].endswith(".parquet")
]

if not all_parquet_files:
    print("⚠️ No Bronze parquet files found!")
    sys.exit(0)

# Filter to only new files (incremental processing)
parquet_files = [f for f in all_parquet_files if f not in processed_files_set]

if not parquet_files:
    print(f"✅ All {len(all_parquet_files)} Bronze files already processed. Nothing new to process.")
    con.close()
    sys.exit(0)

print(f"📦 Found {len(all_parquet_files)} total Bronze files, {len(parquet_files)} new files to process")

print("🚀 Transforming Bronze → Silver (incremental processing)...")


for file in parquet_files:
    try:
        # Generate unique parquet name
        silver_file = f"s3://{bucket_name}/{silver_prefix}/silver_{uuid.uuid4().hex}.parquet"

        con.execute(f"""
            COPY (
                SELECT 
                    user_id,
                    name,
                    email,
                    phone,
                    street_address,
                    city,
                    country,
                    transaction_id,
                    TRY_CAST(transaction_date AS TIMESTAMP) AS transaction_date,
                    TRY_CAST(amount AS DOUBLE) AS amount,
                    currency,
                    merchant,
                    description,
                    transaction_type,
                    payment_method
                FROM read_parquet('{file}')
                WHERE email LIKE '%@%'
                  AND amount IS NOT NULL
                  AND TRY_CAST(amount AS DOUBLE) > 0
                  AND TRY_CAST(transaction_date AS TIMESTAMP) IS NOT NULL
            )
            TO '{silver_file}'
            (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE)
        """)

        # Mark this Bronze file as processed ONLY after successful transformation
        con.execute(f"""
            INSERT OR IGNORE INTO processed_bronze_files (file_path)
            VALUES ('{file}')
        """)

        print(f"✅ Silver file written: {silver_file} (Bronze file marked as processed)")
    except Exception as e:
        print(f"❌ Error processing Bronze file {file}: {e}")
        print(f"⚠️ File NOT marked as processed - will retry on next run")
        continue

# ============================================================
# 6. Preview sample of silver output
# ============================================================
try:
    df_preview = con.execute(f"""
        SELECT * 
        FROM read_parquet('s3://{bucket_name}/{silver_prefix}/*.parquet')
        LIMIT 5
    """).fetchdf()

    print("🧊 Silver data preview:")
    print(df_preview)

except Exception as e:
    print("⚠️ Could not preview Silver data:", e)


con.close()
print("🎉 Silver ETL complete, connection closed.")

