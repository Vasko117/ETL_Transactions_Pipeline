#!/usr/bin/env python3
"""
Silver to Gold ETL with Fast ML Classification
Uses optimized sentence-transformers model (loads in seconds, not minutes)
"""
import duckdb
import boto3
import uuid
import hashlib
import os
import sys
from datetime import datetime
import pandas as pd

# Import the fast ML category classifier
script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)

from category_classifier_fast import get_fast_classifier

DUCKDB_FILE = "etl.duckdb"
MINIO_ENDPOINT = "http://minio:9000"
AWS_KEY = "minio"
AWS_SECRET = "minio123"
AWS_REGION = "us-east-1"

bucket_name = "datalake"
silver_prefix = "silver"
gold_prefix = "gold"

def hash_key(value: str) -> int:
    return int(hashlib.md5(value.encode()).hexdigest()[:12], 16)

def read_create_sql(path):
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()

con = duckdb.connect(DUCKDB_FILE)

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET,
    region_name=AWS_REGION
)

con.execute(f"""
    SET s3_endpoint='{MINIO_ENDPOINT.replace('http://','')}';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
    SET s3_access_key_id='{AWS_KEY}';
    SET s3_secret_access_key='{AWS_SECRET}';
    SET s3_region='{AWS_REGION}';
""")

print("✅ DuckDB connected and MinIO configured")

# Initialize FAST classifier (loads in seconds, not minutes)
print("⚡ Initializing FAST ML classifier (sentence-transformers - loads in ~5 seconds)...")
classifier = get_fast_classifier()
print("✅ Fast classifier ready!")

# Create tables
script_dir = os.path.dirname(os.path.realpath(__file__))
create_sql_path = os.path.join(script_dir, "create_dw_tables.sql")
if os.path.exists(create_sql_path):
    try:
        ddl = read_create_sql(create_sql_path)
        con.execute(ddl)
        print(f"✅ Executed DDL from {create_sql_path}")
    except Exception as e:
        print("⚠️ Failed to execute create_dw_tables.sql:", e)

# List silver files
try:
    resp = s3.list_objects_v2(Bucket=bucket_name, Prefix=f"{silver_prefix}/")
except Exception as e:
    print("❌ Error listing objects from MinIO:", e)
    sys.exit(1)

if "Contents" not in resp:
    print("⚠️ No objects under silver/ prefix. Exiting.")
    sys.exit(0)

silver_files = [
    f"s3://{bucket_name}/{obj['Key']}"
    for obj in resp["Contents"]
    if obj["Key"].endswith(".parquet")
]

if not silver_files:
    print("⚠️ No Silver parquet files found. Exiting.")
    sys.exit(0)

print(f"📦 Found {len(silver_files)} silver parquet files")

required_cols_user = {"user_id", "name", "street_address", "phone", "city", "country", "email"}
required_cols_fact = {"transaction_id", "description", "merchant", "transaction_date", "amount", "transaction_type", "currency", "payment_method", "user_id"}

for file in silver_files:
    print(f"\n➡️ Processing silver file: {file}")
    try:
        df = con.execute(f"SELECT * FROM read_parquet('{file}')").fetchdf()
    except Exception as e:
        print(f"⚠️ Failed to read parquet {file}: {e}. Skipping.")
        continue

    if df.empty:
        print("⚠️ Silver file empty, skipping.")
        continue

    cols = set(df.columns)
    missing_for_user = required_cols_user - cols
    missing_for_fact = required_cols_fact - cols

    if missing_for_user and missing_for_fact:
        print(f"⚠️ Missing many expected columns. Skipping.")
        continue

    # Process user dimension
    try:
        if required_cols_user.intersection(cols):
            take = list(required_cols_user.intersection(cols))
            df_user = df[take].drop_duplicates().copy()
            rename_map = {}
            if "street_address" in df_user.columns:
                rename_map["street_address"] = "address"
            if "phone" in df_user.columns:
                rename_map["phone"] = "phone_number"
            if rename_map:
                df_user = df_user.rename(columns=rename_map)

            con.register("tmp_users", df_user)
            con.execute("""
                INSERT OR IGNORE INTO dim_user
                (user_id, name, address, phone_number, city, country, email)
                SELECT user_id, name, address, phone_number, city, country, email
                FROM tmp_users;
            """)
            print(f"✔ DIM USER upserted ({len(df_user)} rows)")
    except Exception as e:
        print("⚠️ Error inserting dim_user:", e)

    # Fast ML classification for categories
    try:
        if {"description", "merchant"}.issubset(cols):
            print(f"⚡ Fast ML classification for {len(df)} records...")
            
            df_cat = df[["description", "merchant"]].drop_duplicates().copy()
            
            # Fast batch classification
            categories = classifier.classify_batch(
                df_cat["description"].fillna("").astype(str).tolist(),
                df_cat["merchant"].fillna("").astype(str).tolist()
            )
            
            df_cat["category"] = categories
            df_cat["category_id"] = df_cat.apply(lambda r: hash_key(str(r["category"]) + str(r["merchant"])), axis=1)
            
            con.register("tmp_cat", df_cat)
            con.execute("""
                INSERT OR IGNORE INTO dim_category
                (category_id, category_type, merchant)
                SELECT category_id, category, merchant FROM tmp_cat;
            """)
            print(f"✔ DIM CATEGORY upserted ({len(df_cat)} rows)")
            category_counts = df_cat["category"].value_counts()
            for cat, count in category_counts.head(5).items():
                print(f"   {cat}: {count}")
    except Exception as e:
        print(f"⚠️ Error inserting dim_category: {e}")

    # Process payment dimension
    try:
        if {"transaction_type", "currency", "payment_method"}.issubset(cols):
            df_pay = df[["transaction_type", "currency", "payment_method"]].drop_duplicates().copy()
            df_pay["payment_id"] = df_pay.apply(lambda r: hash_key(str(r["transaction_type"]) + str(r["currency"]) + str(r["payment_method"])), axis=1)
            con.register("tmp_pay", df_pay)
            con.execute("""
                INSERT OR IGNORE INTO dim_payment
                (payment_id, payment_type, payment_currency, payment_method)
                SELECT payment_id, transaction_type, currency, payment_method
                FROM tmp_pay;
            """)
            print(f"✔ DIM PAYMENT upserted ({len(df_pay)} rows)")
    except Exception as e:
        print("⚠️ Error inserting dim_payment:", e)

    # Process date dimension
    try:
        if "transaction_date" in cols:
            df["transaction_date"] = df["transaction_date"].astype("datetime64[ns]")
            df_date = df[["transaction_date"]].drop_duplicates().copy()
            df_date["date_id"] = df_date["transaction_date"].apply(lambda d: int(d.strftime("%Y%m%d%H%M")))
            df_date["year"] = df_date["transaction_date"].dt.year
            df_date["quarter"] = df_date["transaction_date"].dt.quarter
            df_date["month"] = df_date["transaction_date"].dt.month
            df_date["weekday"] = df_date["transaction_date"].dt.day_name()
            df_date["day"] = df_date["transaction_date"].dt.day
            df_date["hour"] = df_date["transaction_date"].dt.hour
            df_date["minute"] = df_date["transaction_date"].dt.minute

            con.register("tmp_dates", df_date)
            con.execute("""
                INSERT OR IGNORE INTO dim_date
                (date_id, year, quarter, month, weekday, day, hour, minute)
                SELECT date_id, year, quarter, month, weekday, day, hour, minute
                FROM tmp_dates;
            """)
            print(f"✔ DIM DATE upserted ({len(df_date)} rows)")
    except Exception as e:
        print("⚠️ Error inserting dim_date:", e)

    # Process fact table
    try:
        if required_cols_fact.intersection(cols):
            if "transaction_date" in df.columns:
                df["transaction_date"] = df["transaction_date"].astype("datetime64[ns]")
                df["date_id"] = df["transaction_date"].apply(lambda d: int(d.strftime("%Y%m%d%H%M")))
            else:
                df["date_id"] = None

            # Fast ML classification for fact table
            if {"description", "merchant"}.issubset(df.columns):
                print(f"⚡ Fast ML classification for fact table ({len(df)} records)...")
                categories = classifier.classify_batch(
                    df["description"].fillna("").astype(str).tolist(),
                    df["merchant"].fillna("").astype(str).tolist()
                )
                df["category"] = categories
                df["category_id"] = df.apply(lambda r: hash_key(str(r["category"]) + str(r["merchant"])), axis=1)
            else:
                df["category_id"] = None

            if {"transaction_type", "currency", "payment_method"}.issubset(df.columns):
                df["payment_id"] = df.apply(lambda r: hash_key(str(r.get("transaction_type","")) + str(r.get("currency","")) + str(r.get("payment_method",""))), axis=1)
            else:
                df["payment_id"] = None

            df_fact = df[["transaction_id","category_id","date_id","user_id","payment_id","amount"]].copy()
            df_fact.columns = ["transaction_id", "category_id", "date_id", "user_id", "payment_id", "transaction_amount"]

            con.register("tmp_fact", df_fact)
            con.execute("""
                INSERT OR IGNORE INTO transaction_fact
                (transaction_id, category_id, date_id, user_id, payment_id, transaction_amount)
                SELECT transaction_id, category_id, date_id, user_id, payment_id, transaction_amount
                FROM tmp_fact;
            """)
            print(f"✔ FACT rows inserted ({len(df_fact)} rows)")
    except Exception as e:
        print("⚠️ Error inserting facts:", e)

# Write to gold layer
tables = {
    "dim_user": f"{gold_prefix}/dim_user.parquet",
    "dim_category": f"{gold_prefix}/dim_category.parquet",
    "dim_payment": f"{gold_prefix}/dim_payment.parquet",
    "dim_date": f"{gold_prefix}/dim_date.parquet",
    "transaction_fact": f"{gold_prefix}/transaction_fact.parquet",
}

for table, path in tables.items():
    try:
        target = f"s3://{bucket_name}/{path}"
        con.execute(f"""
            COPY (SELECT * FROM {table})
            TO '{target}'
            (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE);
        """)
        print(f"✔ Wrote {table} → {target}")
    except Exception as e:
        print(f"⚠️ Failed to write {table} to gold: {e}")

print("\n🎉 Silver to Gold ETL Complete (Fast ML)")
con.close()
