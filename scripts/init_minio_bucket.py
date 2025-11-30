#!/usr/bin/env python3
"""
Initialize MinIO bucket on startup
Creates the 'datalake' bucket if it doesn't exist
"""
import boto3
import time
import sys

MINIO_ENDPOINT = "http://minio:9000"
AWS_KEY = "minio"
AWS_SECRET = "minio123"
BUCKET_NAME = "datalake"

def wait_for_minio(max_retries=30, delay=2):
    """Wait for MinIO to be ready"""
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name="us-east-1"
    )
    
    for i in range(max_retries):
        try:
            s3.list_buckets()
            print("✅ MinIO is ready")
            return s3
        except Exception as e:
            if i < max_retries - 1:
                print(f"⏳ Waiting for MinIO... (attempt {i+1}/{max_retries})")
                time.sleep(delay)
            else:
                print(f"❌ MinIO not ready after {max_retries} attempts: {e}")
                sys.exit(1)
    return s3

def create_bucket(s3):
    """Create bucket if it doesn't exist"""
    try:
        # Check if bucket exists
        try:
            s3.head_bucket(Bucket=BUCKET_NAME)
            print(f"ℹ️  Bucket '{BUCKET_NAME}' already exists")
            return True
        except:
            # Bucket doesn't exist, create it
            s3.create_bucket(Bucket=BUCKET_NAME)
            print(f"✅ Bucket '{BUCKET_NAME}' created successfully")
            return True
    except Exception as e:
        print(f"❌ Error creating bucket: {e}")
        sys.exit(1)

if __name__ == "__main__":
    print("🚀 Initializing MinIO bucket...")
    s3 = wait_for_minio()
    create_bucket(s3)
    print("🎉 MinIO initialization complete")

