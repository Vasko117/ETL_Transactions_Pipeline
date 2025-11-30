# Airflow ETL Pipeline Automation

This project now includes an automated Airflow DAG that orchestrates the entire ETL workflow, eliminating the need to run scripts manually.

## Setup

1. **Start all services:**
   ```bash
   docker compose up -d
   ```

2. **Wait for Airflow to initialize** (first time only):
   - The `airflow-init` service will set up the database and create an admin user
   - This may take 1-2 minutes

3. **Access Airflow Web UI:**
   - Open http://localhost:8080 (NOT /admin - just the root URL)
   - Wait 1-2 minutes for Airflow to fully initialize
   - Login: `admin` / `admin`
   - If you see "page not found", wait a bit longer and refresh

## Running the ETL Pipeline

### Option 1: Via Airflow Web UI (Recommended)

1. Open the Airflow Web UI at http://localhost:8080
2. Find the `etl_pipeline` DAG in the list
3. Click the play button (▶️) to trigger the DAG
4. Monitor the progress in the Graph View or Tree View

### Option 2: Via Airflow CLI

```bash
# Trigger the DAG
docker exec -it <airflow-scheduler-container> airflow dags trigger etl_pipeline

# Check DAG status
docker exec -it <airflow-scheduler-container> airflow dags list
```

## Pipeline Steps

The DAG automates the following steps in sequence:

1. **create_kafka_topic**: Creates the `raw-data` Kafka topic (if it doesn't exist)
2. **produce_to_kafka**: Reads `transactions.csv` and produces messages to Kafka
3. **bronze_write**: Consumes from Kafka and writes Parquet files to MinIO Bronze layer
4. **bronze_to_silver**: Transforms Bronze data to Silver (cleaned/validated)
5. **silver_to_gold**: Transforms Silver data to Gold (Data Warehouse tables)

## File Locations

- **DAG**: `dags/etl_pipeline.py`
- **Scripts**: `scripts/` (mounted to `/opt/airflow/scripts` in containers)
- **CSV Data**: `scripts/transactions.csv` (used by producer)
- **DuckDB**: `scripts/etl.duckdb` (created automatically)

## Configuration

### Container Endpoints

The scripts automatically detect if they're running in Airflow containers and use the correct endpoints:

- **Kafka**: `kafka:19092` (container) or `localhost:9092` (local)
- **MinIO**: `http://minio:9000` (container) or `http://localhost:9000` (local)

### Dependencies

Required Python packages are automatically installed in Airflow containers via the `_PIP_ADDITIONAL_REQUIREMENTS` environment variable.

## Troubleshooting

### DAG not appearing in Airflow UI

1. Check if Airflow scheduler is running:
   ```bash
   docker ps | grep airflow-scheduler
   ```

2. Check scheduler logs:
   ```bash
   docker logs <airflow-scheduler-container>
   ```

### Task failures

1. Check task logs in Airflow UI (click on the task → View Log)
2. Verify all services are running:
   ```bash
   docker compose ps
   ```

3. Ensure Kafka topic exists:
   ```bash
   docker exec -it <kafka-container> kafka-topics.sh --list --bootstrap-server localhost:9092
   ```

### Script errors

- Verify `transactions.csv` exists in `scripts/` folder
- Check that MinIO bucket `datalake` is accessible
- Ensure DuckDB file permissions are correct

## Manual Execution (Fallback)

If you need to run scripts manually for debugging:

```bash
# 1. Create Kafka topic
docker exec -it <kafka-container> kafka-topics.sh --create --topic raw-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# 2. Run producer
python3 newProducer.py

# 3. Run ETL scripts
cd scripts
python3 bronze_write.py
python3 bronze_to_silver.py
python3 silver_to_gold.py
```

## Notes

- The DAG is set to manual trigger by default (`schedule_interval=None`)
- To schedule it, modify `schedule_interval` in `dags/etl_pipeline.py`
- The pipeline processes all available data in each run
- Bronze and Silver layers accumulate data (not overwritten)
- Gold layer uses `INSERT OR IGNORE` to handle duplicates

