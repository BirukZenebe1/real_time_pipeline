# Real-Time Ride CDC Pipeline

This project uses change data capture (CDC) from PostgreSQL WAL (logical decoding) into Kafka with Debezium, then processes events with Spark Structured Streaming.

## Architecture

PostgreSQL (WAL CDC) -> Debezium Kafka Connect -> Kafka topic -> Spark Structured Streaming -> Parquet (local or S3)

## Is AWS DMS Free?

AWS DMS is not totally free in general (you pay for replication instances/serverless usage and other resources).  
For a no-cost local setup, this repo uses Debezium in Docker.

## Services

Defined in `/Users/brk/Desktop/real_time_pipeline/docker-compose.yml`:

- `postgres` on host `localhost:5433` (configured for logical replication)
- `kafka` on host `localhost:9092`
- `zookeeper`
- `connect` (Debezium Kafka Connect REST API on `localhost:8083`)

## 1. Install Python deps

```bash
cd /Users/brk/Desktop/real_time_pipeline
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## 2. Start CDC stack

```bash
cd /Users/brk/Desktop/real_time_pipeline
docker-compose down
docker-compose up -d
docker-compose ps
```

## 3. Register Debezium connector

```bash
cd /Users/brk/Desktop/real_time_pipeline
bash scripts/register_connector.sh
```

Check status:

```bash
curl -s http://localhost:8083/connectors/rides-cdc-connector/status
```

## 4. Run Spark CDC consumer

Local output:

```bash
cd /Users/brk/Desktop/real_time_pipeline
source .venv/bin/activate
unset OUTPUT_PATH CHECKPOINT_PATH
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark_streaming/spark_cdc_consumer.py
```

S3 output:

```bash
cd /Users/brk/Desktop/real_time_pipeline
source .venv/bin/activate
export AWS_ACCESS_KEY_ID="YOUR_KEY"
export AWS_SECRET_ACCESS_KEY="YOUR_SECRET"
export AWS_DEFAULT_REGION="eu-central-1"
export OUTPUT_PATH="s3a://real-time-pipeline-bolt-78652782/bolt_rides"
export CHECKPOINT_PATH="s3a://real-time-pipeline-bolt-78652782/checkpoints/bolt_rides"
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  spark_streaming/spark_cdc_consumer.py
```

## 5. Produce CDC events by writing to Postgres

Insert:

```bash
cd /Users/brk/Desktop/real_time_pipeline
docker-compose exec postgres psql -U admin -d rides_db -c \
"INSERT INTO rides (driver_id, city, timestamp, price, status) VALUES (104, 'Warsaw', NOW(), 30.25, 'completed');"
```

Update:

```bash
cd /Users/brk/Desktop/real_time_pipeline
docker-compose exec postgres psql -U admin -d rides_db -c \
"UPDATE rides SET price = 35.00 WHERE ride_id = 1;"
```

Delete:

```bash
cd /Users/brk/Desktop/real_time_pipeline
docker-compose exec postgres psql -U admin -d rides_db -c \
"DELETE FROM rides WHERE ride_id = 2;"
```

## 6. Verify output

Local:

```bash
ls -la /Users/brk/Desktop/real_time_pipeline/s3_mock/rides_cdc
ls -la /Users/brk/Desktop/real_time_pipeline/checkpoints/rides_cdc
```

S3:

```bash
aws s3 ls s3://real-time-pipeline-bolt-78652782/bolt_rides/ --recursive
aws s3 ls s3://real-time-pipeline-bolt-78652782/checkpoints/bolt_rides/ --recursive
```

## CDC Topic and Format

- Debezium topic: `rides_cdc.public.rides`
- Message format includes `before`, `after`, and `op` (`c`, `u`, `d`, `r`)
- Spark consumer (`spark_cdc_consumer.py`) currently writes `c`, `u`, `r` events with `after` payload.
