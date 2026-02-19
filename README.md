# Real-Time Ride Events Pipeline

This project streams ride records from PostgreSQL to Kafka, then processes them with Spark Structured Streaming and writes Parquet output.

## Architecture

PostgreSQL -> Kafka -> Spark Structured Streaming -> Parquet output

## Setup

```bash
cd /Users/brk/Desktop/real_time_pipeline
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Option A: Run Locally with Docker

1. Start local services (Postgres + Kafka + Zookeeper):

```bash
docker-compose up -d
docker-compose ps
```

2. Run the producer:

```bash
cd /Users/brk/Desktop/real_time_pipeline
source .venv/bin/activate
export POSTGRES_PORT="5433"
python producer/produce_rides.py
```

3. Run the Spark consumer in a second terminal:

```bash
cd /Users/brk/Desktop/real_time_pipeline
source .venv/bin/activate
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark_streaming/spark_consumer.py
```

4. Verify output:

```bash
ls -la s3_mock/bolt_rides
ls -la checkpoints/bolt_rides
```

## Option B: Run with AWS Services (No Docker)

Use:
- Amazon RDS for PostgreSQL
- Amazon MSK for Kafka
- Amazon S3 for output

Set environment variables before running scripts:

```bash
export POSTGRES_HOST="<rds-endpoint>"
export POSTGRES_PORT="5432"
export POSTGRES_DB="rides_db"
export POSTGRES_USER="<db-user>"
export POSTGRES_PASSWORD="<db-password>"

export KAFKA_BOOTSTRAP_SERVERS="<msk-bootstrap-1:9092,msk-bootstrap-2:9092>"
export KAFKA_TOPIC="bolt_rides"

export OUTPUT_PATH="s3a://<your-bucket>/bolt_rides"
export CHECKPOINT_PATH="s3a://<your-bucket>/checkpoints/bolt_rides"
```

Run producer:

```bash
cd /Users/brk/Desktop/real_time_pipeline
source .venv/bin/activate
python producer/produce_rides.py
```

Run Spark consumer (Spark must have Kafka + Hadoop AWS packages):

```bash
cd /Users/brk/Desktop/real_time_pipeline
source .venv/bin/activate
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  spark_streaming/spark_consumer.py
```

If Spark is outside AWS (for example local laptop), also pass AWS credentials or IAM role-compatible access for writing to S3.
