import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

APP_NAME = os.getenv("SPARK_APP_NAME", "BoltRideStreaming")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "bolt_rides")
STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "./s3_mock/bolt_rides")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "./checkpoints/bolt_rides")

spark = SparkSession.builder \
    .appName(APP_NAME) \
    .master(SPARK_MASTER) \
    .getOrCreate()

# Define schema
schema = StructType([
    StructField("ride_id", IntegerType()),
    StructField("driver_id", IntegerType()),
    StructField("city", StringType()),
    StructField("timestamp", StringType()),
    StructField("price", DoubleType()),
    StructField("status", StringType())
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", STARTING_OFFSETS) \
    .load()

# Deserialize
df_parsed = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json("json", schema).alias("data")) \
    .select("data.*")

# Data quality filter
df_clean = df_parsed.filter(
    (col("ride_id").isNotNull()) &
    (col("price") > 0) &
    (col("status").isin("completed", "cancelled"))
)

# Write to S3 or local folder
query = df_clean.writeStream \
    .format("parquet") \
    .option("path", OUTPUT_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .outputMode("append") \
    .start()

query.awaitTermination()
