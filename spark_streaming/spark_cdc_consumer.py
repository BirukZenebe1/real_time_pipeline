import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

APP_NAME = os.getenv("SPARK_APP_NAME", "BoltRideCDCStreaming")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "rides_cdc.public.rides")
STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "./s3_mock/rides_cdc")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "./checkpoints/rides_cdc")

spark = SparkSession.builder.appName(APP_NAME).master(SPARK_MASTER).getOrCreate()

ride_schema = StructType(
    [
        StructField("ride_id", IntegerType()),
        StructField("driver_id", IntegerType()),
        StructField("city", StringType()),
        StructField("timestamp", StringType()),
        StructField("price", DoubleType()),
        StructField("status", StringType()),
    ]
)

cdc_schema = StructType(
    [
        StructField("before", ride_schema),
        StructField("after", ride_schema),
        StructField("op", StringType()),
        StructField("ts_ms", LongType()),
    ]
)

raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", STARTING_OFFSETS)
    .load()
)

parsed_df = (
    raw_df.selectExpr("CAST(value AS STRING) as json")
    .select(from_json("json", cdc_schema).alias("cdc"))
    .select("cdc.*")
)

rides_df = (
    parsed_df.filter(col("op").isin("c", "u", "r"))
    .filter(col("after").isNotNull())
    .select("after.*")
)

clean_df = rides_df.filter(
    (col("ride_id").isNotNull())
    & (col("price") > 0)
    & (col("status").isin("completed", "cancelled"))
)

query = (
    clean_df.writeStream.format("parquet")
    .option("path", OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .start()
)

query.awaitTermination()
