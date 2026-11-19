from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (StructType, StructField,IntegerType, StringType)
import os

S3_ENDPOINT = os.getenv("S3_ENDPOINT")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

BUCKET = os.getenv("S3_BUCKET", "lake")
SILVER = os.getenv("S3_SILVER_PREFIX", "silver")


def s3url(path):
    return f"s3a://{BUCKET}/{path}"


def build_spark():
    # 1) Create Spark session
    spark = (
        SparkSession.builder
        .appName("KafkaToSparkStreaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    conf = spark._jsc.hadoopConfiguration()
    conf.set("fs.s3a.endpoint", S3_ENDPOINT)
    conf.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    conf.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    conf.set("fs.s3a.path.style.access", "true")
    conf.set("fs.s3a.connection.ssl.enabled", "false")

    return spark

spark = build_spark()
spark.sparkContext.setLogLevel("WARN")


# 2) Define schema for the Debezium JSON inside Kafka "value"
order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("order_amount", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("status", StringType(), True),
])

event_schema = StructType([
    StructField("before", order_schema, True),
    StructField("after", order_schema, True),
    StructField("op", StringType(), True),
])


# 3) Read stream from Kafka
df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "shop.public.orders")
    .option("startingOffsets", "earliest")
    .load()
)


# 4) Parse the Kafka JSON and Performing basic transformations on streamed data
df_parsed = (
    df_raw
    .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_timestamp")
    .select(
        F.from_json(
            F.coalesce(F.get_json_object("json_str", "$.payload"), F.col("json_str")),
            event_schema
        ).alias("e"),
        F.col("kafka_timestamp")
    )
    .select(
        F.col("e.op").alias("op"),
        F.coalesce(F.col("e.after.order_id"), F.col("e.before.order_id")).alias("order_id"),
        F.col("e.after.customer_id").alias("customer_id"),
        F.col("e.after.product_id").alias("product_id"),
        F.col("e.after.quantity").alias("quantity"),
        F.col("e.after.order_amount").cast("decimal(14,2)").alias("order_amount"),
        F.upper(F.col("e.after.currency")).alias("currency"),
        F.upper(F.col("e.after.status")).alias("status"),
        F.col("kafka_timestamp")
    )
    .filter(F.col("op").isin("r", "c", "u", "d"))
)


# 5) Write to console (debug)
query_console = (
    df_parsed.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .trigger(processingTime="5 seconds")
    .start()
)


# Writing Spark DataFrames in Delta format
query_delta = (
    df_parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("path", s3url(f"{SILVER}/orders"))
    .option("checkpointLocation", "/opt/spark/output/checkpoints/orders_to_delta")
    .trigger(processingTime="5 seconds")
    .start()
)

# 6) Wait for termination (until container is stopped)
spark.streams.awaitAnyTermination()