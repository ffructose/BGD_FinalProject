from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (StructType, StructField,IntegerType, StringType)

# 1) Create Spark session
spark = (
    SparkSession.builder
    .appName("KafkaToSparkStreaming")
    .getOrCreate()
)

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


# 6) Wait for termination (until container is stopped)
query_console.awaitTermination()
