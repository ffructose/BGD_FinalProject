import os
from pyspark.sql import SparkSession

S3_ENDPOINT = os.getenv("S3_ENDPOINT")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

BUCKET = os.getenv("S3_BUCKET", "lake")
SILVER = os.getenv("S3_SILVER_PREFIX", "silver")


def s3url(path):
    return f"s3a://{BUCKET}/{path}"


def build_spark():
    spark = (
        SparkSession.builder
        .appName("ReadDeltaOrders")
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


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    orders = spark.read.format("delta").load(s3url(f"{SILVER}/orders"))
    orders.show(truncate=False)
    print("Total rows:", orders.count())

    spark.stop()


if __name__ == "__main__":
    main()
