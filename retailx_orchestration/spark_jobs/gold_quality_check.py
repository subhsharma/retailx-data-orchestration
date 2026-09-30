from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum


spark = (
    SparkSession.builder
    .appName("RetailX Gold Quality Check")
    .master("spark://172.17.0.2:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


MINIO_ENDPOINT = "http://retailx-minio:9000"
BUCKET = "retailx"

spark.conf.set(
    "spark.hadoop.fs.s3a.endpoint",
    MINIO_ENDPOINT,
)

spark.conf.set(
    "spark.hadoop.fs.s3a.access.key",
    "minioadmin",
)

spark.conf.set(
    "spark.hadoop.fs.s3a.secret.key",
    "minioadmin123",
)

spark.conf.set(
    "spark.hadoop.fs.s3a.path.style.access",
    "true",
)

spark.conf.set(
    "spark.hadoop.fs.s3a.connection.ssl.enabled",
    "false",
)

spark.conf.set(
    "spark.hadoop.fs.s3a.impl",
    "org.apache.hadoop.fs.s3a.S3AFileSystem",
)


print("=" * 50)
print("Reading Gold Sales")
print("=" * 50)

gold_sales = spark.read.parquet(
    f"s3a://{BUCKET}/gold/sales"
)

row_count = gold_sales.count()

null_order_ids = gold_sales.filter(
    col("order_id").isNull()
).count()

negative_amounts = gold_sales.filter(
    col("total_amount") < 0
).count()

null_products = gold_sales.filter(
    col("product_id").isNull()
).count()

null_customers = gold_sales.filter(
    col("customer_id").isNull()
).count()

total_sales = gold_sales.select(
    spark_sum("total_amount")
).collect()[0][0]


print("=" * 50)
print("QUALITY RESULTS")
print("=" * 50)

print(f"Row count: {row_count}")
print(f"Null order IDs: {null_order_ids}")
print(f"Negative amounts: {negative_amounts}")
print(f"Null product IDs: {null_products}")
print(f"Null customer IDs: {null_customers}")
print(f"Total sales: {total_sales}")


quality_passed = (
    row_count > 0
    and null_order_ids == 0
    and negative_amounts == 0
    and null_products == 0
    and null_customers == 0
)


if not quality_passed:
    print("=" * 50)
    print("QUALITY CHECK FAILED")
    print("=" * 50)

    spark.stop()
    raise Exception("Gold Sales data quality validation failed")


print("=" * 50)
print("QUALITY CHECK PASSED")
print("=" * 50)

spark.stop()