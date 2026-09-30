from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, initcap


spark = (
    SparkSession.builder
    .appName("RetailX Customers Silver")
    .master("spark://172.17.0.2:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


MINIO_ENDPOINT = "http://localhost:9000"
BUCKET = "retailx"

INPUT_PATH = f"s3a://{BUCKET}/bronze/customers.csv"
OUTPUT_PATH = f"s3a://{BUCKET}/silver/customers"


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


customers = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(INPUT_PATH)
)


silver_customers = (
    customers
    .withColumn("customer_id", col("customer_id").cast("int"))
    .withColumn("customer_name", initcap(trim(col("customer_name"))))
    .withColumn("city", initcap(trim(col("city"))))
    .filter(col("customer_id").isNotNull())
    .filter(col("customer_name").isNotNull())
    .filter(col("city").isNotNull())
)


silver_customers.write.mode("overwrite").parquet(OUTPUT_PATH)


print("======================================")
print("RetailX Customers Silver completed")
print("======================================")

print(f"Input:  {INPUT_PATH}")
print(f"Output: {OUTPUT_PATH}")

print(f"Rows: {silver_customers.count()}")

silver_customers.show(truncate=False)


spark.stop()