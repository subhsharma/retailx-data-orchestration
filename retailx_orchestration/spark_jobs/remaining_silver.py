from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, initcap, to_date


spark = (
    SparkSession.builder
    .appName("RetailX Remaining Silver")
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


# =========================================================
# PRODUCTS
# =========================================================

print("======================================")
print("Processing Products")
print("======================================")


products = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"s3a://{BUCKET}/bronze/products.csv")
)


silver_products = (
    products
    .withColumn(
        "product_id",
        col("product_id").cast("int")
    )
    .withColumn(
        "product_name",
        initcap(trim(col("product_name")))
    )
    .withColumn(
        "category",
        initcap(trim(col("category")))
    )
    .withColumn(
        "price",
        col("price").cast("double")
    )
    .filter(col("product_id").isNotNull())
    .filter(col("product_name").isNotNull())
    .filter(col("category").isNotNull())
    .filter(col("price") >= 0)
)


silver_products.write.mode("overwrite").parquet(
    f"s3a://{BUCKET}/silver/products"
)


print(f"Products rows: {silver_products.count()}")
silver_products.show(truncate=False)


# =========================================================
# ORDERS
# =========================================================

print("======================================")
print("Processing Orders")
print("======================================")


orders = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"s3a://{BUCKET}/bronze/orders.csv")
)


silver_orders = (
    orders
    .withColumn(
        "order_id",
        col("order_id").cast("int")
    )
    .withColumn(
        "customer_id",
        col("customer_id").cast("int")
    )
    .withColumn(
        "product_id",
        col("product_id").cast("int")
    )
    .withColumn(
        "quantity",
        col("quantity").cast("int")
    )
    .withColumn(
        "order_date",
        to_date(col("order_date"))
    )
    .filter(col("order_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    .filter(col("product_id").isNotNull())
    .filter(col("quantity") > 0)
    .filter(col("order_date").isNotNull())
)


silver_orders.write.mode("overwrite").parquet(
    f"s3a://{BUCKET}/silver/orders"
)


print(f"Orders rows: {silver_orders.count()}")
silver_orders.show(truncate=False)


# =========================================================
# PAYMENTS
# =========================================================

print("======================================")
print("Processing Payments")
print("======================================")


payments = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"s3a://{BUCKET}/bronze/payments.csv")
)


silver_payments = (
    payments
    .withColumn(
        "payment_id",
        col("payment_id").cast("int")
    )
    .withColumn(
        "order_id",
        col("order_id").cast("int")
    )
    .withColumn(
        "payment_method",
        initcap(trim(col("payment_method")))
    )
    .withColumn(
        "payment_status",
        initcap(trim(col("payment_status")))
    )
    .withColumn(
        "amount",
        col("amount").cast("double")
    )
    .filter(col("payment_id").isNotNull())
    .filter(col("order_id").isNotNull())
    .filter(col("payment_method").isNotNull())
    .filter(col("payment_status").isNotNull())
    .filter(col("amount") >= 0)
)


silver_payments.write.mode("overwrite").parquet(
    f"s3a://{BUCKET}/silver/payments"
)


print(f"Payments rows: {silver_payments.count()}")
silver_payments.show(truncate=False)


print("======================================")
print("ALL SILVER TRANSFORMATIONS COMPLETED")
print("======================================")


spark.stop()