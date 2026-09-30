from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    round,
    sum as spark_sum,
    countDistinct,
    count,
    avg,
)


spark = (
    SparkSession.builder
    .appName("RetailX Gold Sales")
    .master("spark://172.17.0.2:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# MINIO CONFIGURATION
# =========================================================

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
# READ SILVER DATA
# =========================================================

print("======================================")
print("Reading Silver datasets")
print("======================================")


customers = spark.read.parquet(
    f"s3a://{BUCKET}/silver/customers"
)

products = spark.read.parquet(
    f"s3a://{BUCKET}/silver/products"
)

orders = spark.read.parquet(
    f"s3a://{BUCKET}/silver/orders"
)

payments = spark.read.parquet(
    f"s3a://{BUCKET}/silver/payments"
)


# =========================================================
# GOLD SALES
# =========================================================

print("======================================")
print("Building Gold Sales")
print("======================================")


gold_sales = (
    orders.alias("o")

    .join(
        customers.alias("c"),
        col("o.customer_id") == col("c.customer_id"),
        "left",
    )

    .join(
        products.alias("p"),
        col("o.product_id") == col("p.product_id"),
        "left",
    )

    .join(
        payments.alias("pay"),
        col("o.order_id") == col("pay.order_id"),
        "left",
    )

    .select(
        col("o.order_id"),
        col("o.order_date"),

        col("o.customer_id"),
        col("c.customer_name"),
        col("c.city").alias("customer_city"),

        col("o.product_id"),
        col("p.product_name"),
        col("p.category"),

        col("o.quantity"),
        col("p.price").alias("unit_price"),

        (
            col("o.quantity") * col("p.price")
        ).alias("total_amount"),

        col("pay.payment_method"),
        col("pay.payment_status"),
    )

    .withColumn(
        "total_amount",
        round(col("total_amount"), 2),
    )
)


gold_sales.write.mode("overwrite").parquet(
    f"s3a://{BUCKET}/gold/sales"
)


gold_sales_count = gold_sales.count()

gold_sales_total = (
    gold_sales
    .select(
        spark_sum("total_amount").alias("total")
    )
    .collect()[0]["total"]
)


print(f"Gold sales rows: {gold_sales_count}")
print(f"Total sales: {gold_sales_total}")

gold_sales.show(truncate=False)


# =========================================================
# GOLD DAILY SALES
# =========================================================

print("======================================")
print("Building Gold Daily Sales")
print("======================================")


gold_daily_sales = (
    gold_sales
    .groupBy("order_date")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        spark_sum("quantity").alias("total_items"),
        round(
            spark_sum("total_amount"),
            2,
        ).alias("total_revenue"),
        round(
            avg("total_amount"),
            2,
        ).alias("average_order_value"),
    )
    .orderBy("order_date")
)


gold_daily_sales.write.mode("overwrite").parquet(
    f"s3a://{BUCKET}/gold/daily_sales"
)


print("Daily Sales:")
gold_daily_sales.show(truncate=False)


# =========================================================
# GOLD CATEGORY SALES
# =========================================================

print("======================================")
print("Building Gold Category Sales")
print("======================================")


gold_category_sales = (
    gold_sales
    .groupBy("category")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        spark_sum("quantity").alias("total_items"),
        round(
            spark_sum("total_amount"),
            2,
        ).alias("total_revenue"),
        round(
            avg("total_amount"),
            2,
        ).alias("average_order_value"),
    )
    .orderBy(
        col("total_revenue").desc()
    )
)


gold_category_sales.write.mode("overwrite").parquet(
    f"s3a://{BUCKET}/gold/category_sales"
)


print("Category Sales:")
gold_category_sales.show(truncate=False)


# =========================================================
# COMPLETE
# =========================================================

print("======================================")
print("ALL GOLD TRANSFORMATIONS COMPLETED")
print("======================================")

print(
    "Gold Sales: "
    "s3a://retailx/gold/sales"
)

print(
    "Gold Daily Sales: "
    "s3a://retailx/gold/daily_sales"
)

print(
    "Gold Category Sales: "
    "s3a://retailx/gold/category_sales"
)


spark.stop()