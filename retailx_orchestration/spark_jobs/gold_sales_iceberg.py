from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = (
    SparkSession.builder
    .appName("RetailX-Gold-Sales-Iceberg")
    .master("spark://172.17.0.2:7077")
    .config(
        "spark.sql.catalog.retailx_catalog",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    .config(
        "spark.sql.catalog.retailx_catalog.type",
        "hadoop",
    )
    .config(
        "spark.sql.catalog.retailx_catalog.warehouse",
        "s3a://retailx/iceberg/warehouse",
    )
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        "http://retailx-minio:9000",
    )
    .config(
        "spark.hadoop.fs.s3a.access.key",
        "minioadmin",
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        "minioadmin123",
    )
    .config(
        "spark.hadoop.fs.s3a.path.style.access",
        "true",
    )
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    )
    .config(
        "spark.sql.defaultCatalog",
        "retailx_catalog",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


customers = spark.table("retailx_catalog.retailx.customers")
products = spark.table("retailx_catalog.retailx.products")
orders = spark.table("retailx_catalog.retailx.orders")
payments = spark.table("retailx_catalog.retailx.payments")


sales = (
    orders
    .join(
        customers,
        orders.customer_id == customers.customer_id,
        "inner",
    )
    .join(
        products,
        orders.product_id == products.product_id,
        "inner",
    )
    .join(
        payments,
        orders.order_id == payments.order_id,
        "left",
    )
    .select(
        orders.order_id,
        orders.order_date,
        orders.customer_id,
        customers.customer_name,
        customers.city.alias("customer_city"),
        orders.product_id,
        products.product_name,
        products.category,
        orders.quantity,
        products.price.alias("unit_price"),
        (
            orders.quantity * products.price
        ).alias("total_amount"),
        payments.payment_method,
        payments.payment_status,
    )
)


sales = sales.orderBy("order_id")

print(f"Gold sales rows: {sales.count()}")

sales.show(truncate=False)


table_name = "retailx_catalog.retailx.sales"

spark.sql(f"DROP TABLE IF EXISTS {table_name}")

(
    sales.writeTo(table_name)
    .using("iceberg")
    .create()
)


result = spark.table(table_name)

print("\n===== GOLD ICEBERG TABLE =====")
result.show(truncate=False)

print(f"Gold Iceberg rows: {result.count()}")

total_sales = result.selectExpr(
    "sum(total_amount) as total_sales"
).collect()[0]["total_sales"]

print(f"Total sales: {total_sales}")

print("\n===== ICEBERG TABLES =====")

spark.sql(
    "SHOW TABLES IN retailx_catalog.retailx"
).show(truncate=False)


spark.stop()