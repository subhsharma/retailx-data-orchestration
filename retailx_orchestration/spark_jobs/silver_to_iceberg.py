from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("RetailX-Silver-to-Iceberg")
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


DATABASE = "retailx"

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS retailx_catalog.{DATABASE}")


datasets = {
    "customers": "s3a://retailx/silver/customers",
    "products": "s3a://retailx/silver/products",
    "orders": "s3a://retailx/silver/orders",
    "payments": "s3a://retailx/silver/payments",
}


for table_name, source_path in datasets.items():

    print(f"\nProcessing {table_name}")

    df = spark.read.parquet(source_path)

    print(f"Source rows: {df.count()}")

    table_identifier = f"retailx_catalog.{DATABASE}.{table_name}"

    spark.sql(
        f"DROP TABLE IF EXISTS {table_identifier}"
    )

    (
        df.writeTo(table_identifier)
        .using("iceberg")
        .create()
    )

    count = spark.table(table_identifier).count()

    print(
        f"Iceberg table created: {table_identifier}"
    )
    print(f"Iceberg rows: {count}")


print("\n===== ICEBERG SILVER TABLES =====")

spark.sql(
    "SHOW TABLES IN retailx_catalog.retailx"
).show(truncate=False)


spark.stop()