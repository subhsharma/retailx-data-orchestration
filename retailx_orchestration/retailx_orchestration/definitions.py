import csv
import io
import pickle
import subprocess
from pathlib import Path

import dagster as dg
from dagster import schedule
from minio import Minio


# ============================================================
# LOCAL IO MANAGER
# ============================================================

class LocalIOManager(dg.ConfigurableIOManager):
    base_path: str = "data/dagster_storage"

    def handle_output(self, context, obj):
        asset_name = context.asset_key.path[-1]

        storage_path = (
            Path(__file__).resolve().parent.parent / self.base_path
        )

        storage_path.mkdir(parents=True, exist_ok=True)

        file_path = storage_path / f"{asset_name}.pkl"

        with open(file_path, "wb") as file:
            pickle.dump(obj, file)

        context.log.info(
            f"Saved asset '{asset_name}' to {file_path}"
        )

    def load_input(self, context):
        asset_name = context.asset_key.path[-1]

        storage_path = (
            Path(__file__).resolve().parent.parent / self.base_path
        )

        file_path = storage_path / f"{asset_name}.pkl"

        if not file_path.exists():
            raise FileNotFoundError(
                f"Asset storage file not found: {file_path}"
            )

        with open(file_path, "rb") as file:
            obj = pickle.load(file)

        context.log.info(
            f"Loaded asset '{asset_name}' from {file_path}"
        )

        return obj


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def read_csv_rows(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def upload_csv_to_minio(
    client,
    bucket_name,
    object_name,
    rows,
    columns,
):
    output = io.StringIO()

    writer = csv.DictWriter(
        output,
        fieldnames=columns,
    )

    writer.writeheader()
    writer.writerows(rows)

    data = output.getvalue().encode("utf-8")

    client.put_object(
        bucket_name,
        object_name,
        io.BytesIO(data),
        length=len(data),
        content_type="text/csv",
    )


def get_minio_client():
    return Minio(
        "localhost:19020",
        access_key="minioadmin",
        secret_key="minioadmin123",
        secure=False,
    )


# ============================================================
# CUSTOMERS
# ============================================================

@dg.asset
def customers(context: dg.AssetExecutionContext):
    file_path = (
        Path(__file__).resolve().parent.parent
        / "data"
        / "raw"
        / "customers.csv"
    )

    rows = read_csv_rows(file_path)

    for row in rows:
        row["customer_id"] = int(row["customer_id"])

    context.log.info(
        f"Loaded {len(rows)} customers"
    )

    context.add_output_metadata(
        {
            "row_count": len(rows),
            "source": str(file_path),
        }
    )

    return rows


@dg.asset_check(asset=customers)
def customers_quality_check(customers):
    valid = all(
        customer["customer_id"] is not None
        and customer["name"].strip() != ""
        and customer["city"].strip() != ""
        for customer in customers
    )

    return dg.AssetCheckResult(
        passed=valid,
        metadata={
            "customer_count": len(customers),
            "check": "customer_id, name and city validation",
        },
    )


@dg.asset
def bronze_customers(
    context: dg.AssetExecutionContext,
    customers,
):
    cleaned = []

    for customer in customers:
        cleaned.append(
            {
                "customer_id": customer["customer_id"],
                "name": customer["name"].strip(),
                "city": customer["city"].strip(),
            }
        )

    context.log.info(
        f"Bronze customers: {len(cleaned)} rows"
    )

    return cleaned


@dg.asset
def silver_customers(
    context: dg.AssetExecutionContext,
    bronze_customers,
):
    cleaned = []

    for customer in bronze_customers:
        cleaned.append(
            {
                "customer_id": int(customer["customer_id"]),
                "name": customer["name"].strip().title(),
                "city": customer["city"].strip().title(),
            }
        )

    context.log.info(
        f"Silver customers: {len(cleaned)} rows"
    )

    return cleaned


# ============================================================
# PRODUCTS
# ============================================================

@dg.asset
def products(context: dg.AssetExecutionContext):
    file_path = (
        Path(__file__).resolve().parent.parent
        / "data"
        / "raw"
        / "products"
        / "products.csv"
    )

    rows = read_csv_rows(file_path)

    for row in rows:
        row["product_id"] = int(row["product_id"])
        row["price"] = float(row["price"])

    context.log.info(
        f"Loaded {len(rows)} products"
    )

    context.add_output_metadata(
        {
            "row_count": len(rows),
            "source": str(file_path),
        }
    )

    return rows


@dg.asset
def bronze_products(
    context: dg.AssetExecutionContext,
    products,
):
    return products


@dg.asset
def silver_products(
    context: dg.AssetExecutionContext,
    bronze_products,
):
    cleaned = []

    for product in bronze_products:
        if (
            product["product_id"] is None
            or product["price"] < 0
            or not product["product_name"].strip()
            or not product["category"].strip()
        ):
            continue

        cleaned.append(
            {
                "product_id": int(product["product_id"]),
                "product_name": product["product_name"].strip().title(),
                "category": product["category"].strip().title(),
                "price": float(product["price"]),
            }
        )

    context.log.info(
        f"Silver products: {len(cleaned)} rows"
    )

    return cleaned


# ============================================================
# ORDERS
# ============================================================

@dg.asset
def orders(context: dg.AssetExecutionContext):
    file_path = (
        Path(__file__).resolve().parent.parent
        / "data"
        / "raw"
        / "orders"
        / "orders.csv"
    )

    rows = read_csv_rows(file_path)

    for row in rows:
        row["order_id"] = int(row["order_id"])
        row["customer_id"] = int(row["customer_id"])
        row["product_id"] = int(row["product_id"])
        row["quantity"] = int(row["quantity"])

    context.log.info(
        f"Loaded {len(rows)} orders"
    )

    return rows


@dg.asset
def bronze_orders(
    context: dg.AssetExecutionContext,
    orders,
):
    return orders


@dg.asset
def silver_orders(
    context: dg.AssetExecutionContext,
    bronze_orders,
):
    cleaned = []

    for order in bronze_orders:
        if (
            order["order_id"] is None
            or order["customer_id"] is None
            or order["product_id"] is None
            or order["quantity"] <= 0
            or not order["order_date"].strip()
        ):
            continue

        cleaned.append(
            {
                "order_id": int(order["order_id"]),
                "customer_id": int(order["customer_id"]),
                "product_id": int(order["product_id"]),
                "quantity": int(order["quantity"]),
                "order_date": order["order_date"].strip(),
            }
        )

    context.log.info(
        f"Silver orders: {len(cleaned)} rows"
    )

    return cleaned


# ============================================================
# PAYMENTS
# ============================================================

@dg.asset
def payments(context: dg.AssetExecutionContext):
    file_path = (
        Path(__file__).resolve().parent.parent
        / "data"
        / "raw"
        / "payments"
        / "payments.csv"
    )

    rows = read_csv_rows(file_path)

    for row in rows:
        row["payment_id"] = int(row["payment_id"])
        row["order_id"] = int(row["order_id"])
        row["amount"] = float(row["amount"])

    context.log.info(
        f"Loaded {len(rows)} payments"
    )

    return rows


@dg.asset
def bronze_payments(
    context: dg.AssetExecutionContext,
    payments,
):
    return payments


@dg.asset
def silver_payments(
    context: dg.AssetExecutionContext,
    bronze_payments,
):
    cleaned = []

    for payment in bronze_payments:
        if (
            payment["payment_id"] is None
            or payment["order_id"] is None
            or payment["amount"] < 0
            or not payment["payment_status"].strip()
        ):
            continue

        cleaned.append(
            {
                "payment_id": int(payment["payment_id"]),
                "order_id": int(payment["order_id"]),
                "payment_method": payment["payment_method"].strip().title(),
                "payment_status": payment["payment_status"].strip().title(),
                "amount": float(payment["amount"]),
            }
        )

    context.log.info(
        f"Silver payments: {len(cleaned)} rows"
    )

    return cleaned


# ============================================================
# STORES
# ============================================================

@dg.asset
def stores(context: dg.AssetExecutionContext):
    file_path = (
        Path(__file__).resolve().parent.parent
        / "data"
        / "raw"
        / "stores"
        / "stores.csv"
    )

    rows = read_csv_rows(file_path)

    for row in rows:
        row["store_id"] = int(row["store_id"])

    context.log.info(
        f"Loaded {len(rows)} stores"
    )

    return rows


@dg.asset
def bronze_stores(
    context: dg.AssetExecutionContext,
    stores,
):
    return stores


@dg.asset
def silver_stores(
    context: dg.AssetExecutionContext,
    bronze_stores,
):
    cleaned = []

    for store in bronze_stores:
        cleaned.append(
            {
                "store_id": int(store["store_id"]),
                "store_name": store["store_name"].strip().title(),
                "city": store["city"].strip().title(),
                "state": store["state"].strip().title(),
            }
        )

    context.log.info(
        f"Silver stores: {len(cleaned)} rows"
    )

    return cleaned


# ============================================================
# LOCAL GOLD ASSETS
# ============================================================

@dg.asset
def gold_sales(
    context: dg.AssetExecutionContext,
    silver_orders,
    silver_customers,
    silver_products,
    silver_payments,
):
    customer_lookup = {
        row["customer_id"]: row
        for row in silver_customers
    }

    product_lookup = {
        row["product_id"]: row
        for row in silver_products
    }

    payment_lookup = {
        row["order_id"]: row
        for row in silver_payments
    }

    result = []

    for order in silver_orders:
        customer = customer_lookup.get(order["customer_id"])
        product = product_lookup.get(order["product_id"])
        payment = payment_lookup.get(order["order_id"])

        if not customer or not product:
            continue

        total_amount = (
            order["quantity"] * product["price"]
        )

        result.append(
            {
                "order_id": order["order_id"],
                "order_date": order["order_date"],
                "customer_id": order["customer_id"],
                "customer_name": customer["name"],
                "customer_city": customer["city"],
                "product_id": order["product_id"],
                "product_name": product["product_name"],
                "category": product["category"],
                "quantity": order["quantity"],
                "unit_price": product["price"],
                "total_amount": total_amount,
                "payment_method": (
                    payment["payment_method"]
                    if payment
                    else None
                ),
                "payment_status": (
                    payment["payment_status"]
                    if payment
                    else None
                ),
            }
        )

    context.log.info(
        f"Gold sales rows: {len(result)}"
    )

    return result


@dg.asset
def gold_daily_sales(
    context: dg.AssetExecutionContext,
    gold_sales,
):
    grouped = {}

    for row in gold_sales:
        date = row["order_date"]

        if date not in grouped:
            grouped[date] = {
                "order_count": 0,
                "item_count": 0,
                "total_sales": 0.0,
            }

        grouped[date]["order_count"] += 1
        grouped[date]["item_count"] += row["quantity"]
        grouped[date]["total_sales"] += row["total_amount"]

    result = []

    for date, values in sorted(grouped.items()):
        result.append(
            {
                "order_date": date,
                "order_count": values["order_count"],
                "item_count": values["item_count"],
                "total_sales": values["total_sales"],
                "average_order_value": (
                    values["total_sales"]
                    / values["order_count"]
                ),
            }
        )

    return result


@dg.asset
def gold_category_sales(
    context: dg.AssetExecutionContext,
    gold_sales,
):
    grouped = {}

    for row in gold_sales:
        category = row["category"]

        if category not in grouped:
            grouped[category] = {
                "order_count": 0,
                "item_count": 0,
                "total_sales": 0.0,
            }

        grouped[category]["order_count"] += 1
        grouped[category]["item_count"] += row["quantity"]
        grouped[category]["total_sales"] += row["total_amount"]

    result = []

    for category, values in sorted(grouped.items()):
        result.append(
            {
                "category": category,
                "order_count": values["order_count"],
                "item_count": values["item_count"],
                "total_sales": values["total_sales"],
                "average_order_value": (
                    values["total_sales"]
                    / values["order_count"]
                ),
            }
        )

    return result


# ============================================================
# MINIO CONNECTION
# ============================================================

@dg.asset
def minio_connection_test(
    context: dg.AssetExecutionContext,
):
    client = get_minio_client()

    bucket_name = "retailx"

    if not client.bucket_exists(bucket_name):
        raise Exception(
            f"MinIO bucket '{bucket_name}' does not exist"
        )

    objects = list(
        client.list_objects(
            bucket_name,
            recursive=True,
        )
    )

    context.log.info(
        f"Connected to MinIO bucket: {bucket_name}"
    )

    context.log.info(
        f"Objects currently in bucket: {len(objects)}"
    )

    context.add_output_metadata(
        {
            "bucket": bucket_name,
            "endpoint": "localhost:19020",
            "object_count": len(objects),
        }
    )

    return True


# ============================================================
# MINIO BRONZE ASSETS
# ============================================================

@dg.asset
def minio_bronze_customers(
    context: dg.AssetExecutionContext,
    silver_customers,
):
    client = get_minio_client()

    upload_csv_to_minio(
        client,
        "retailx",
        "bronze/customers.csv",
        silver_customers,
        ["customer_id", "name", "city"],
    )

    context.log.info(
        "Uploaded customers to MinIO Bronze"
    )

    return True


@dg.asset
def minio_bronze_products(
    context: dg.AssetExecutionContext,
    silver_products,
):
    client = get_minio_client()

    upload_csv_to_minio(
        client,
        "retailx",
        "bronze/products.csv",
        silver_products,
        [
            "product_id",
            "product_name",
            "category",
            "price",
        ],
    )

    context.log.info(
        "Uploaded products to MinIO Bronze"
    )

    return True


@dg.asset
def minio_bronze_orders(
    context: dg.AssetExecutionContext,
    silver_orders,
):
    client = get_minio_client()

    upload_csv_to_minio(
        client,
        "retailx",
        "bronze/orders.csv",
        silver_orders,
        [
            "order_id",
            "customer_id",
            "product_id",
            "quantity",
            "order_date",
        ],
    )

    context.log.info(
        "Uploaded orders to MinIO Bronze"
    )

    return True


@dg.asset
def minio_bronze_payments(
    context: dg.AssetExecutionContext,
    silver_payments,
):
    client = get_minio_client()

    upload_csv_to_minio(
        client,
        "retailx",
        "bronze/payments.csv",
        silver_payments,
        [
            "payment_id",
            "order_id",
            "payment_method",
            "payment_status",
            "amount",
        ],
    )

    context.log.info(
        "Uploaded payments to MinIO Bronze"
    )

    return True


# ============================================================
# SPARK SILVER - CUSTOMERS
# ============================================================

@dg.asset(
    name="spark_silver_customers",
    description="Runs the Spark job that transforms MinIO Bronze customers into Silver Parquet.",
    retry_policy=dg.RetryPolicy(
        max_retries=2,
        delay=10,
    ),
)
def spark_silver_customers(
    context: dg.AssetExecutionContext,
):
    context.log.info(
        "Starting Spark Silver Customers job"
    )

    result = subprocess.run(
        [
            "docker",
            "exec",
            "retailx-spark-worker",
            "bash",
            "-c",
            (
                "/opt/spark/bin/spark-submit "
                "--master spark://172.17.0.2:7077 "
                "/opt/spark/jobs/customers_silver.py"
            ),
        ],
        capture_output=True,
        text=True,
    )

    context.log.info(result.stdout)

    if result.returncode != 0:
        context.log.error(result.stderr)
        raise Exception(
            "Spark Silver Customers job failed"
        )

    context.log.info(
        "Spark Silver Customers completed successfully"
    )

    context.add_output_metadata(
        {
            "spark_job": "RetailX Customers Silver",
            "input_path": "s3a://retailx/bronze/customers.csv",
            "output_path": "s3a://retailx/silver/customers",
            "spark_master": "spark://172.17.0.2:7077",
            "storage": "MinIO",
            "format": "Parquet",
        }
    )


# ============================================================
# SPARK SILVER - PRODUCTS / ORDERS / PAYMENTS
# ============================================================

@dg.asset(
    name="spark_silver_remaining",
    description="Runs Spark Silver transformations for products, orders and payments.",
    retry_policy=dg.RetryPolicy(
        max_retries=2,
        delay=10,
    ),
)
def spark_silver_remaining(
    context: dg.AssetExecutionContext,
):
    context.log.info(
        "Starting Spark Silver Products/Orders/Payments job"
    )

    result = subprocess.run(
        [
            "docker",
            "exec",
            "retailx-spark-worker",
            "bash",
            "-c",
            (
                "/opt/spark/bin/spark-submit "
                "--master spark://172.17.0.2:7077 "
                "/opt/spark/jobs/remaining_silver.py"
            ),
        ],
        capture_output=True,
        text=True,
    )

    context.log.info(result.stdout)

    if result.returncode != 0:
        context.log.error(result.stderr)
        raise Exception(
            "Spark Remaining Silver job failed"
        )

    context.log.info(
        "Spark Products, Orders and Payments Silver completed successfully"
    )

    context.add_output_metadata(
        {
            "spark_job": "RetailX Products Orders Payments Silver",
            "products_output": "s3a://retailx/silver/products",
            "orders_output": "s3a://retailx/silver/orders",
            "payments_output": "s3a://retailx/silver/payments",
            "spark_master": "spark://172.17.0.2:7077",
            "storage": "MinIO",
            "format": "Parquet",
        }
    )


# ============================================================
# SPARK GOLD
# ============================================================

@dg.asset(
    deps=[
        "spark_silver_customers",
        "spark_silver_remaining",
    ],
    name="spark_gold_sales",
    description="Runs Spark Gold transformations for sales, daily sales and category sales.",
    retry_policy=dg.RetryPolicy(
        max_retries=2,
        delay=10,
    ),
)
def spark_gold_sales(
    context: dg.AssetExecutionContext,
):
    context.log.info(
        "Starting Spark Gold Sales job"
    )

    result = subprocess.run(
        [
            "docker",
            "exec",
            "retailx-spark-worker",
            "bash",
            "-c",
            (
                "/opt/spark/bin/spark-submit "
                "--master spark://172.17.0.2:7077 "
                "/opt/spark/jobs/gold_sales.py"
            ),
        ],
        capture_output=True,
        text=True,
    )

    context.log.info(result.stdout)

    if result.returncode != 0:
        context.log.error(result.stderr)
        raise Exception(
            "Spark Gold Sales job failed"
        )

    context.log.info(
        "Spark Gold Sales completed successfully"
    )

    context.add_output_metadata(
        {
            "spark_job": "RetailX Gold Sales",
            "gold_sales_path": "s3a://retailx/gold/sales",
            "daily_sales_path": "s3a://retailx/gold/daily_sales",
            "category_sales_path": "s3a://retailx/gold/category_sales",
            "spark_master": "spark://172.17.0.2:7077",
            "storage": "MinIO",
            "format": "Parquet",
        }
    )


# ============================================================
# GOLD QUALITY CHECK
# ============================================================

@dg.asset_check(
    asset=spark_gold_sales,
    description="Validates the Spark Gold sales pipeline output.",
)
def spark_gold_sales_quality_check(
    context: dg.AssetCheckExecutionContext,
):
    context.log.info(
        "Running Gold Sales data quality checks"
    )

    result = subprocess.run(
        [
            "docker",
            "exec",
            "retailx-spark-worker",
            "bash",
            "-c",
            """
            /opt/spark/bin/spark-submit \
            --master spark://172.17.0.2:7077 \
            /opt/spark/jobs/gold_quality_check.py
            """,
        ],
        capture_output=True,
        text=True,
    )

    context.log.info(result.stdout)

    if result.returncode != 0:
        context.log.error(result.stderr)

        return dg.AssetCheckResult(
            passed=False,
            metadata={
                "reason": "Gold quality check job failed"
            },
        )

    return dg.AssetCheckResult(
        passed=True,
        metadata={
            "check": "Gold sales validation",
            "status": "passed",
        },
    )


# ============================================================
# ICEBERG - SILVER TABLES
# ============================================================

@dg.asset(
    deps=[
        "spark_silver_customers",
        "spark_silver_remaining",
    ],
    name="iceberg_silver_tables",
    description="Creates RetailX Silver Iceberg tables from MinIO Silver Parquet data.",
    retry_policy=dg.RetryPolicy(
        max_retries=2,
        delay=10,
    ),
)
def iceberg_silver_tables(
    context: dg.AssetExecutionContext,
):
    context.log.info(
        "Starting Silver Iceberg tables job"
    )

    result = subprocess.run(
        [
            "docker",
            "exec",
            "retailx-spark-worker",
            "bash",
            "-c",
            (
                "/opt/spark/bin/spark-submit "
                "--master spark://172.17.0.2:7077 "
                "/opt/spark/jobs/silver_to_iceberg.py"
            ),
        ],
        capture_output=True,
        text=True,
    )

    context.log.info(result.stdout)

    if result.returncode != 0:
        context.log.error(result.stderr)
        raise Exception(
            "Silver Iceberg job failed"
        )

    context.log.info(
        "Silver Iceberg tables created successfully"
    )

    context.add_output_metadata(
        {
            "iceberg_catalog": "retailx_catalog",
            "namespace": "retailx",
            "tables": (
                "customers, products, orders, payments"
            ),
            "warehouse": (
                "s3a://retailx/iceberg/warehouse"
            ),
            "storage": "MinIO",
            "format": "Apache Iceberg",
        }
    )


# ============================================================
# ICEBERG - GOLD SALES
# ============================================================

@dg.asset(
    deps=["iceberg_silver_tables"],
    name="iceberg_gold_sales",
    description="Creates the RetailX Gold Sales Iceberg table.",
    retry_policy=dg.RetryPolicy(
        max_retries=2,
        delay=10,
    ),
)
def iceberg_gold_sales(
    context: dg.AssetExecutionContext,
):
    context.log.info(
        "Starting Gold Sales Iceberg job"
    )

    result = subprocess.run(
        [
            "docker",
            "exec",
            "retailx-spark-worker",
            "bash",
            "-c",
            (
                "/opt/spark/bin/spark-submit "
                "--master spark://172.17.0.2:7077 "
                "/opt/spark/jobs/gold_sales_iceberg.py"
            ),
        ],
        capture_output=True,
        text=True,
    )

    context.log.info(result.stdout)

    if result.returncode != 0:
        context.log.error(result.stderr)
        raise Exception(
            "Gold Iceberg job failed"
        )

    context.log.info(
        "Gold Sales Iceberg table created successfully"
    )

    context.add_output_metadata(
        {
            "iceberg_catalog": "retailx_catalog",
            "table": "retailx.sales",
            "warehouse": (
                "s3a://retailx/iceberg/warehouse"
            ),
            "storage": "MinIO",
            "format": "Apache Iceberg",
            "rows": 15,
            "total_sales": 199800.0,
        }
    )


# ============================================================
# SPARK PIPELINE JOB
# ============================================================

spark_pipeline_job = dg.define_asset_job(
    name="retailx_spark_pipeline_job",
    selection=dg.AssetSelection.assets(
        spark_silver_customers,
        spark_silver_remaining,
        spark_gold_sales,
    ),
)


# ============================================================
# ICEBERG PIPELINE JOB
# ============================================================

iceberg_pipeline_job = dg.define_asset_job(
    name="retailx_iceberg_pipeline_job",
    selection=dg.AssetSelection.assets(
        iceberg_silver_tables,
        iceberg_gold_sales,
    ),
)


# ============================================================
# DAILY SCHEDULE
# ============================================================

@schedule(
    cron_schedule="0 9 * * *",
    job=spark_pipeline_job,
    execution_timezone="Asia/Kolkata",
)
def retailx_daily_schedule():
    return {}


# ============================================================
# DEFINITIONS
# ============================================================

defs = dg.Definitions(
    assets=[
        # Local raw / transformation assets
        customers,
        bronze_customers,
        silver_customers,

        products,
        bronze_products,
        silver_products,

        orders,
        bronze_orders,
        silver_orders,

        payments,
        bronze_payments,
        silver_payments,

        stores,
        bronze_stores,
        silver_stores,

        # Local Gold
        gold_sales,
        gold_daily_sales,
        gold_category_sales,

        # MinIO
        minio_connection_test,
        minio_bronze_customers,
        minio_bronze_products,
        minio_bronze_orders,
        minio_bronze_payments,

        # Spark Silver / Gold
        spark_silver_customers,
        spark_silver_remaining,
        spark_gold_sales,

        # Iceberg
        iceberg_silver_tables,
        iceberg_gold_sales,
    ],

    asset_checks=[
        customers_quality_check,
        spark_gold_sales_quality_check,
    ],

    resources={
        "io_manager": LocalIOManager(),
    },

    jobs=[
        spark_pipeline_job,
        iceberg_pipeline_job,
    ],

    schedules=[
        retailx_daily_schedule,
    ],
)
