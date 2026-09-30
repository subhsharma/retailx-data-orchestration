# RetailX Data Orchestration & Lakehouse Platform

An end-to-end data engineering pipeline built using Dagster, Apache Spark, MinIO, Apache Iceberg, and Dremio.

## Architecture

CSV / Raw Data
↓
Dagster
↓
MinIO / Bronze
↓
Apache Spark
↓
Silver
↓
Gold
↓
Apache Iceberg
↓
MinIO Object Storage
↓
Dremio
↓
SQL Analytics

## Technology Stack

- Python
- Dagster
- Apache Spark
- MinIO
- Apache Iceberg
- Dremio
- Docker
- PowerShell
- SQL

## Pipeline Layers

### Bronze

Raw datasets are stored in MinIO in S3-compatible object storage.

### Silver

Apache Spark cleans and transforms customers, products, orders, and payments.

### Gold

Spark creates business-ready sales datasets including:

- Sales
- Daily sales
- Category sales

### Iceberg

Apache Iceberg provides table metadata, snapshots, and schema-managed tables for the Silver and Gold datasets.

### Dremio

Dremio provides the SQL analytics layer for querying the Gold data.

## Orchestration

Dagster orchestrates the Spark pipeline and Iceberg pipeline.

The project includes:

- Dagster assets
- Asset dependencies
- Asset checks
- Spark orchestration
- Iceberg orchestration
- Daily schedule

## Data Quality

The pipeline includes quality checks for:

- Null customer IDs
- Null order IDs
- Null product IDs
- Negative sales amounts
- Empty datasets

## Analytics Results

The Gold sales dataset contains 15 sales records with total sales of 199800.

Dremio successfully executes analytical queries such as:

```sql
SELECT
    category,
    COUNT(*) AS order_lines,
    SUM(quantity) AS total_items,
    SUM(total_amount) AS total_sales
FROM sales
GROUP BY category
ORDER BY total_sales DESC;