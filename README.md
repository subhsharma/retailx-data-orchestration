# 🚀 RetailX Data Orchestration & Lakehouse Platform

> **End-to-End Modern Data Engineering Project**
> Dagster • Apache Spark • MinIO • Apache Iceberg • Dremio • Docker • Python • SQL

---

## 📌 1. Project Overview

**RetailX Data Orchestration & Lakehouse Platform** is an end-to-end Data Engineering project that demonstrates how raw retail data can be ingested, orchestrated, transformed, validated, stored in a lakehouse, and exposed for SQL analytics.

The platform follows a modern **Bronze → Silver → Gold** architecture.

### Main Pipeline

```text
Raw CSV Data
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
MinIO Lakehouse
     ↓
Dremio
     ↓
SQL Analytics
```

---

# 🏗️ 2. Architecture

```text
                         RETAILX DATA PLATFORM

 ┌────────────────────┐
 │     Raw CSV Data   │
 │                    │
 │ Customers          │
 │ Products           │
 │ Orders             │
 │ Payments           │
 │ Stores             │
 └──────────┬─────────┘
            │
            ▼
 ┌────────────────────┐
 │      Dagster       │
 │                    │
 │ Assets             │
 │ Dependencies       │
 │ Scheduling         │
 │ Retries            │
 │ Data Quality       │
 └──────────┬─────────┘
            │
            ▼
 ┌────────────────────┐
 │       MinIO        │
 │    Bronze Layer    │
 │   S3-Compatible    │
 │      Storage       │
 └──────────┬─────────┘
            │
            ▼
 ┌────────────────────┐
 │   Apache Spark     │
 │                    │
 │ Transformations    │
 │ Joins              │
 │ Aggregations       │
 └──────────┬─────────┘
            │
       ┌────┴────┐
       ▼         ▼
 ┌──────────┐ ┌──────────┐
 │  Silver  │ │   Gold   │
 │  Layer   │ │  Layer   │
 └────┬─────┘ └────┬─────┘
      │             │
      └──────┬──────┘
             ▼
 ┌────────────────────┐
 │   Apache Iceberg   │
 │                    │
 │ Lakehouse Tables   │
 │ Metadata           │
 │ Snapshots          │
 │ Schema Management  │
 └──────────┬─────────┘
            │
            ▼
 ┌────────────────────┐
 │      Dremio        │
 │                    │
 │ SQL Query Engine   │
 │ Analytics Layer    │
 └──────────┬─────────┘
            │
            ▼
 ┌────────────────────┐
 │ Business Analytics │
 └────────────────────┘
```

---

# 🧰 3. Technology Stack

| Technology                | Purpose                                   |
| ------------------------- | ----------------------------------------- |
| **Python**                | Data engineering and pipeline development |
| **Dagster**               | Workflow orchestration                    |
| **Apache Spark 3.5.6**    | Distributed data processing               |
| **MinIO**                 | S3-compatible object storage              |
| **Apache Iceberg 1.10.0** | Lakehouse table format                    |
| **Dremio**                | SQL analytics and query layer             |
| **Docker**                | Containerized infrastructure              |
| **SQL**                   | Data analysis                             |
| **Git**                   | Version control                           |
| **GitHub**                | Source code hosting                       |
| **PowerShell**            | Windows development environment           |

---

# 📂 4. Source Data

The project uses five retail datasets.

### 👥 Customers

```text
customer_id
name
city
```

### 📦 Products

```text
product_id
product_name
category
price
```

### 🛒 Orders

```text
order_id
customer_id
product_id
quantity
order_date
```

### 💳 Payments

```text
payment_id
order_id
payment_method
payment_status
amount
```

### 🏪 Stores

```text
store_id
store_name
city
state
```

---

# 🗂️ 5. Project Structure

```text
retailx-data-orchestration/
│
├── .gitignore
├── README.md
│
└── retailx_orchestration/
    │
    ├── pyproject.toml
    ├── README.md
    │
    ├── data/
    │   ├── raw/
    │   │   ├── customers.csv
    │   │   ├── products/
    │   │   │   └── products.csv
    │   │   ├── orders/
    │   │   │   └── orders.csv
    │   │   ├── payments/
    │   │   │   └── payments.csv
    │   │   └── stores/
    │   │       └── stores.csv
    │   │
    │   └── lake/
    │       └── bronze/
    │
    ├── retailx_orchestration/
    │   ├── __init__.py
    │   ├── assets.py
    │   └── definitions.py
    │
    ├── retailx_orchestration_tests/
    │   ├── __init__.py
    │   └── test_assets.py
    │
    └── spark_jobs/
        ├── customers_silver.py
        ├── remaining_silver.py
        ├── gold_sales.py
        ├── gold_quality_check.py
        ├── silver_to_iceberg.py
        └── gold_sales_iceberg.py
```

---

# 🥉 6. Bronze Layer

The Bronze layer stores raw data in **MinIO** using an S3-compatible interface.

```text
retailx/
└── bronze/
    ├── customers.csv
    ├── products.csv
    ├── orders.csv
    └── payments.csv
```

### Purpose

* Preserve raw data
* Provide a reproducible starting point
* Separate ingestion from transformation
* Enable reprocessing
* Follow the Medallion Architecture

---

# 🥈 7. Silver Layer

Apache Spark processes the Bronze datasets and creates cleaned Silver datasets.

```text
silver/
├── customers
├── products
├── orders
└── payments
```

### Processing

Spark performs:

* Data reading
* Schema handling
* Data type conversion
* Data cleaning
* Transformations
* Dataset writing
* Preparation for downstream joins

---

# 🥇 8. Gold Layer

The Gold layer contains business-ready analytical datasets.

```text
gold/
├── sales
├── daily_sales
└── category_sales
```

### Gold Sales

The main Gold Sales dataset combines:

* Orders
* Customers
* Products
* Payments
* Quantity
* Unit price
* Total amount
* Payment method
* Payment status

### Final Result

```text
Gold Sales Records: 15
Total Sales:        199,800
```

---

# 🔄 9. Dagster Orchestration

Dagster is the orchestration layer of the project.

It manages:

* Assets
* Asset dependencies
* Pipeline execution
* Retries
* Scheduling
* Data quality checks
* Spark execution
* Iceberg execution

### Spark Pipeline

```text
spark_silver_customers
          │
          ├──────────────┐
          │              │
          ▼              ▼
spark_silver_remaining  ...
          │
          ▼
   spark_gold_sales
```

### Retry Configuration

Spark pipeline assets are configured with retry handling to improve reliability when Spark execution fails.

### Schedule

```text
Frequency: Daily
Time:      09:00
Timezone:  Asia/Kolkata
```

---

# ⚡ 10. Apache Spark

Apache Spark 3.5.6 is used for distributed data processing.

### Spark Jobs

```text
spark_jobs/
│
├── customers_silver.py
├── remaining_silver.py
├── gold_sales.py
├── gold_quality_check.py
├── silver_to_iceberg.py
└── gold_sales_iceberg.py
```

### Processing Flow

```text
MinIO Bronze
     │
     ▼
Apache Spark
     │
     ├── Customers → Silver Customers
     ├── Products  → Silver Products
     ├── Orders    → Silver Orders
     └── Payments  → Silver Payments
                          │
                          ▼
                     Gold Sales
```

---

# 🧊 11. Apache Iceberg Lakehouse

Apache Iceberg is used as the lakehouse table format.

### Iceberg Warehouse

```text
s3a://retailx/iceberg/warehouse
```

### Iceberg Tables

```text
retailx
├── customers
├── products
├── orders
├── payments
└── sales
```

### Gold Sales Schema

```text
order_id
order_date
customer_id
customer_name
customer_city
product_id
product_name
category
quantity
unit_price
total_amount
payment_method
payment_status
```

### Why Iceberg?

Iceberg provides:

* Table metadata
* Snapshots
* Schema management
* Reliable table operations
* Lakehouse table organization
* Separation of compute and storage

---

# 🔎 12. Dremio Analytics

Dremio is used as the SQL analytics layer.

Dremio connects to MinIO and queries the Iceberg tables.

### Example Query

```sql
SELECT
    category,
    COUNT(*) AS order_lines,
    SUM(quantity) AS total_items,
    SUM(total_amount) AS total_sales
FROM sales
GROUP BY category
ORDER BY total_sales DESC;
```

---

# 📊 13. Category Sales Results

| Category    | Order Lines | Total Items | Total Sales |
| ----------- | ----------: | ----------: | ----------: |
| Electronics |           8 |          15 |     151,400 |
| Furniture   |           4 |           5 |      45,500 |
| Accessories |           1 |           1 |       1,800 |
| Stationery  |           2 |          15 |       1,100 |
| **Total**   |      **15** |      **36** | **199,800** |

---

# 📅 14. Daily Sales Results

| Date       | Orders |  Items |     Revenue |
| ---------- | -----: | -----: | ----------: |
| 2026-09-01 |      2 |      3 |      58,000 |
| 2026-09-02 |      2 |      4 |      10,900 |
| 2026-09-03 |      2 |      2 |      12,500 |
| 2026-09-04 |      2 |      7 |      24,600 |
| 2026-09-05 |      2 |     11 |       2,300 |
| 2026-09-06 |      2 |      3 |      60,000 |
| 2026-09-07 |      2 |      4 |      14,500 |
| 2026-09-08 |      1 |      2 |      17,000 |
| **Total**  | **15** | **36** | **199,800** |

---

# ✅ 15. Data Quality

The project contains automated data quality checks.

### Customer Quality Check

Validates:

* `customer_id` is not null
* `name` is not empty
* `city` is not empty

### Gold Sales Quality Check

Validates:

* Row count is greater than zero
* `order_id` is not null
* `product_id` is not null
* `customer_id` is not null
* Sales amount is not negative

Successful execution produces:

```text
QUALITY CHECK PASSED
```

---

# 🐳 16. Docker Infrastructure

The project uses Docker for infrastructure.

### MinIO

```text
Container: retailx-minio
API:       localhost:19020
Console:   localhost:19021
```

### Spark Master

```text
Container: retailx-spark
UI:        localhost:18080
Master:    port 17077
```

### Spark Worker

```text
Container: retailx-spark-worker
```

### Dremio

```text
Container: retailx-dremio
UI:        localhost:9047
```

### Dagster

```text
UI: http://localhost:3000
```

---

# 🔁 17. Complete End-to-End Pipeline

```text
                 RAW DATA
                    │
                    ▼
               ┌─────────┐
               │ Dagster │
               └────┬────┘
                    │
                    ▼
               ┌─────────┐
               │  MinIO  │
               │ Bronze  │
               └────┬────┘
                    │
                    ▼
               ┌─────────┐
               │  Spark  │
               └────┬────┘
                    │
             ┌──────┴──────┐
             ▼             ▼
        ┌─────────┐   ┌─────────┐
        │ Silver  │   │  Gold   │
        └────┬────┘   └────┬────┘
             │             │
             └──────┬──────┘
                    ▼
             ┌─────────────┐
             │   Iceberg   │
             │  Lakehouse  │
             └──────┬──────┘
                    │
                    ▼
               ┌─────────┐
               │ Dremio  │
               └────┬────┘
                    │
                    ▼
             SQL Analytics
```

---

# 📈 18. Project Results

| Metric             |  Result |
| ------------------ | ------: |
| Customers          |      10 |
| Products           |      10 |
| Orders             |      15 |
| Payments           |      15 |
| Gold Sales Records |      15 |
| Total Sales        | 199,800 |
| Categories         |       4 |
| Daily Sales Dates  |       8 |

---

# 🎯 19. Engineering Concepts Demonstrated

This project demonstrates practical experience with:

* Data Engineering
* ETL / ELT
* Medallion Architecture
* Data Lakehouse
* Workflow Orchestration
* Dagster Assets
* Asset Dependencies
* Pipeline Scheduling
* Retry Handling
* Data Quality
* Apache Spark
* Distributed Processing
* MinIO
* S3-Compatible Storage
* Apache Iceberg
* Dremio
* SQL Analytics
* Docker
* Python
* Git
* GitHub

---

# 🚀 20. Setup

## Prerequisites

Install:

* Python 3.11+
* Docker Desktop
* Git
* Java
* Apache Spark-compatible environment

## Clone Repository

```bash
git clone https://github.com/subhsharma/retailx-data-orchestration.git
cd retailx-data-orchestration
```

## Create Virtual Environment

Windows PowerShell:

```powershell
python -m venv .venv
```

Activate:

```powershell
.venv\Scripts\Activate.ps1
```

## Install Project

```powershell
cd retailx_orchestration
pip install -e .
```

## Start Infrastructure

Start the required Docker services:

```text
MinIO
Spark Master
Spark Worker
Dremio
```

## Start Dagster

```powershell
dagster dev
```

Open:

```text
http://localhost:3000
```

---

# 🧪 21. Validation

The project was validated through:

* Dagster asset execution
* Spark job execution
* MinIO storage verification
* Iceberg table creation
* Iceberg table querying
* Dremio SQL queries
* Data quality checks
* Git repository validation

---

# 💼 22. Resume Description

### RetailX Data Orchestration & Lakehouse Platform

Built an end-to-end Data Engineering platform using **Dagster, Apache Spark, MinIO, Apache Iceberg, Docker, and Dremio**. Implemented Bronze, Silver, and Gold data layers, orchestrated Spark and Iceberg workflows using Dagster, added automated data quality checks, stored analytical datasets as Iceberg tables on S3-compatible MinIO storage, and enabled SQL-based analytics through Dremio. Processed retail customers, products, orders, and payments data to generate sales, daily revenue, and category-level analytics with total sales of **199,800**.

---

# 🎤 23. Interview Explanation

### What did you build?

I built an end-to-end retail Data Engineering platform using Dagster, Spark, MinIO, Iceberg, and Dremio.

### How does the pipeline work?

Raw CSV data is first stored in the Bronze layer using MinIO. Dagster orchestrates the workflow. Spark reads the Bronze data, performs transformations, and creates Silver and Gold datasets. The processed data is stored as Iceberg tables in the lakehouse. Dremio then connects to the lakehouse and provides a SQL analytics layer.

### Why Dagster?

Dagster is used for workflow orchestration, asset management, dependencies, scheduling, retries, and data quality checks.

### Why Spark?

Spark is used for distributed data processing, transformations, joins, and aggregations.

### Why MinIO?

MinIO provides S3-compatible object storage for the local lakehouse environment.

### Why Iceberg?

Iceberg provides a modern lakehouse table format with metadata, snapshots, schema management, and reliable table operations.

### Why Dremio?

Dremio provides a SQL query and analytics layer on top of the lakehouse.

---

# 🌟 24. Future Improvements

The platform can be extended with:

* AWS S3
* AWS Glue
* Amazon Athena
* Amazon EMR
* AWS Lambda
* Snowflake
* dbt
* Apache Kafka
* Apache Airflow
* Kubernetes
* CI/CD
* GitHub Actions
* Cloud monitoring
* Alerting
* Incremental processing
* Partitioning
* Schema evolution
* Larger datasets
* Production cloud deployment

---

# 👨‍💻 25. Author

**Yogesh Bhardwaj**

**Data Engineer**

### Core Skills

```text
Python
SQL
Apache Spark
Dagster
AWS
Snowflake
dbt
Kafka
Airflow
Docker
MinIO
Apache Iceberg
Dremio
ETL / ELT
Data Lakehouse
Data Quality
Git
GitHub
```

---

# ⭐ 26. Project Status

**Status: Completed ✅**

The project successfully demonstrates an end-to-end Data Engineering workflow from **raw data ingestion → orchestration → Bronze → Spark transformation → Silver → Gold → Iceberg Lakehouse → Dremio SQL Analytics → Data Quality Validation**.
