# RetailX Data Orchestration & Lakehouse Platform

An end-to-end Data Engineering platform that demonstrates modern data ingestion, orchestration, distributed processing, lakehouse storage, data quality, and SQL analytics.

## Architecture

```text
                 RAW DATA
              CSV / Source Data
                     |
                     v
                ┌─────────┐
                │ Dagster │
                │Orchestrator
                └────┬────┘
                     |
                     v
              ┌─────────────┐
              │    MinIO    │
              │ Bronze Layer│
              └──────┬──────┘
                     |
                     v
              ┌─────────────┐
              │ Apache Spark│
              │ Transformation
              └──────┬──────┘
                     |
             ┌───────┴───────┐
             v               v
        Silver Layer      Gold Layer
             |               |
             └───────┬───────┘
                     v
             ┌───────────────┐
             │ Apache Iceberg│
             │ Lakehouse      │
             └───────┬───────┘
                     |
                     v
              ┌─────────────┐
              │    Dremio   │
              │ SQL Analytics│
              └─────────────┘
ORDER BY total_sales DESC;
