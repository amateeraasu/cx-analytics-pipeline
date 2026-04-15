# Databricks — Medallion Architecture

This folder ports the dbt + DuckDB pipeline to Databricks using Delta Lake's
medallion (Bronze / Silver / Gold) pattern.

## Architecture

```
CSV files (DBFS upload)
        │
        ▼
┌─────────────────┐
│   olist_bronze  │  01_bronze_ingest.py
│   9 Delta tables│  Raw CSVs → typed Delta with metadata columns
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   olist_silver  │  02_silver_transform.py
│   3 Delta tables│  Joins · delivery KPIs · customer aggregation · ML features
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   olist_gold    │  03_gold_kpis.py
│   3 Delta tables│  Monthly KPIs · customer segments · delivery by state
└─────────────────┘
```

## dbt ↔ Databricks model mapping

| dbt layer | dbt model | Databricks table |
|---|---|---|
| Staging | stg_orders + stg_payments + stg_reviews | `bronze.raw_orders` + joins in Silver |
| Staging | stg_customers | `bronze.raw_customers` |
| Intermediate | int_orders_enriched | `silver.silver_orders` |
| Intermediate | int_customer_orders | `silver.silver_customers` |
| Intermediate | int_churn_features | `silver.silver_churn_features` |
| Mart | cx_satisfaction_summary | `gold.gold_monthly_kpis` |
| Mart | dim_customers | `gold.gold_customer_segments` |
| Mart | fct_orders (delivery slice) | `gold.gold_delivery_by_state` |

## Setup (Databricks Community Edition — free)

### 1. Create account
Sign up at https://community.cloud.databricks.com/

### 2. Create a cluster
- Compute → Create Cluster
- Cluster Mode: Single Node
- Databricks Runtime: 13.3 LTS (includes Delta Lake, PySpark)
- Node type: default Community Edition node

### 3. Upload CSV files to DBFS
- Data → Add Data → Upload Files
- Target path: `dbfs:/FileStore/olist/raw/`
- Upload all 9 Olist CSV files (download from Kaggle first)

### 4. Import notebooks
- Workspace → Import → select each `.py` file from this folder
- Databricks recognises the `# Databricks notebook source` header

### 5. Run in order
```
01_bronze_ingest.py   → creates olist_bronze database + 9 tables
02_silver_transform.py → creates olist_silver database + 3 tables
03_gold_kpis.py       → creates olist_gold database + 3 tables
```

## What each notebook does

### 01_bronze_ingest.py
- Reads all 9 CSVs from DBFS with schema inference
- Casts timestamps and numerics explicitly
- Adds `_source_file` and `_ingested_at` metadata columns
- Writes to Delta Lake at `dbfs:/delta/olist/bronze/`
- Registers tables in `olist_bronze` database

### 02_silver_transform.py
- **silver_orders**: joins orders + payments + reviews; computes `days_to_deliver`,
  `delivery_delta_days`, `delivered_on_time`, `hours_to_approve`
- **silver_customers**: aggregates to customer_unique_id grain; computes RFM
  metrics, segments (one_time/repeat/loyal, satisfied/neutral/dissatisfied)
- **silver_churn_features**: adds churn label (churned = no orders in 180 days),
  boolean feature columns for ML

### 03_gold_kpis.py
- **gold_monthly_kpis**: monthly CSAT rate, on-time rate, GMV, avg review score
- **gold_customer_segments**: customer dimension with geo coordinates
- **gold_delivery_by_state**: delivery performance aggregated by Brazilian state
- Includes 3 inline SQL analysis cells showing key business insights
