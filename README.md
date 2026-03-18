# CX Analytics Pipeline

dbt + DuckDB analytics pipeline for B2C customer experience analysis, built on the
[Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

## What's Inside

| Layer | Models | Purpose |
|-------|--------|---------|
| **Staging** | `stg_orders`, `stg_customers`, `stg_order_items`, `stg_order_reviews`, `stg_order_payments`, `stg_products`, `stg_sellers`, `stg_geolocation` | Rename columns, cast types, light null handling |
| **Intermediate** | `int_orders_enriched`, `int_customer_orders` | Delivery KPIs, review joins, customer-level aggregation |
| **Marts** | `fct_orders`, `dim_customers`, `cx_satisfaction_summary` | Reporting-ready tables with segments and monthly KPIs |

## Key Metrics

- **CSAT rate** — share of delivered orders with review score ≥ 4
- **On-time delivery rate** — delivered on or before `estimated_delivery_date`
- **Days to deliver** — purchase → customer delivery
- **Average order value (BRL)**
- **Customer segments** — one-time / repeat / loyal; satisfied / neutral / dissatisfied

## Quick Start

```bash
# 1. Create virtual environment (requires Python 3.10–3.12)
python3.10 -m venv .venv && source .venv/bin/activate

# 2. Install dependencies
pip install dbt-core dbt-duckdb duckdb

# 3. Download Olist CSVs and place in data/raw/
#    kaggle datasets download -d olistbr/brazilian-ecommerce -p data/raw/ --unzip

# 4. Install dbt packages
dbt deps --profiles-dir .

# 5. Build all models
dbt run --profiles-dir .

# 6. Run data quality tests
dbt test --profiles-dir .

# 7. Explore docs
dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .
```

## Querying Results

```python
import duckdb
con = duckdb.connect("data/cx_analytics.duckdb")
con.execute("SELECT * FROM main_customer_experience.cx_satisfaction_summary").fetchdf()
```

## Dataset Files (`data/raw/`)

| File | Rows (approx) |
|------|--------------|
| `olist_orders_dataset.csv` | 99,441 |
| `olist_customers_dataset.csv` | 99,441 |
| `olist_order_items_dataset.csv` | 112,650 |
| `olist_order_reviews_dataset.csv` | 100,000 |
| `olist_order_payments_dataset.csv` | 103,886 |
| `olist_products_dataset.csv` | 32,951 |
| `olist_sellers_dataset.csv` | 3,095 |
| `olist_geolocation_dataset.csv` | 1,000,163 |
| `product_category_name_translation.csv` | 71 |

Raw CSVs are gitignored — download separately from Kaggle.
