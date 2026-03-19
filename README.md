# CX Analytics Pipeline

dbt + DuckDB analytics pipeline for B2C customer experience analysis, built on the
[Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

## What's Inside

| Layer | Models | Purpose |
|-------|--------|---------|
| **Staging** | `stg_orders`, `stg_customers`, `stg_order_items`, `stg_order_reviews`, `stg_order_payments`, `stg_products`, `stg_sellers`, `stg_geolocation` | Rename columns, cast types, light null handling |
| **Intermediate** | `int_orders_enriched`, `int_customer_orders`, `int_churn_features` | Delivery KPIs, review joins, customer-level aggregation, RFM churn features |
| **Marts** | `fct_orders`, `dim_customers`, `cx_satisfaction_summary`, `mart_churn_predictions` | Reporting-ready tables with segments, monthly KPIs, and churn scores |

## Key Metrics

- **CSAT rate** — share of delivered orders with review score ≥ 4
- **On-time delivery rate** — delivered on or before `estimated_delivery_date`
- **Days to deliver** — purchase → customer delivery
- **Average order value (BRL)**
- **Customer segments** — one-time / repeat / loyal; satisfied / neutral / dissatisfied

## Churn Prediction

`notebooks/churn_prediction.ipynb` trains three classifiers on the `int_churn_features` feature store to identify customers unlikely to reorder within 90 days.

**Dataset:** 86,924 customers (Sep 2016 – Oct 2018) · 99.7% churn rate (Olist is a structurally one-time-purchase marketplace)

**Results:**

| Model | ROC-AUC | F1 Retained | Recall Retained |
|---|---|---|---|
| Logistic Regression | 1.000 | 0.925 | 0.896 |
| Random Forest | 1.000 | 0.990 | 0.979 |
| XGBoost | 1.000 | 0.979 | 0.979 |

All models achieve perfect ROC-AUC; Random Forest has the best minority-class F1. The primary metric is **F1 on the retained class (label=0)** — correctly identifying the ~240 customers who do reorder is the actionable signal on this platform.

**Top churn drivers (by feature importance):**
1. `days_since_last_order` — recency is the dominant signal
2. `days_since_first_order` — old inactive customers are high-risk
3. `order_frequency_segment_one_time` — one-time buyers churn at ~100%
4. `order_frequency_segment_repeat` — repeat buyers churn far less
5. `satisfaction_segment_dissatisfied` — low review scores predict churn

Predictions are exported to `data/churn_predictions.csv` and materialised as `mart_churn_predictions` in DuckDB.

```python
con.execute("SELECT * FROM main_customer_experience.mart_churn_predictions WHERE predicted_label = 0").fetchdf()
```

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
