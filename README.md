# CX Analytics Pipeline

dbt + DuckDB analytics pipeline for B2C customer experience analysis, extended with a churn
prediction model — built on the
[Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

## Architecture overview

```
Raw CSVs (Olist)
      │
      ▼
 dbt pipeline (DuckDB)
  ├── Staging      → clean, typed source tables
  ├── Intermediate → delivery KPIs, customer aggregations
  └── Marts        → fct_orders, dim_customers, cx_satisfaction_summary
                             │
                             ▼
               int_churn_features  (feature mart)
                             │
                             ▼
          churn_prediction.ipynb  (ML layer)
          ├── Logistic Regression
          ├── Random Forest  ← best model
          └── XGBoost
                             │
                             ▼
              data/churn_predictions.csv
```

## dbt model layers

| Layer | Models | Purpose |
|-------|--------|---------|
| **Staging** | `stg_orders`, `stg_customers`, `stg_order_items`, `stg_order_reviews`, `stg_order_payments`, `stg_products`, `stg_sellers`, `stg_geolocation` | Rename columns, cast types, light null handling |
| **Intermediate** | `int_orders_enriched`, `int_customer_orders`, `int_churn_features` | Delivery KPIs, review joins, customer-level aggregation, churn feature engineering |
| **Marts** | `fct_orders`, `dim_customers`, `cx_satisfaction_summary` | Reporting-ready tables with segments and monthly KPIs |

## Key metrics (dbt layer)

- **CSAT rate** — share of delivered orders with review score ≥ 4
- **On-time delivery rate** — delivered on or before `estimated_delivery_date`
- **Days to deliver** — purchase → customer delivery
- **Average order value (BRL)**
- **Customer segments** — one-time / repeat / loyal; satisfied / neutral / dissatisfied

## Churn prediction

### Problem framing

Olist is structurally a one-time-purchase marketplace — 99.7% of customers never return.
The goal is not to predict who will churn (almost everyone does), but to identify the rare
~240 customers who exhibit repeat purchase behaviour. These are the customers worth targeting
for retention campaigns.

Label definition: `churned = 1` if no orders in the past 180 days; `retained = 0` otherwise.

### Dataset

| Class | Count | Share |
|---|---|---|
| Churned | 86,684 | 99.7% |
| Retained | 240 | 0.3% |

### Feature engineering (`int_churn_features`)

Features are computed entirely within dbt before any ML code runs:

| Feature | Description |
|---|---|
| `days_since_last_order` | Recency signal — primary churn driver |
| `days_since_first_order` | Customer tenure |
| `order_frequency_segment_one_time` | Boolean: single-purchase customer |
| `order_frequency_segment_repeat` | Boolean: multi-purchase customer |
| `satisfaction_segment_dissatisfied` | Boolean: review score ≤ 2 |

### Model results

| Model | ROC-AUC | F1 Retained | Recall Retained | Threshold |
|---|---|---|---|---|
| Logistic Regression | 1.000 | 0.9247 | 0.8958 | 0.006 |
| **Random Forest** | **1.000** | **0.9895** | **0.9792** | **0.303** |
| XGBoost | 1.000 | 0.9792 | 0.9792 | 0.999 |

**Selected model: Random Forest.** All three models achieve perfect ROC-AUC, which is expected
given the extreme class imbalance — a near-trivial separator exists between retained and churned
customers. The more meaningful comparison is on the retained class metrics. Random Forest leads
on F1 Retained and produces the most calibrated decision threshold (0.303 vs 0.006 for LR and
0.999 for XGBoost), indicating it is generating meaningful probabilities rather than extreme
edge values.

### Key findings

- **Recency dominates.** `days_since_last_order` is the strongest signal by a wide margin — customers who have not ordered recently almost never return, which is consistent with the platform's marketplace model.
- **Purchase behaviour is near-deterministic.** `order_frequency_segment_one_time` alone accounts for nearly all churned customers. This is a structural feature of the platform, not a failure of the model.
- **Satisfaction matters at the margin.** Dissatisfied customers (review ≤ 2) show elevated churn even within the already-high-churn population, suggesting review score is a retention lever worth monitoring.
- **226 customers predicted retained** out of 86,924 total (vs 240 actual retained in the labelled set). The model misses approximately 14 retained customers — an acceptable false negative rate given the domain.

### Output

Predictions written to `data/churn_predictions.csv` with columns: `customer_id`, `churn_probability`, `predicted_label`.

## Quick start

```bash
# 1. Create virtual environment (requires Python 3.10–3.12)
python3.10 -m venv .venv && source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Download Olist CSVs and place in data/raw/
#    kaggle datasets download -d olistbr/brazilian-ecommerce -p data/raw/ --unzip

# 4. Install dbt packages
dbt deps --profiles-dir .

# 5. Build all models (includes int_churn_features)
dbt run --profiles-dir .

# 6. Run data quality tests
dbt test --profiles-dir .

# 7. Run churn prediction notebook
jupyter notebook notebooks/churn_prediction.ipynb

# 8. Explore dbt docs
dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .
```

## Querying results

```python
import duckdb

con = duckdb.connect("data/cx_analytics.duckdb")

# CX summary
con.execute("SELECT * FROM main_customer_experience.cx_satisfaction_summary").fetchdf()

# Churn features
con.execute("SELECT * FROM main_customer_experience.int_churn_features LIMIT 100").fetchdf()
```

## Dataset files (`data/raw/`)

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

## Tech stack

- **dbt-core + dbt-duckdb** — transformation layer
- **DuckDB** — embedded analytical database
- **scikit-learn** — Logistic Regression, Random Forest
- **XGBoost** — gradient-boosted trees
- **pandas / numpy** — data manipulation
- **Jupyter** — exploratory analysis and model evaluation
