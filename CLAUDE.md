# CX Analytics Pipeline — Claude Context

## Project Overview
End-to-end customer experience analytics on the Olist Brazilian E-Commerce dataset (100K+ orders).
The pipeline spans dbt + DuckDB locally, Databricks + Delta Lake for the production path, an MCP
server for natural language querying, and a Streamlit dashboard for interactive exploration.

[Kaggle dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

## Stack
- **dbt-core** with **dbt-duckdb** adapter — local transformation pipeline
- **DuckDB** as the local analytical engine (`data/cx_analytics.duckdb`)
- **Databricks / PySpark / Delta Lake** — production-equivalent notebooks in `databricks/`
- **MCP server** (`mcp/server.py`) — exposes DuckDB marts as Claude-queryable tools
- **Streamlit** (`streamlit/app.py`) — interactive KPI dashboard over DuckDB
- **XGBoost / Random Forest** — churn prediction in `notebooks/churn_prediction.ipynb`

## Repository Layout
```
models/
  staging/          # 1-to-1 with Olist source tables; rename + light casting only
  intermediate/     # Row-level business logic; joins, enrichment, ML features
  marts/
    customer_experience/   # fct_orders, dim_customers, cx_satisfaction_summary, mart_churn_predictions
databricks/
  notebooks/
    01_bronze_ingest.py    # CSV → Delta Lake (Bronze layer, 9 tables)
    02_silver_transform.py # Bronze → Silver (cleaned, joined, delivery KPIs)
    03_gold_kpis.py        # Silver → Gold (monthly KPIs, customer segments, delivery by state)
  README.md                # Databricks Community Edition setup guide
mcp/
  server.py          # FastMCP server: 6 tools over DuckDB (read-only)
  audit_logger.py    # Per-query audit trail (caller, params, row count, duration)
  data_masker.py     # @mask_pii() decorator — partial masks customer_unique_id
  config.json        # Claude Desktop config template
  requirements.txt   # mcp>=1.0.0, duckdb>=1.1
  README.md          # Setup guide + example prompts
  demo.md            # Extended demo prompts and tool reference
  tests/             # Unit tests for date_validator and data_masker
streamlit/
  app.py             # Multi-tab dashboard: Monthly KPIs · Delivery by State · Churn Risk
  requirements.txt   # streamlit, plotly, duckdb, pandas
sql/
  advanced_queries.sql  # 7 standalone SQL queries showcasing window functions + CTEs
docs/
  architecture.md    # Mermaid diagram + schema reference + design decisions
notebooks/
  churn_prediction.ipynb          # RF + XGBoost + LR churn model comparison
  ab_test_delivery_vs_satisfaction.ipynb
seeds/              # Small static reference data
tests/              # Generic + singular dbt tests
macros/             # Reusable Jinja helpers
analyses/           # Ad-hoc queries (not materialized)
data/raw/           # Olist CSV files (gitignored)
```

## Source Tables (Olist)
| File | dbt source alias |
|------|-----------------|
| olist_orders_dataset.csv | orders |
| olist_customers_dataset.csv | customers |
| olist_order_items_dataset.csv | order_items |
| olist_order_reviews_dataset.csv | order_reviews |
| olist_order_payments_dataset.csv | order_payments |
| olist_products_dataset.csv | products |
| olist_sellers_dataset.csv | sellers |
| product_category_name_translation.csv | category_translation |

## DuckDB Schema
All mart models land in schema `main_customer_experience`.
Tables: `fct_orders`, `dim_customers`, `cx_satisfaction_summary`, `mart_churn_predictions`

## Key Conventions
- **Staging models** are prefixed `stg_`, materialized as `view`.
- **Intermediate models** are prefixed `int_`, materialized as `view`.
- **Mart models** use plain names, materialized as `table`.
- All timestamps are cast to `TIMESTAMPTZ` in staging.
- Monetary amounts are in BRL.
- Use `{{ dbt_utils.generate_surrogate_key([...]) }}` for surrogate keys.

## Running the Project
```bash
# Install dependencies
pip install -r requirements.txt

# Place Olist CSVs in data/raw/ then:
dbt debug          # verify connection
dbt seed           # load seed files
dbt run            # build all models
dbt test           # run 40 data quality tests
dbt docs generate && dbt docs serve

# Optional: generate churn predictions (needed for mart_churn_predictions)
jupyter notebook notebooks/churn_prediction.ipynb

# Launch dashboard
pip install -r streamlit/requirements.txt
streamlit run streamlit/app.py

# Launch MCP server
pip install -r mcp/requirements.txt
python mcp/server.py
```

## MCP Server — Tools
| Tool | Description |
|---|---|
| `run_sql` | Execute any read-only SELECT/WITH query |
| `get_monthly_kpis` | CSAT, on-time rate, GMV for a date range |
| `get_customer_segments` | Filter dim_customers by state/segment/spend |
| `get_churn_risk` | Churn predictions filtered by tier/state/spend |
| `get_delivery_performance` | Delivery metrics grouped by state/month/dow/payment type |
| `list_tables` | Schema explorer — tables and column names |

Security: all user values are parameterised `?` bindings. `group_by` is validated against an allowlist.
Date inputs use `datetime.date.fromisoformat()` (rejects injection). Connection is `read_only=True`.

## DuckDB Profile
Profile is in `profiles.yml` (local path). The DuckDB file lands at `data/cx_analytics.duckdb`.
Do not commit the `.duckdb` file or files in `data/raw/`.

## Development Notes
- Do not mock DuckDB in tests — use `dbt test` against the real DuckDB file.
- Keep staging models thin: only rename, cast, and coalesce nulls.
- Business logic belongs in intermediate or mart models.
- Add `not_null` + `unique` tests on every primary key column.
- The `kaggle_repo/` directory is a copy of the project for Kaggle — do not edit it directly.
- `archive/` and `archive.zip` contain the original raw CSV files — these are gitignored in normal flow.
