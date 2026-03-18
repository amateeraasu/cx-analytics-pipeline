# CX Analytics Pipeline — Claude Context

## Project Overview
dbt + DuckDB analytics pipeline for B2C customer experience analysis using the
[Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

## Stack
- **dbt-core** with **dbt-duckdb** adapter
- **DuckDB** as the local analytical engine (no external warehouse needed)
- Raw CSVs land in `data/raw/`, loaded via dbt sources pointing at DuckDB external files

## Repository Layout
```
models/
  staging/          # 1-to-1 with Olist source tables; rename + light casting only
  intermediate/     # Row-level business logic; joins, enrichment
  marts/
    customer_experience/   # CX-facing facts, dims, and summary aggregates
seeds/              # Small static reference data (e.g. state codes)
tests/              # Generic + singular tests
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
dbt test           # run data quality tests
dbt docs generate && dbt docs serve
```

## DuckDB Profile
Profile is in `profiles.yml` (local path). The DuckDB file lands at `data/cx_analytics.duckdb`.
Do not commit the `.duckdb` file.

## Development Notes
- Do not mock DuckDB in tests — use `dbt test` against the real DuckDB file.
- Keep staging models thin: only rename, cast, and coalesce nulls.
- Business logic belongs in intermediate or mart models.
- Add `not_null` + `unique` tests on every primary key column.
