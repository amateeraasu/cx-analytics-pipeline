# Data Quality Tests

40 dbt tests across 15 models. Three test types: `not_null`, `unique`, `accepted_values`.

## Tests by Model

| Layer | Model | Column | Tests |
|---|---|---|---|
| **Staging** | `stg_orders` | `order_id` | unique, not_null |
| | | `customer_id` | not_null |
| | | `order_status` | accepted_values: approved, canceled, created, delivered, invoiced, processing, shipped, unavailable |
| | `stg_customers` | `customer_id` | unique, not_null |
| | | `customer_unique_id` | not_null |
| | `stg_order_items` | `order_item_sk` | unique, not_null |
| | `stg_order_reviews` | `review_id` | not_null |
| | | `order_id` | not_null |
| | | `review_score` | accepted_values: 1, 2, 3, 4, 5 |
| | `stg_order_payments` | `order_id` | unique, not_null |
| | `stg_products` | `product_id` | unique, not_null |
| | `stg_sellers` | `seller_id` | unique, not_null |
| **Intermediate** | `int_orders_enriched` | `order_id` | unique, not_null |
| | `int_customer_orders` | `customer_unique_id` | unique, not_null |
| | `int_churn_features` | `customer_unique_id` | unique, not_null |
| | | `is_churned` | not_null, accepted_values: 0, 1 |
| **Marts** | `dim_customers` | `customer_unique_id` | unique, not_null |
| | | `customer_sk` | unique, not_null |
| | `fct_orders` | `order_id` | unique, not_null |
| | `cx_satisfaction_summary` | `order_month` | unique, not_null |
| | `mart_churn_predictions` | `customer_unique_id` | unique, not_null |
| | | `churn_probability` | not_null |
| | | `churn_risk_tier` | not_null, accepted_values: critical, high, medium, low |

## Test Types Explained

| Test | What it catches |
|---|---|
| `not_null` | Missing values — NULL in a primary key breaks every downstream join |
| `unique` | Duplicate rows — silently inflates revenue, CSAT, and row counts |
| `accepted_values` | Unexpected categories from source — catches schema drift before it corrupts reports |

## How Tests Run

```bash
dbt test --profiles-dir .
```

If any test fails — dbt stops the run and reports exactly which model, column, and rows failed.
Staging tests run first — bad data is caught before it reaches any business logic.
