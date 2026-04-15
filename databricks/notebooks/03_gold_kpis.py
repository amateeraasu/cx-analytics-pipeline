# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Business-Ready KPI Tables
# MAGIC
# MAGIC **Medallion layer:** Gold (serving)
# MAGIC **Source:** `olist_silver` Delta tables
# MAGIC **Output:** 3 Delta tables in `olist_gold` database
# MAGIC
# MAGIC This notebook mirrors the dbt **mart** layer:
# MAGIC
# MAGIC | dbt mart                  | Gold table                 | What it does |
# MAGIC |---------------------------|----------------------------|--------------|
# MAGIC | `cx_satisfaction_summary` | `gold_monthly_kpis`        | Monthly CSAT, on-time rate, GMV |
# MAGIC | `dim_customers`           | `gold_customer_segments`   | Customer dimension with segments |
# MAGIC | `fct_orders` (delivery)   | `gold_delivery_by_state`   | Delivery performance by state |
# MAGIC
# MAGIC Gold tables are the final serving layer — what dashboards, reports, and
# MAGIC stakeholders consume directly.
# MAGIC
# MAGIC **Prerequisite:** Run `01_bronze_ingest` and `02_silver_transform` first.

# COMMAND ----------

# MAGIC %md ## 0. Configuration

# COMMAND ----------

SILVER_DB  = "olist_silver"
GOLD_DB    = "olist_gold"
DELTA_ROOT = "dbfs:/delta/olist/gold"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DB}")
print(f"Database '{GOLD_DB}' ready.")

# COMMAND ----------

from pyspark.sql import functions as F


def write_gold(df, table_name: str) -> int:
    delta_path = f"{DELTA_ROOT}/{table_name}"
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(delta_path)
    )
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_DB}.{table_name}
        USING DELTA LOCATION '{delta_path}'
    """)
    n = spark.read.format("delta").load(delta_path).count()
    print(f"  ✓  {GOLD_DB}.{table_name:35s} {n:>6,} rows")
    return n

# COMMAND ----------

# MAGIC %md ## 1. gold_monthly_kpis — CSAT, on-time rate, GMV by month
# MAGIC
# MAGIC Mirrors `cx_satisfaction_summary` in dbt.
# MAGIC One row per calendar month across the full dataset (2016–2018).

# COMMAND ----------

silver_orders = spark.table(f"{SILVER_DB}.silver_orders")

gold_monthly = (
    silver_orders
    .filter(F.col("order_status") == "delivered")
    .groupBy("order_month")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.round(F.avg("review_score"), 3).alias("avg_review_score"),
        F.round(F.avg("days_to_deliver"), 2).alias("avg_days_to_deliver"),

        F.round(
            F.sum(F.when(F.col("review_score") >= 4, 1).otherwise(0)).cast("double")
            / F.count(F.col("review_score")),
            4
        ).alias("csat_rate"),

        F.round(
            F.sum(F.col("delivered_on_time").cast("integer")).cast("double")
            / F.count("order_id"),
            4
        ).alias("on_time_rate"),

        F.round(F.avg("total_payment_value"), 2).alias("avg_order_value_brl"),
        F.round(F.sum("total_payment_value"), 2).alias("total_gmv_brl"),

        F.count(F.when(F.col("has_review_comment"), 1)).alias("orders_with_comment"),
        F.count(F.when(F.col("review_score") == 1,  1)).alias("low_score_orders"),
        F.count(F.when(F.col("used_voucher"),        1)).alias("voucher_orders"),
    )
    .orderBy("order_month")
    .withColumn("_updated_at", F.current_timestamp())
)

print("Building gold_monthly_kpis...")
write_gold(gold_monthly, "gold_monthly_kpis")

# COMMAND ----------

# MAGIC %md ## 2. gold_customer_segments — customer dimension
# MAGIC
# MAGIC Mirrors `dim_customers` in dbt. One row per unique customer.
# MAGIC Enriched with geo coordinates from the geolocation table.

# COMMAND ----------

from pyspark.sql import functions as F

silver_customers = spark.table(f"{SILVER_DB}.silver_customers")

# Aggregate geolocation to city/state grain (1M+ rows → deduplicated)
geo = (
    spark.table("olist_bronze.raw_geolocation")
    .groupBy("city", "state")
    .agg(
        F.round(F.avg("lat"), 4).alias("lat"),
        F.round(F.avg("lng"), 4).alias("lng"),
    )
)

gold_customers = (
    silver_customers
    .join(geo, on=["city", "state"], how="left")
    .select(
        "customer_unique_id",
        "state",
        "city",
        F.col("lat"),
        F.col("lng"),
        "total_orders",
        "delivered_orders",
        "canceled_orders",
        "first_order_at",
        "last_order_at",
        "customer_lifespan_days",
        "total_spend_brl",
        "avg_order_value_brl",
        "avg_review_score",
        "review_count",
        "avg_days_to_deliver",
        "on_time_deliveries",
        "order_frequency_segment",
        "satisfaction_segment",
    )
    .withColumn("_updated_at", F.current_timestamp())
)

print("Building gold_customer_segments...")
write_gold(gold_customers, "gold_customer_segments")

# COMMAND ----------

# MAGIC %md ## 3. gold_delivery_by_state — delivery performance by state
# MAGIC
# MAGIC Mirrors the `get_delivery_performance(group_by="state")` MCP query.
# MAGIC Useful for executive dashboards showing geographic delivery quality.

# COMMAND ----------

silver_customers_lite = spark.table(f"{SILVER_DB}.silver_customers").select(
    "customer_unique_id", "state"
)

orders_with_state = (
    silver_orders
    .filter(F.col("order_status") == "delivered")
    .join(
        spark.table("olist_bronze.raw_customers").select("customer_id", "customer_unique_id"),
        on="customer_id",
        how="left",
    )
    .join(silver_customers_lite, on="customer_unique_id", how="left")
)

gold_delivery = (
    orders_with_state
    .filter(F.col("state").isNotNull())
    .groupBy("state")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.round(F.avg("days_to_deliver"), 1).alias("avg_days_to_deliver"),
        F.round(F.avg("delivery_delta_days"), 1).alias("avg_delta_vs_estimate"),
        F.round(
            F.sum(F.col("delivered_on_time").cast("integer")).cast("double")
            / F.count("order_id") * 100, 1
        ).alias("on_time_pct"),
        F.round(F.avg("review_score"), 2).alias("avg_review_score"),
        F.round(
            F.sum(F.when(F.col("review_score") >= 4, 1).otherwise(0)).cast("double")
            / F.count(F.col("review_score")) * 100, 1
        ).alias("csat_pct"),
    )
    .filter(F.col("total_orders") >= 100)
    .orderBy(F.col("avg_days_to_deliver").desc())
    .withColumn("_updated_at", F.current_timestamp())
)

print("Building gold_delivery_by_state...")
write_gold(gold_delivery, "gold_delivery_by_state")

# COMMAND ----------

# MAGIC %md ## 4. Business insights — the story in numbers

# COMMAND ----------

# MAGIC %md ### 4a. Monthly KPI trend

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     date_format(order_month, 'yyyy-MM') AS month,
# MAGIC     total_orders,
# MAGIC     round(csat_rate * 100, 1)   AS csat_pct,
# MAGIC     round(on_time_rate * 100, 1) AS on_time_pct,
# MAGIC     round(total_gmv_brl / 1000, 1) AS gmv_k_brl
# MAGIC FROM olist_gold.gold_monthly_kpis
# MAGIC ORDER BY order_month

# COMMAND ----------

# MAGIC %md ### 4b. Worst states for delivery (bottom 5)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT state, total_orders, avg_days_to_deliver, on_time_pct, avg_review_score
# MAGIC FROM olist_gold.gold_delivery_by_state
# MAGIC ORDER BY avg_days_to_deliver DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md ### 4c. Customer segment breakdown

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     order_frequency_segment,
# MAGIC     satisfaction_segment,
# MAGIC     count(*)                            AS customers,
# MAGIC     round(avg(total_spend_brl), 2)      AS avg_spend_brl,
# MAGIC     round(avg(avg_review_score), 2)     AS avg_review
# MAGIC FROM olist_gold.gold_customer_segments
# MAGIC GROUP BY order_frequency_segment, satisfaction_segment
# MAGIC ORDER BY customers DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline complete
# MAGIC
# MAGIC ```
# MAGIC CSV files (DBFS)
# MAGIC     │
# MAGIC     ▼
# MAGIC olist_bronze  ← 01_bronze_ingest.py
# MAGIC     │  9 raw Delta tables, metadata columns added
# MAGIC     ▼
# MAGIC olist_silver  ← 02_silver_transform.py
# MAGIC     │  silver_orders / silver_customers / silver_churn_features
# MAGIC     │  Delivery KPIs, customer aggregations, ML features
# MAGIC     ▼
# MAGIC olist_gold    ← 03_gold_kpis.py  (this notebook)
# MAGIC     │  gold_monthly_kpis / gold_customer_segments / gold_delivery_by_state
# MAGIC     │  Business-ready, stakeholder-consumable
# MAGIC     ▼
# MAGIC Dashboards / MCP / Streamlit
# MAGIC ```
# MAGIC
# MAGIC **Total tables:** 9 Bronze + 3 Silver + 3 Gold = 15 Delta tables
# MAGIC (matches the 15 dbt models in the local DuckDB pipeline)
