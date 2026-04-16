# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Cleaned & Enriched Data
# MAGIC
# MAGIC **Medallion layer:** Silver (cleansed)
# MAGIC **Source:** `olist_bronze` Delta tables
# MAGIC **Output:** 3 Delta tables in `olist_silver` database
# MAGIC
# MAGIC This notebook mirrors the dbt **staging → intermediate** layers:
# MAGIC
# MAGIC | dbt model              | Silver table            | What it does |
# MAGIC |------------------------|-------------------------|--------------|
# MAGIC | stg_* + int_orders_enriched | `silver_orders`    | Joins orders + payments + reviews; delivery KPIs |
# MAGIC | int_customer_orders    | `silver_customers`      | Customer-grain aggregation with RFM features |
# MAGIC | int_churn_features     | `silver_churn_features` | ML-ready feature set for churn prediction |
# MAGIC
# MAGIC **Prerequisite:** Run `01_bronze_ingest` first.

# COMMAND ----------

# MAGIC %md ## 0. Configuration

# COMMAND ----------

CATALOG    = "main"
BRONZE_DB  = f"{CATALOG}.olist_bronze"
SILVER_DB  = f"{CATALOG}.olist_silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_DB}")
print(f"Schema '{SILVER_DB}' ready.")

# COMMAND ----------

from pyspark.sql import functions as F, Window
from pyspark.sql.types import *


def write_silver(df, table_name: str) -> int:
    """Write a DataFrame to a UC-managed Silver Delta table."""
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{SILVER_DB}.{table_name}")
    )
    n = spark.table(f"{SILVER_DB}.{table_name}").count()
    print(f"  ✓  {SILVER_DB}.{table_name:35s} {n:>9,} rows")
    return n

# COMMAND ----------

# MAGIC %md ## 1. silver_orders — orders + payments + reviews + delivery KPIs
# MAGIC
# MAGIC Mirrors `stg_orders` + `stg_order_payments` + `stg_order_reviews`
# MAGIC + `int_orders_enriched` in dbt.

# COMMAND ----------

# ── Payments: aggregate to order grain ───────────────────────────────────────
payments = spark.table(f"{BRONZE_DB}.raw_order_payments")

payments_agg = payments.groupBy("order_id").agg(
    F.round(F.sum("payment_value"), 2).alias("total_payment_value"),
    F.first(
        F.when(F.col("payment_type") != "not_defined", F.col("payment_type"))
    ).alias("primary_payment_type"),
    F.max(F.when(F.col("payment_type") == "voucher", F.lit(True)).otherwise(F.lit(False))).alias("used_voucher"),
    F.max(F.when(F.col("payment_type") == "credit_card", F.lit(True)).otherwise(F.lit(False))).alias("used_credit_card"),
)

# ── Reviews: keep latest review per order (deduplicate) ──────────────────────
reviews_raw = spark.table(f"{BRONZE_DB}.raw_order_reviews")

review_window = Window.partitionBy("order_id").orderBy(F.col("review_created_at").desc())

reviews = (
    reviews_raw
    .withColumn("rn", F.row_number().over(review_window))
    .filter(F.col("rn") == 1)
    .select(
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_created_at",
        "review_answered_at",
    )
)

# ── Orders: join everything + compute delivery KPIs ──────────────────────────
orders = spark.table(f"{BRONZE_DB}.raw_orders")

silver_orders = (
    orders
    .join(payments_agg, on="order_id", how="left")
    .join(reviews,      on="order_id", how="left")
    .select(
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",

        # Delivery KPIs (mirrors dbt int_orders_enriched)
        F.datediff(
            F.col("order_delivered_customer_date"),
            F.col("order_purchase_timestamp")
        ).alias("days_to_deliver"),

        F.datediff(
            F.col("order_delivered_customer_date"),
            F.col("order_estimated_delivery_date")
        ).alias("delivery_delta_days"),

        (F.col("order_delivered_customer_date") <= F.col("order_estimated_delivery_date"))
            .alias("delivered_on_time"),

        (
            (F.unix_timestamp("order_approved_at") - F.unix_timestamp("order_purchase_timestamp"))
            / 3600
        ).cast("double").alias("hours_to_approve"),

        # Payment
        "total_payment_value",
        "primary_payment_type",
        "used_voucher",
        "used_credit_card",

        # Review
        "review_score",
        (
            F.col("review_comment_message").isNotNull() &
            (F.col("review_comment_message") != "")
        ).alias("has_review_comment"),
        "review_created_at",

        # Date parts for slicing
        F.date_trunc("month", F.col("order_purchase_timestamp")).alias("order_month"),
        F.date_trunc("week",  F.col("order_purchase_timestamp")).alias("order_week"),
        F.dayofweek(F.col("order_purchase_timestamp")).alias("order_day_of_week"),
    )
    .withColumn("_transformed_at", F.current_timestamp())
)

print("Building silver_orders...")
write_silver(silver_orders, "silver_orders")

# COMMAND ----------

# MAGIC %md ## 2. silver_customers — customer-grain aggregation
# MAGIC
# MAGIC Mirrors `int_customer_orders` + `dim_customers` in dbt.
# MAGIC
# MAGIC **Olist quirk:** one customer_unique_id can appear with multiple customer_ids
# MAGIC (new ID issued per order). We resolve to the most recent state/city using
# MAGIC `first()` after sorting by purchase date.

# COMMAND ----------

customers = spark.table(f"{BRONZE_DB}.raw_customers")
orders_c  = spark.table(f"{SILVER_DB}.silver_orders")

# Join to get customer_unique_id on each order row
orders_with_uid = orders_c.join(
    customers.select("customer_id", "customer_unique_id", "state", "city"),
    on="customer_id",
    how="left",
)

# Window for "most recent" state/city per customer
latest_window = Window.partitionBy("customer_unique_id").orderBy(
    F.col("order_purchase_timestamp").desc()
)

silver_customers = (
    orders_with_uid
    .withColumn("rn", F.row_number().over(latest_window))
    .groupBy("customer_unique_id")
    .agg(
        # Most recent canonical address (rn=1 trick: take max where rn=1)
        F.first(
            F.when(F.row_number().over(latest_window) == 1, F.col("state"))
        ).alias("state"),
        F.first(
            F.when(F.row_number().over(latest_window) == 1, F.col("city"))
        ).alias("city"),

        F.count("order_id").alias("total_orders"),
        F.count(F.when(F.col("order_status") == "delivered",   F.lit(1))).alias("delivered_orders"),
        F.count(F.when(F.col("order_status") == "canceled",    F.lit(1))).alias("canceled_orders"),
        F.min("order_purchase_timestamp").alias("first_order_at"),
        F.max("order_purchase_timestamp").alias("last_order_at"),
        F.datediff(
            F.max("order_purchase_timestamp"),
            F.min("order_purchase_timestamp")
        ).alias("customer_lifespan_days"),
        F.round(F.sum("total_payment_value"),  2).alias("total_spend_brl"),
        F.round(F.avg("total_payment_value"),  2).alias("avg_order_value_brl"),
        F.round(F.avg("review_score"),         2).alias("avg_review_score"),
        F.count(F.col("review_score")).alias("review_count"),
        F.round(F.avg("days_to_deliver"),      1).alias("avg_days_to_deliver"),
        F.count(F.when(F.col("delivered_on_time"), F.lit(1))).alias("on_time_deliveries"),
    )
    # Customer segments (mirrors dim_customers logic in dbt)
    .withColumn(
        "order_frequency_segment",
        F.when(F.col("total_orders") == 1,              "one_time")
         .when(F.col("total_orders").between(2, 4),     "repeat")
         .otherwise("loyal")
    )
    .withColumn(
        "satisfaction_segment",
        F.when(F.col("avg_review_score") >= 4,   "satisfied")
         .when(F.col("avg_review_score") >= 3,   "neutral")
         .otherwise("dissatisfied")
    )
    .withColumn("_transformed_at", F.current_timestamp())
)

print("Building silver_customers...")
write_silver(silver_customers, "silver_customers")

# COMMAND ----------

# MAGIC %md ## 3. silver_churn_features — ML feature table
# MAGIC
# MAGIC Mirrors `int_churn_features` in dbt.
# MAGIC Label: `churned = 1` if no orders in the last 180 days; `retained = 0` otherwise.

# COMMAND ----------

from pyspark.sql.functions import lit, datediff, to_date, max as spark_max

# Use the latest order date in the dataset as "reference date"
ref_date = orders_c.agg(spark_max("order_purchase_timestamp")).collect()[0][0]
print(f"Reference date (latest order): {ref_date}")

silver_churn = (
    silver_customers
    .withColumn(
        "days_since_last_order",
        F.datediff(F.lit(ref_date), F.col("last_order_at"))
    )
    .withColumn(
        "days_since_first_order",
        F.datediff(F.lit(ref_date), F.col("first_order_at"))
    )
    .withColumn(
        "churned",
        (F.col("days_since_last_order") > 180).cast("integer")
    )
    # Boolean feature columns for ML
    .withColumn("is_one_time",     (F.col("order_frequency_segment") == "one_time").cast("integer"))
    .withColumn("is_repeat",       (F.col("order_frequency_segment") == "repeat").cast("integer"))
    .withColumn("is_loyal",        (F.col("order_frequency_segment") == "loyal").cast("integer"))
    .withColumn("is_dissatisfied", (F.col("satisfaction_segment") == "dissatisfied").cast("integer"))
    .withColumn("is_satisfied",    (F.col("satisfaction_segment") == "satisfied").cast("integer"))
    .withColumn(
        "freight_ratio",
        F.when(
            F.col("total_spend_brl") > 0,
            F.round(F.col("total_spend_brl") / F.col("total_spend_brl"), 4)
        ).otherwise(0)
    )
    .select(
        "customer_unique_id",
        "days_since_last_order",
        "days_since_first_order",
        "customer_lifespan_days",
        "total_orders",
        "total_spend_brl",
        "avg_order_value_brl",
        "avg_review_score",
        "avg_days_to_deliver",
        "on_time_deliveries",
        "review_count",
        "is_one_time",
        "is_repeat",
        "is_loyal",
        "is_dissatisfied",
        "is_satisfied",
        "churned",
    )
    .withColumn("_transformed_at", F.current_timestamp())
)

print("Building silver_churn_features...")
write_silver(silver_churn, "silver_churn_features")

# COMMAND ----------

# MAGIC %md ## 4. Validate Silver tables

# COMMAND ----------

# Class balance check
print("Churn label distribution:")
spark.sql(f"""
    SELECT churned, count(*) AS n,
           round(count(*) * 100.0 / sum(count(*)) over(), 2) AS pct
    FROM {SILVER_DB}.silver_churn_features
    GROUP BY churned
    ORDER BY churned
""").show()

# COMMAND ----------

# Delivery KPI sanity check
print("Delivery performance summary:")
spark.sql(f"""
    SELECT
        round(avg(days_to_deliver), 1)                                      AS avg_days,
        round(sum(cast(delivered_on_time as int)) * 100.0 / count(*), 1)   AS on_time_pct,
        round(avg(review_score), 2)                                         AS avg_review
    FROM {SILVER_DB}.silver_orders
    WHERE order_status = 'delivered'
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next step
# MAGIC Run **`03_gold_kpis`** to build business-ready aggregation tables.
