# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Ingestion to Delta Lake
# MAGIC
# MAGIC **Medallion layer:** Bronze (raw)
# MAGIC **Source:** Olist Brazilian E-Commerce CSVs (9 files, ~100K orders)
# MAGIC **Output:** 9 Delta Lake tables in `olist_bronze` database
# MAGIC
# MAGIC This notebook ingests the raw Olist CSV files into Delta Lake with minimal
# MAGIC transformation — column renaming only, no business logic. All source data is
# MAGIC preserved exactly as delivered.
# MAGIC
# MAGIC **Run this notebook first.** Silver and Gold depend on it.
# MAGIC
# MAGIC ## Setup
# MAGIC 1. Download the dataset: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
# MAGIC 2. Upload all 9 CSV files to DBFS: `dbfs:/FileStore/olist/raw/`
# MAGIC    - In Databricks UI: Data → Add Data → Upload Files
# MAGIC 3. Attach this notebook to a cluster (DBR 13.0+ recommended)
# MAGIC 4. Run All

# COMMAND ----------

# MAGIC %md ## 0. Configuration

# COMMAND ----------

# Paths — adjust if you uploaded CSVs to a different DBFS location
RAW_PATH   = "dbfs:/FileStore/olist/raw"
BRONZE_DB  = "olist_bronze"
DELTA_ROOT = "dbfs:/delta/olist/bronze"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")
print(f"Database '{BRONZE_DB}' ready.")

# COMMAND ----------

# MAGIC %md ## 1. Helper — ingest one CSV to Delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *


def ingest_csv(filename: str, table_name: str, transformations=None) -> int:
    """
    Read a CSV from DBFS, optionally apply column renames/casts,
    write to Delta Lake, and register in the metastore.

    Args:
        filename:        CSV filename under RAW_PATH
        table_name:      Target Delta table (written to BRONZE_DB)
        transformations: Optional callable(df) → df for renames/casts

    Returns:
        Row count of the written table
    """
    path = f"{RAW_PATH}/{filename}"
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("quote", '"')
        .option("escape", '"')
        .csv(path)
    )

    if transformations:
        df = transformations(df)

    # Add Bronze metadata columns
    df = (
        df
        .withColumn("_source_file",    F.lit(filename))
        .withColumn("_ingested_at",     F.current_timestamp())
    )

    delta_path = f"{DELTA_ROOT}/{table_name}"
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(delta_path)
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_DB}.{table_name}
        USING DELTA LOCATION '{delta_path}'
    """)

    n = spark.read.format("delta").load(delta_path).count()
    print(f"  ✓  {BRONZE_DB}.{table_name:35s} {n:>9,} rows")
    return n

# COMMAND ----------

# MAGIC %md ## 2. Ingest all 9 source tables

# COMMAND ----------

print("Ingesting Olist source tables → Bronze Delta tables\n")

# ── Orders ────────────────────────────────────────────────────────────────────
ingest_csv(
    "olist_orders_dataset.csv",
    "raw_orders",
    lambda df: df.select(
        "order_id",
        "customer_id",
        "order_status",
        F.col("order_purchase_timestamp").cast("timestamp").alias("order_purchase_timestamp"),
        F.col("order_approved_at").cast("timestamp").alias("order_approved_at"),
        F.col("order_delivered_carrier_date").cast("timestamp").alias("order_delivered_carrier_date"),
        F.col("order_delivered_customer_date").cast("timestamp").alias("order_delivered_customer_date"),
        F.col("order_estimated_delivery_date").cast("timestamp").alias("order_estimated_delivery_date"),
    )
)

# ── Customers ─────────────────────────────────────────────────────────────────
ingest_csv(
    "olist_customers_dataset.csv",
    "raw_customers",
    lambda df: df.select(
        "customer_id",
        "customer_unique_id",
        F.col("customer_zip_code_prefix").cast("integer").alias("zip_code_prefix"),
        F.col("customer_city").alias("city"),
        F.col("customer_state").alias("state"),
    )
)

# ── Order Items ───────────────────────────────────────────────────────────────
ingest_csv(
    "olist_order_items_dataset.csv",
    "raw_order_items",
    lambda df: df.select(
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        F.col("shipping_limit_date").cast("timestamp").alias("shipping_limit_date"),
        F.col("price").cast("double").alias("price"),
        F.col("freight_value").cast("double").alias("freight_value"),
    )
)

# ── Order Payments ────────────────────────────────────────────────────────────
ingest_csv(
    "olist_order_payments_dataset.csv",
    "raw_order_payments",
    lambda df: df.select(
        "order_id",
        "payment_sequential",
        "payment_type",
        F.col("payment_installments").cast("integer").alias("payment_installments"),
        F.col("payment_value").cast("double").alias("payment_value"),
    )
)

# ── Order Reviews ─────────────────────────────────────────────────────────────
ingest_csv(
    "olist_order_reviews_dataset.csv",
    "raw_order_reviews",
    lambda df: df.select(
        "review_id",
        "order_id",
        F.col("review_score").cast("integer").alias("review_score"),
        "review_comment_title",
        "review_comment_message",
        F.col("review_creation_date").cast("timestamp").alias("review_created_at"),
        F.col("review_answer_timestamp").cast("timestamp").alias("review_answered_at"),
    )
)

# ── Products ──────────────────────────────────────────────────────────────────
ingest_csv(
    "olist_products_dataset.csv",
    "raw_products",
    lambda df: df.select(
        "product_id",
        F.col("product_category_name").alias("category_name_pt"),
        F.col("product_name_lenght").cast("integer").alias("name_length"),
        F.col("product_description_lenght").cast("integer").alias("description_length"),
        F.col("product_photos_qty").cast("integer").alias("photos_qty"),
        F.col("product_weight_g").cast("double").alias("weight_g"),
    )
)

# ── Sellers ───────────────────────────────────────────────────────────────────
ingest_csv(
    "olist_sellers_dataset.csv",
    "raw_sellers",
    lambda df: df.select(
        "seller_id",
        F.col("seller_zip_code_prefix").cast("integer").alias("zip_code_prefix"),
        F.col("seller_city").alias("city"),
        F.col("seller_state").alias("state"),
    )
)

# ── Geolocation ───────────────────────────────────────────────────────────────
ingest_csv(
    "olist_geolocation_dataset.csv",
    "raw_geolocation",
    lambda df: df.select(
        F.col("geolocation_zip_code_prefix").cast("integer").alias("zip_code_prefix"),
        F.col("geolocation_lat").cast("double").alias("lat"),
        F.col("geolocation_lng").cast("double").alias("lng"),
        F.col("geolocation_city").alias("city"),
        F.col("geolocation_state").alias("state"),
    )
)

# ── Category Translation ──────────────────────────────────────────────────────
ingest_csv(
    "product_category_name_translation.csv",
    "raw_category_translation",
)

# COMMAND ----------

# MAGIC %md ## 3. Validate Bronze tables

# COMMAND ----------

print("Bronze table summary:\n")
spark.sql(f"SHOW TABLES IN {BRONZE_DB}").show(truncate=False)

# COMMAND ----------

# Spot check: order status distribution
spark.sql(f"""
    SELECT order_status, count(*) AS n
    FROM {BRONZE_DB}.raw_orders
    GROUP BY order_status
    ORDER BY n DESC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next step
# MAGIC Run **`02_silver_transform`** to clean, join, and enrich the Bronze tables.
