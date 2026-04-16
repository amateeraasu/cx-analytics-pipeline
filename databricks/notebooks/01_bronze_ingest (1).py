# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Ingestion to Delta Lake
# MAGIC
# MAGIC **Medallion layer:** Bronze (raw)
# MAGIC **Source:** Olist Brazilian E-Commerce CSVs (9 files, \~100K orders)
# MAGIC **Output:** 9 managed Delta tables in `workspace.olist_bronze`
# MAGIC
# MAGIC This notebook ingests the raw Olist CSV files into Unity Catalog managed
# MAGIC Delta tables with minimal transformation — column renaming only, no
# MAGIC business logic. All source data is preserved exactly as delivered.
# MAGIC
# MAGIC **Run this notebook first.** Silver and Gold depend on it.
# MAGIC
# MAGIC ## Setup
# MAGIC 1. Download the dataset: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
# MAGIC 2. Upload all 9 CSV files to the UC Volume: `/Volumes/workspace/olist_bronze/raw_files/`
# MAGIC    - In Databricks UI: Catalog → workspace → olist_bronze → raw_files → Upload
# MAGIC 3. Attach this notebook to compute (serverless or DBR 13.0+ cluster)
# MAGIC 4. Run All

# COMMAND ----------

# MAGIC %md ## 0. Configuration

# COMMAND ----------

# Paths — upload CSVs to the UC Volume below
RAW_PATH   = "/Volumes/workspace/olist_bronze/raw_files"
BRONZE_DB  = "workspace.olist_bronze"

print(f"Raw CSV path : {RAW_PATH}")
print(f"Target schema: {BRONZE_DB}")

# COMMAND ----------

# MAGIC %md ## 1. Helper — ingest one CSV to Delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *


def ingest_csv(filename: str, table_name: str, transformations=None) -> int:
    """
    Read a CSV from a UC Volume, optionally apply column renames/casts,
    and write to a managed Delta table in Unity Catalog.

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
        .option("multiLine", "true")
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

    full_table_name = f"{BRONZE_DB}.{table_name}"
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )

    n = spark.table(full_table_name).count()
    print(f"  ✓  {full_table_name:55s} {n:>9,} rows")
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
        # Physical dimensions — needed by dbt stg_products
        F.col("product_length_cm").cast("double").alias("product_length_cm"),
        F.col("product_height_cm").cast("double").alias("product_height_cm"),
        F.col("product_width_cm").cast("double").alias("product_width_cm"),
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
# MAGIC ## 4. dbt compatibility views
# MAGIC
# MAGIC The Bronze tables renamed several columns for clarity (e.g. `customer_city` → `city`).
# MAGIC The dbt staging models expect the original CSV column names, so we create lightweight
# MAGIC views that restore the original names.
# MAGIC
# MAGIC After running these views, the dbt pipeline can run on Databricks unchanged:
# MAGIC ```bash
# MAGIC export DBT_OLIST_SOURCE_SCHEMA=olist_bronze
# MAGIC dbt run --target databricks --profiles-dir .
# MAGIC ```

# COMMAND ----------

print("Creating dbt compatibility views in olist_bronze...\n")

# orders — no column renames, just alias the raw table
spark.sql(f"CREATE OR REPLACE VIEW {BRONZE_DB}.orders AS SELECT * FROM {BRONZE_DB}.raw_orders")
print("  ✓  olist_bronze.orders")

# customers — restore prefixed column names
spark.sql(f"""
    CREATE OR REPLACE VIEW {BRONZE_DB}.customers AS
    SELECT
        customer_id,
        customer_unique_id,
        zip_code_prefix  AS customer_zip_code_prefix,
        city             AS customer_city,
        state            AS customer_state
    FROM {BRONZE_DB}.raw_customers
""")
print("  ✓  olist_bronze.customers")

# order_items — no column renames
spark.sql(f"CREATE OR REPLACE VIEW {BRONZE_DB}.order_items AS SELECT * FROM {BRONZE_DB}.raw_order_items")
print("  ✓  olist_bronze.order_items")

# order_payments — no column renames
spark.sql(f"CREATE OR REPLACE VIEW {BRONZE_DB}.order_payments AS SELECT * FROM {BRONZE_DB}.raw_order_payments")
print("  ✓  olist_bronze.order_payments")

# order_reviews — restore original date column names
spark.sql(f"""
    CREATE OR REPLACE VIEW {BRONZE_DB}.order_reviews AS
    SELECT
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_created_at  AS review_creation_date,
        review_answered_at AS review_answer_timestamp
    FROM {BRONZE_DB}.raw_order_reviews
""")
print("  ✓  olist_bronze.order_reviews")

# products — restore original column names and spellings (incl. typos in source data)
spark.sql(f"""
    CREATE OR REPLACE VIEW {BRONZE_DB}.products AS
    SELECT
        product_id,
        category_name_pt    AS product_category_name,
        name_length         AS product_name_lenght,
        description_length  AS product_description_lenght,
        photos_qty          AS product_photos_qty,
        weight_g            AS product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm
    FROM {BRONZE_DB}.raw_products
""")
print("  ✓  olist_bronze.products")

# sellers — restore prefixed column names
spark.sql(f"""
    CREATE OR REPLACE VIEW {BRONZE_DB}.sellers AS
    SELECT
        seller_id,
        zip_code_prefix  AS seller_zip_code_prefix,
        city             AS seller_city,
        state            AS seller_state
    FROM {BRONZE_DB}.raw_sellers
""")
print("  ✓  olist_bronze.sellers")

# geolocation — restore original prefixed column names
spark.sql(f"""
    CREATE OR REPLACE VIEW {BRONZE_DB}.geolocation AS
    SELECT
        zip_code_prefix  AS geolocation_zip_code_prefix,
        lat              AS geolocation_lat,
        lng              AS geolocation_lng,
        city             AS geolocation_city,
        state            AS geolocation_state
    FROM {BRONZE_DB}.raw_geolocation
""")
print("  ✓  olist_bronze.geolocation")

# category_translation — no column renames
spark.sql(f"""
    CREATE OR REPLACE VIEW {BRONZE_DB}.category_translation
    AS SELECT * FROM {BRONZE_DB}.raw_category_translation
""")
print("  ✓  olist_bronze.category_translation")

print("\nAll 8 compatibility views created.")
print(f"Run: dbt run --target databricks --profiles-dir . (with DBT_OLIST_SOURCE_SCHEMA=olist_bronze)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next step
# MAGIC Run **`02_silver_transform`** to clean, join, and enrich the Bronze tables.
# MAGIC
# MAGIC Or, if running the full dbt pipeline on Databricks:
# MAGIC ```bash
# MAGIC export DBT_OLIST_SOURCE_SCHEMA=olist_bronze
# MAGIC export DBT_DATABRICKS_HOST=<your-workspace>.azuredatabricks.net
# MAGIC export DBT_DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<warehouse-id>
# MAGIC export DBT_DATABRICKS_TOKEN=<personal-access-token>
# MAGIC dbt run --target databricks --profiles-dir .
# MAGIC ```
