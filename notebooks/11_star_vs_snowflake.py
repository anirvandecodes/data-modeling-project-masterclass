# Databricks notebook source
# MAGIC %md
# MAGIC # 11 — Star Schema vs Snowflake Schema
# MAGIC
# MAGIC Both are dimensional modeling patterns. The difference is how dimension tables
# MAGIC are structured — flat (star) vs normalized (snowflake).
# MAGIC
# MAGIC ## Star Schema
# MAGIC
# MAGIC ```
# MAGIC                 dim_date
# MAGIC                    │
# MAGIC dim_customer ── fact_orders ── dim_product
# MAGIC                    │
# MAGIC                dim_location
# MAGIC ```
# MAGIC
# MAGIC - Each dimension is a **single wide flat table**
# MAGIC - All attributes live in one place — no hierarchy tables
# MAGIC - Fewer joins = faster queries
# MAGIC - Slightly denormalized (some redundancy, e.g., category repeated per product)
# MAGIC - **Best for:** Analytics workloads, BI tools, Kimball methodology
# MAGIC
# MAGIC ## Snowflake Schema
# MAGIC
# MAGIC ```
# MAGIC                    dim_date
# MAGIC                       │
# MAGIC dim_customer ── fact_orders ── dim_product ── dim_subcategory ── dim_category
# MAGIC                       │
# MAGIC             dim_city ── dim_state ── dim_region
# MAGIC ```
# MAGIC
# MAGIC - Dimensions are **normalized** — hierarchies split into separate tables
# MAGIC - Eliminates redundancy
# MAGIC - More joins = slower queries (especially at scale)
# MAGIC - Harder for analysts to write SQL without deep schema knowledge
# MAGIC - **Best for:** Storage-constrained environments, when dimension tables are very large
# MAGIC
# MAGIC ## Recommendation
# MAGIC
# MAGIC For analytics workloads (Databricks, Snowflake, BigQuery, Redshift):
# MAGIC **Use star schema.** Storage is cheap; query speed and analyst simplicity matter more
# MAGIC than eliminating attribute redundancy.

# COMMAND ----------

catalog = "workspace"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo: Build a Snowflake Schema version of dim_product
# MAGIC
# MAGIC Instead of one wide `dim_product`, split it into:
# MAGIC - `dim_category` — category_key, category_name
# MAGIC - `dim_subcategory` — subcategory_key, subcategory_name, category_key (FK)
# MAGIC - `dim_product_sf` — product_key, sku, product_name, subcategory_key (FK), brand, price

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_category

# COMMAND ----------

from pyspark.sql.functions import md5, col, concat_ws

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_category (
        category_key   STRING,
        category_name  STRING
    )
    USING DELTA
""")

spark.sql(f"TRUNCATE TABLE {catalog}.gold.dim_category")

spark.sql(f"""
    INSERT INTO {catalog}.gold.dim_category
    SELECT DISTINCT
        md5(category)   AS category_key,
        category        AS category_name
    FROM {catalog}.bronze.products
""")

spark.sql(f"SELECT * FROM {catalog}.gold.dim_category ORDER BY category_name").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_subcategory

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_subcategory (
        subcategory_key  STRING,
        subcategory_name STRING,
        category_key     STRING
    )
    USING DELTA
""")

spark.sql(f"TRUNCATE TABLE {catalog}.gold.dim_subcategory")

spark.sql(f"""
    INSERT INTO {catalog}.gold.dim_subcategory
    SELECT DISTINCT
        md5(concat_ws('|', category, subcategory))  AS subcategory_key,
        subcategory                                  AS subcategory_name,
        md5(category)                                AS category_key
    FROM {catalog}.bronze.products
""")

spark.sql(f"SELECT * FROM {catalog}.gold.dim_subcategory ORDER BY subcategory_name").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_product_sf (snowflake version)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_product_sf (
        product_key      STRING,
        source_product_id INT,
        sku              STRING,
        product_name     STRING,
        subcategory_key  STRING,
        brand            STRING,
        unit_price       DOUBLE,
        cost_price       DOUBLE
    )
    USING DELTA
""")

spark.sql(f"TRUNCATE TABLE {catalog}.gold.dim_product_sf")

spark.sql(f"""
    INSERT INTO {catalog}.gold.dim_product_sf
    SELECT
        md5(concat_ws('|', cast(product_id as string), sku)) AS product_key,
        cast(product_id as int)                               AS source_product_id,
        sku,
        product_name,
        md5(concat_ws('|', category, subcategory))           AS subcategory_key,
        brand,
        cast(unit_price as double),
        cast(cost_price as double)
    FROM {catalog}.bronze.products
""")

spark.sql(f"SELECT * FROM {catalog}.gold.dim_product_sf LIMIT 5").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Querying the Snowflake Schema
# MAGIC
# MAGIC More joins required — the analyst must traverse the hierarchy.

# COMMAND ----------

spark.sql(f"""
    SELECT
        cat.category_name,
        sub.subcategory_name,
        p.product_name,
        COUNT(DISTINCT f.order_number)   AS orders,
        ROUND(SUM(f.total_amount), 2)    AS total_sales
    FROM {catalog}.gold.fact_orders      f
    JOIN {catalog}.gold.dim_product_sf   p   ON f.product_key   = p.product_key
    JOIN {catalog}.gold.dim_subcategory  sub ON p.subcategory_key = sub.subcategory_key
    JOIN {catalog}.gold.dim_category     cat ON sub.category_key  = cat.category_key
    GROUP BY cat.category_name, sub.subcategory_name, p.product_name
    ORDER BY cat.category_name, total_sales DESC
    LIMIT 15
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Star Schema equivalent — same query, fewer joins

# COMMAND ----------

spark.sql(f"""
    SELECT
        p.category,
        p.subcategory,
        p.product_name,
        COUNT(DISTINCT f.order_number)   AS orders,
        ROUND(SUM(f.total_amount), 2)    AS total_sales
    FROM {catalog}.gold.fact_orders  f
    JOIN {catalog}.gold.dim_product  p ON f.product_key = p.product_key
    GROUP BY p.category, p.subcategory, p.product_name
    ORDER BY p.category, total_sales DESC
    LIMIT 15
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: When to choose each
# MAGIC
# MAGIC | | Star Schema | Snowflake Schema |
# MAGIC |---|---|---|
# MAGIC | Joins | Fewer (1 per dim) | More (traverse hierarchy) |
# MAGIC | Query complexity | Simple | More complex |
# MAGIC | Storage | Slightly more (redundancy) | Less |
# MAGIC | BI tool friendliness | High | Lower |
# MAGIC | Best for | Analytics, BI, Databricks Gold layer | Storage-constrained, very large dims |
# MAGIC
# MAGIC **Rule of thumb:** Start with star schema. Normalize into snowflake only if you have
# MAGIC a concrete reason (e.g., category table has 50+ attributes that don't belong on every product row).
