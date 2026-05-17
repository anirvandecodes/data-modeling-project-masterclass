# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 05 — Star Schema vs Snowflake Schema
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Star Schema Anatomy
# MAGIC
# MAGIC A **star schema** is the most widely used design pattern for analytical data modeling. It gets its name from its visual appearance: one central **fact table** surrounded by **dimension tables** that radiate outward like the points of a star.
# MAGIC
# MAGIC ```
# MAGIC                        ┌────────────────┐
# MAGIC                        │   dim_date     │
# MAGIC                        │  (When?)       │
# MAGIC                        └───────┬────────┘
# MAGIC                                │
# MAGIC   ┌────────────────┐           │           ┌────────────────┐
# MAGIC   │  dim_customer  │           │           │  dim_product   │
# MAGIC   │  (Who?)        ├───────────┤           │  (What?)       │
# MAGIC   └────────────────┘           │           └───────┬────────┘
# MAGIC                                │                   │
# MAGIC                        ┌───────┴───────────────────┘
# MAGIC                        │       fact_orders          │
# MAGIC                        │  order_key                 │
# MAGIC                        │  customer_key  (FK)        │
# MAGIC                        │  product_key   (FK)        │
# MAGIC                        │  location_key  (FK)        │
# MAGIC                        │  order_date_key (FK)       │
# MAGIC                        │  quantity                  │
# MAGIC                        │  total_amount              │
# MAGIC                        └───────────┬────────────────┘
# MAGIC                                    │
# MAGIC                        ┌───────────┴────────────┐
# MAGIC                        │    dim_location         │
# MAGIC                        │    (Where?)             │
# MAGIC                        └────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Key properties of a star schema:
# MAGIC
# MAGIC | Property | Description |
# MAGIC |---|---|
# MAGIC | **Clear grain** | Every row in the fact table represents the same unit of measurement (one order line) |
# MAGIC | **Few joins** | Any business question needs only 1–2 joins: fact → one or two dims |
# MAGIC | **Analyst-friendly** | Dimension tables are wide and flat — one lookup per question |
# MAGIC | **Denormalized** | Category and subcategory are in the same `dim_product` row, not a separate table |
# MAGIC | **Fast** | Fewer joins + columnar storage = very fast aggregation queries |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Snowflake Schema Anatomy
# MAGIC
# MAGIC A **snowflake schema** is a variation where dimension tables are **normalized** — split into sub-tables the way OLTP tables are. The resulting diagram looks like a snowflake (more complex, more branching).
# MAGIC
# MAGIC ```
# MAGIC                             ┌──────────────────┐
# MAGIC                             │   dim_category   │
# MAGIC                             │  category_key PK │
# MAGIC                             │  category_name   │
# MAGIC                             └────────┬─────────┘
# MAGIC                                      │ (1:M)
# MAGIC                             ┌────────┴─────────┐
# MAGIC                             │  dim_subcategory │
# MAGIC                             │  subcategory_key │
# MAGIC                             │  subcategory_name│
# MAGIC                             │  category_key FK │
# MAGIC                             └────────┬─────────┘
# MAGIC                                      │ (1:M)
# MAGIC   ┌────────────┐           ┌─────────┴────────┐
# MAGIC   │ fact_orders│──────────►│ dim_product_sf   │
# MAGIC   └────────────┘           │  product_key     │
# MAGIC                            │  product_name    │
# MAGIC                            │  subcategory_key │ ← FK (not inline category)
# MAGIC                            │  brand           │
# MAGIC                            │  unit_price      │
# MAGIC                            └──────────────────┘
# MAGIC ```
# MAGIC
# MAGIC In the snowflake version, `dim_product` does NOT contain `category_name` directly. Instead it has a `subcategory_key` FK that points to `dim_subcategory`, which in turn has a `category_key` FK pointing to `dim_category`.
# MAGIC
# MAGIC To query "sales by category" you now need to join through 3 tables instead of 1.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Building the Snowflake Demo Tables
# MAGIC
# MAGIC Let's build both versions so we can actually compare them. We'll create:
# MAGIC - `dim_category` — the top of the product hierarchy
# MAGIC - `dim_subcategory` — the middle, linking to category
# MAGIC - `dim_product_sf` — the snowflake version of product, with FK to subcategory (not inline category)
# MAGIC
# MAGIC Later we'll also build `dim_product` (the star version) in Notebook 10.

# COMMAND ----------

catalog = "workspace"

# Step 1: Build dim_category (top of hierarchy)
print("Building dim_category...")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_category (
        category_key  STRING,
        category_name STRING
    ) USING DELTA
""")
spark.sql(f"TRUNCATE TABLE {catalog}.gold.dim_category")
spark.sql(f"""
    INSERT INTO {catalog}.gold.dim_category
    SELECT DISTINCT
        md5(category)   AS category_key,
        category        AS category_name
    FROM {catalog}.bronze.products
    WHERE category IS NOT NULL
""")
count = spark.sql(f"SELECT COUNT(*) FROM {catalog}.gold.dim_category").collect()[0][0]
print(f"  {catalog}.gold.dim_category: {count} rows")

# COMMAND ----------

# Step 2: Build dim_subcategory (middle of hierarchy, FK to category)
print("Building dim_subcategory...")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_subcategory (
        subcategory_key  STRING,
        subcategory_name STRING,
        category_key     STRING
    ) USING DELTA
""")
spark.sql(f"TRUNCATE TABLE {catalog}.gold.dim_subcategory")
spark.sql(f"""
    INSERT INTO {catalog}.gold.dim_subcategory
    SELECT DISTINCT
        md5(concat_ws('|', category, subcategory))  AS subcategory_key,
        subcategory                                  AS subcategory_name,
        md5(category)                                AS category_key
    FROM {catalog}.bronze.products
    WHERE subcategory IS NOT NULL
""")
count = spark.sql(f"SELECT COUNT(*) FROM {catalog}.gold.dim_subcategory").collect()[0][0]
print(f"  {catalog}.gold.dim_subcategory: {count} rows")

# COMMAND ----------

# Step 3: Build dim_product_sf (snowflake version — no inline category/subcategory)
# Instead of category and subcategory columns, it has subcategory_key (FK)
print("Building dim_product_sf (snowflake version)...")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_product_sf (
        product_key       STRING,
        source_product_id INT,
        sku               STRING,
        product_name      STRING,
        subcategory_key   STRING,
        brand             STRING,
        unit_price        DOUBLE
    ) USING DELTA
""")
spark.sql(f"TRUNCATE TABLE {catalog}.gold.dim_product_sf")
spark.sql(f"""
    INSERT INTO {catalog}.gold.dim_product_sf
    SELECT
        md5(concat_ws('|', cast(product_id AS STRING), sku))  AS product_key,
        cast(product_id AS INT)                                AS source_product_id,
        sku,
        product_name,
        md5(concat_ws('|', category, subcategory))            AS subcategory_key,
        brand,
        cast(unit_price AS DOUBLE)                            AS unit_price
    FROM {catalog}.bronze.products
""")
count = spark.sql(f"SELECT COUNT(*) FROM {catalog}.gold.dim_product_sf").collect()[0][0]
print(f"  {catalog}.gold.dim_product_sf: {count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seeing the Difference
# MAGIC
# MAGIC Let's look at both versions of the product dimension side by side to understand the structural difference.

# COMMAND ----------

# Snowflake version: dim_product_sf has subcategory_key (a FK) instead of category/subcategory text
print("SNOWFLAKE VERSION: dim_product_sf")
print("Notice: subcategory_key is a hash FK — not readable category text\n")
spark.sql(f"""
    SELECT product_key, source_product_id, product_name, subcategory_key, brand, unit_price
    FROM {catalog}.gold.dim_product_sf
    ORDER BY source_product_id
    LIMIT 8
""").show(truncate=False)

# COMMAND ----------

# The hierarchy tables that the FK points to
print("dim_subcategory (middle of hierarchy):")
spark.sql(f"SELECT * FROM {catalog}.gold.dim_subcategory ORDER BY subcategory_name").show(truncate=False)

print("\ndim_category (top of hierarchy):")
spark.sql(f"SELECT * FROM {catalog}.gold.dim_category ORDER BY category_name").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Star vs Snowflake: The Same Query, Both Ways
# MAGIC
# MAGIC Now let's run the same business question — "What is total sales by product category?" — using both schema designs.
# MAGIC
# MAGIC **NOTE:** The queries below against `fact_orders` will not work until you have completed Notebook 12 (Building fact_orders). Run this cell after completing that notebook.
# MAGIC
# MAGIC We'll show the query structure for both approaches, and demonstrate the multi-table join required for the snowflake version using just the dimension tables.

# COMMAND ----------

# ============================================================
# STAR SCHEMA QUERY (run after completing Notebook 12)
# ============================================================
# Business question: Total sales by product category
#
# Star schema: ONLY 2 tables, 1 join
# fact_orders JOIN dim_product — category is already in dim_product

# NOTE: Run this cell AFTER completing Notebook 12 (fact_orders)

star_query = f"""
    SELECT
        p.category,
        COUNT(f.order_key)          AS total_orders,
        SUM(f.total_amount)         AS total_revenue,
        ROUND(AVG(f.total_amount), 2) AS avg_order_value
    FROM {catalog}.gold.fact_orders f
    JOIN {catalog}.gold.dim_product p ON f.product_key = p.product_key
    GROUP BY p.category
    ORDER BY total_revenue DESC
"""

print("STAR SCHEMA QUERY:")
print("Tables needed: fact_orders + dim_product (2 tables, 1 join)\n")
print(star_query)

# Uncomment the line below after completing Notebook 12:
# spark.sql(star_query).show()

# COMMAND ----------

# ============================================================
# SNOWFLAKE SCHEMA QUERY (can run now — no fact_orders needed
# for this structural demo)
# ============================================================
# Snowflake version: requires dim_product_sf + dim_subcategory + dim_category
# That's 3 dimension tables — and we haven't even touched fact_orders yet.

print("SNOWFLAKE SCHEMA QUERY:")
print("Tables needed: fact_orders + dim_product_sf + dim_subcategory + dim_category")
print("(4 tables total, 3 joins — just to answer the same question)\n")

# Demonstrate the multi-join just on the dimension side (no fact needed for structure demo)
spark.sql(f"""
    SELECT
        c.category_name,
        s.subcategory_name,
        COUNT(p.product_key) AS products_in_subcategory
    FROM {catalog}.gold.dim_product_sf p
    JOIN {catalog}.gold.dim_subcategory s ON p.subcategory_key = s.subcategory_key
    JOIN {catalog}.gold.dim_category    c ON s.category_key    = c.category_key
    GROUP BY c.category_name, s.subcategory_name
    ORDER BY c.category_name, s.subcategory_name
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC The second query — even just joining the three snowflake dimension tables together — already requires 3 joins and 3 tables. With the fact table added, that becomes 4 tables and 4 joins just to answer "sales by category."
# MAGIC
# MAGIC Compare that to the star schema: `fact_orders JOIN dim_product`. That's it. One join.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Star vs Snowflake — Side-by-Side Comparison
# MAGIC
# MAGIC | Dimension | Star Schema | Snowflake Schema |
# MAGIC |---|---|---|
# MAGIC | **Table count** for a typical query | 2–3 | 4–6 |
# MAGIC | **Join count** for a typical query | 1–2 | 3–5 |
# MAGIC | **Storage** | Slightly more (repeated strings) | Slightly less (normalized) |
# MAGIC | **Query complexity** | Low — analysts can write it easily | High — requires knowing the hierarchy depth |
# MAGIC | **Performance** | Faster (columnar DBs optimize for wide scans) | Slower (more joins) |
# MAGIC | **Maintainability** | Simple — one table to update | Complex — must update hierarchy chain |
# MAGIC | **When to use** | Analytics, BI tools, ad-hoc queries | When storage is critical or hierarchy is very deep |
# MAGIC | **Databricks recommendation** | ✅ Star schema | Only when explicitly needed |
# MAGIC
# MAGIC ### The Rule
# MAGIC
# MAGIC > **Use star schema for analytics. Normalize into a snowflake only when you have a concrete, measurable reason — typically very deep hierarchies (4+ levels) where the denormalization creates significant storage bloat.**
# MAGIC
# MAGIC In practice, at any modern cloud data warehouse (Databricks, Snowflake, BigQuery), storage is cheap and compute is the bottleneck. Extra joins cost query time. Denormalized wide tables cost almost nothing extra in storage. **Star schema wins.**
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Our Final Decision: Star Schema
# MAGIC
# MAGIC For this masterclass, we are building a **star schema**. Here is the final schema we will construct:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────┐
# MAGIC │                FINAL STAR SCHEMA — workspace.gold.*                 │
# MAGIC └─────────────────────────────────────────────────────────────────────┘
# MAGIC
# MAGIC
# MAGIC              ┌──────────────────────────┐
# MAGIC              │        dim_date          │
# MAGIC              │  PK: date_key (INT)      │
# MAGIC              │  year, quarter, month    │
# MAGIC              │  day_name, is_weekend    │
# MAGIC              │  is_month_end, ...       │
# MAGIC              └────────────┬─────────────┘
# MAGIC                           │
# MAGIC ┌────────────────────┐    │    ┌─────────────────────┐
# MAGIC │    dim_customer    │    │    │    dim_product       │
# MAGIC │  PK: customer_key  ├────┤    │  PK: product_key    │
# MAGIC │  full_name, email  │    │    │  product_name       │
# MAGIC │  segment, city     │    │    │  category           │
# MAGIC │  signup_date ...   │    │    │  subcategory, brand │
# MAGIC └────────┬───────────┘    │    └──────────┬──────────┘
# MAGIC          │                │               │
# MAGIC          │         ┌──────┴───────────────┘
# MAGIC          │         │
# MAGIC          └────────►│         fact_orders
# MAGIC                    │  ─────────────────────────
# MAGIC                    │  PK: order_key
# MAGIC                    │  FK: customer_key
# MAGIC                    │  FK: product_key
# MAGIC                    │  FK: location_key
# MAGIC                    │  FK: order_date_key
# MAGIC                    │  quantity, unit_price
# MAGIC                    │  total_amount, channel
# MAGIC                    └──────────┬──────────────
# MAGIC                               │
# MAGIC                    ┌──────────┴──────────┐
# MAGIC                    │                     │
# MAGIC              ┌─────┴──────────┐    ┌────┴──────────────┐
# MAGIC              │  dim_location  │    │    fact_returns    │
# MAGIC              │  PK: loc_key   │    │  PK: return_key   │
# MAGIC              │  city, state   │    │  FK: customer_key │
# MAGIC              │  region        │    │  FK: product_key  │
# MAGIC              │  country       │    │  FK: return_date  │
# MAGIC              └────────────────┘    │  return_amount    │
# MAGIC                                    │  reason           │
# MAGIC                                    └───────────────────┘
# MAGIC
# MAGIC   dim_customer, dim_product, dim_location, dim_date are
# MAGIC   CONFORMED DIMENSIONS — both fact tables share them.
# MAGIC ```
# MAGIC
# MAGIC The fact tables share the same dimension tables. This is the key to consistent analytics: when you filter `dim_customer` to the "Gold" segment, it filters consistently whether you're looking at orders or returns.
# MAGIC
# MAGIC ---

# COMMAND ----------

# Verify our snowflake demo tables were created correctly
print("Snowflake demo tables created in workspace.gold:")
spark.sql(f"SHOW TABLES IN {catalog}.gold").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC In this notebook we:
# MAGIC 1. Defined star schema anatomy: fact table at center, flat wide dimension tables
# MAGIC 2. Defined snowflake schema: same but dimensions are normalized into sub-tables
# MAGIC 3. Built three snowflake demo tables: `dim_category`, `dim_subcategory`, `dim_product_sf`
# MAGIC 4. Showed the difference in query complexity: star = 1 join, snowflake = 3+ joins for the same question
# MAGIC 5. Compared both approaches across storage, performance, and maintainability
# MAGIC 6. Made our schema decision: **star schema** for this project
# MAGIC
# MAGIC **Next up — Notebook 06: Physical Setup.** We'll configure our Databricks environment — schemas, volumes, Delta Lake format, partitioning, and Z-ordering — and make all the physical storage decisions for our Gold tables.
