# Databricks notebook source

# MAGIC %md
# MAGIC # Surrogate Keys & Building dim_product
# MAGIC
# MAGIC **Notebook 09 of the Data Modeling Masterclass**
# MAGIC
# MAGIC In the previous notebook we built `dim_date` — a dimension with no natural key complexity. Now we tackle `dim_product`, which introduces one of the most important concepts in dimensional modeling: **surrogate keys**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is a Surrogate Key?
# MAGIC
# MAGIC A **surrogate key** is a system-generated identifier for a dimension row — completely independent of the source system's natural key.
# MAGIC
# MAGIC A **natural key** (also called a business key) is the identifier that exists in the source system: `product_id`, `customer_id`, `sku`, etc.
# MAGIC
# MAGIC ### The Problem with Natural Keys in Fact Tables
# MAGIC
# MAGIC Why not just use `product_id` from the source system as the join key? Three scenarios that break this assumption in practice:
# MAGIC
# MAGIC **1. Source systems change.** The ERP system is replaced. The new system resets `product_id` sequences starting at 1. Now your fact table has `product_id = 1` pointing to two completely different products — one from the old system, one from the new. Your dimension can't distinguish them.
# MAGIC
# MAGIC **2. Two sources merge.** The company acquires a competitor. Both systems have `product_id = 1001`, but they refer to entirely different products. You need a new, unified key that avoids collision.
# MAGIC
# MAGIC **3. Type 2 SCD rows.** (We'll cover this in notebook 12.) When customer attributes change, you insert a new row for the same customer with a new surrogate key. The natural key (`customer_id`) stays the same across both rows. The surrogate key is what differentiates them.
# MAGIC
# MAGIC **The rule:** The fact table should never depend on a raw source ID. It should always join on a surrogate key that lives in the dimension table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Three Ways to Generate Surrogate Keys
# MAGIC
# MAGIC ### Option 1: ROW_NUMBER()
# MAGIC ```sql
# MAGIC SELECT ROW_NUMBER() OVER (ORDER BY product_id) AS product_key, *
# MAGIC FROM bronze.products
# MAGIC ```
# MAGIC **Problem:** Not idempotent. Run this query twice and the numbers will be different if any rows were added or deleted between runs. You can't reliably reload the dimension table.
# MAGIC
# MAGIC ### Option 2: IDENTITY / AUTOINCREMENT
# MAGIC ```sql
# MAGIC CREATE TABLE dim_product (product_key BIGINT GENERATED ALWAYS AS IDENTITY, ...)
# MAGIC ```
# MAGIC **Problem:** Works on a single node database (Postgres, SQL Server). In a distributed system like Spark, generating sequential integers requires coordination across all workers — it becomes a bottleneck and can serialize your writes.
# MAGIC
# MAGIC ### Option 3: MD5 Hash (What We Use)
# MAGIC ```python
# MAGIC md5(concat_ws('|', product_id, sku))
# MAGIC ```
# MAGIC **Advantages:**
# MAGIC - **Deterministic:** Same input always produces the same 32-character hex string. Load the table 100 times — the keys are identical every time.
# MAGIC - **Distributed-friendly:** Each row computes its own key independently. No coordination needed.
# MAGIC - **Collision-resistant:** MD5 has 2^128 possible values. For the scale of data you'll encounter in a business context, collisions are astronomically unlikely.
# MAGIC - **Mergeable across sources:** Two systems can contribute rows without key conflicts, as long as you include a source-system discriminator in the hash input.
# MAGIC
# MAGIC **The hash formula for products:**
# MAGIC ```python
# MAGIC md5(concat_ws('|', product_id, sku))
# MAGIC ```
# MAGIC We hash both `product_id` AND `sku` to make the key stable even if `product_id` alone is ambiguous. The `concat_ws('|', ...)` uses a pipe delimiter to avoid accidental collisions from concatenation (e.g., `12|3` vs `1|23`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Bronze and Generate Surrogate Keys
# MAGIC
# MAGIC We read from `workspace.bronze.products`, cast columns to the correct types, and apply the MD5 surrogate key. The resulting `product_key` column is a 32-character hex string — compact, deterministic, globally unique.

# COMMAND ----------

from pyspark.sql.functions import md5, concat_ws, col

catalog = "workspace"

bronze_products = spark.table(f"{catalog}.bronze.products").select(
    col("product_id").cast("int").alias("source_product_id"),
    col("sku"),
    col("product_name"),
    col("category"),
    col("subcategory"),
    col("brand"),
    col("unit_price").cast("double"),
    col("cost_price").cast("double"),
)

dim_product = bronze_products.withColumn(
    "product_key",
    md5(concat_ws("|", col("source_product_id").cast("string"), col("sku")))
).select(
    "product_key",
    "source_product_id",
    "sku",
    "product_name",
    "category",
    "subcategory",
    "brand",
    "unit_price",
    "cost_price",
)

print(f"Bronze products loaded: {bronze_products.count()} rows")
print("Sample surrogate keys:")
dim_product.select("product_key", "source_product_id", "sku", "product_name").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: The Unknown Member Row
# MAGIC
# MAGIC Here's a scenario that breaks naively-built fact tables: **a fact record arrives that references a product that doesn't exist in your dimension.**
# MAGIC
# MAGIC This happens more often than you'd think:
# MAGIC - Data arrives out of order (fact before dimension)
# MAGIC - A product was deleted from the source but old orders still reference it
# MAGIC - Data quality issues — a `product_id` that was never loaded into the dimension
# MAGIC
# MAGIC If you use an `INNER JOIN` when loading facts, **these rows are silently dropped.** Your fact table will have fewer rows than your source — and you won't know why.
# MAGIC
# MAGIC If you use a `LEFT JOIN`, you get `NULL` in `product_key` — which breaks GROUP BY queries and makes your reports show mysterious null rows.
# MAGIC
# MAGIC ### The Solution: The Unknown Member Row
# MAGIC
# MAGIC Insert a single "Unknown Product" row into `dim_product` with `product_key = 'unknown'`. Then in the fact table load, use:
# MAGIC ```sql
# MAGIC COALESCE(dp.product_key, 'unknown') AS product_key
# MAGIC ```
# MAGIC
# MAGIC Now any fact that can't resolve its product key falls to the unknown member instead of being dropped or having a null. The row is preserved. The report shows a small "Unknown" category. A data engineer investigates. **No data is lost.**
# MAGIC
# MAGIC This is a standard Kimball pattern. Every dimension should have an unknown member.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

unknown_schema = StructType([
    StructField("product_key", StringType(), True),
    StructField("source_product_id", IntegerType(), True),
    StructField("sku", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("subcategory", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("cost_price", DoubleType(), True),
])

unknown = spark.createDataFrame([{
    "product_key": "unknown",
    "source_product_id": -1,
    "sku": "UNKNOWN",
    "product_name": "Unknown Product",
    "category": "Unknown",
    "subcategory": "Unknown",
    "brand": "Unknown",
    "unit_price": 0.0,
    "cost_price": 0.0,
}], schema=unknown_schema)

# Union the unknown member FIRST so it appears at the top of the table
dim_product = unknown.unionByName(dim_product)

print("Unknown member added. Final schema:")
dim_product.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Write to Gold
# MAGIC
# MAGIC We write `dim_product` as a Delta table in the Gold layer. Mode `overwrite` means a full reload every time — safe because MD5 keys are deterministic and the dimension is relatively small.
# MAGIC
# MAGIC For very large dimensions, you'd switch to a MERGE-based incremental load (same pattern as SCD Type 1 in notebook 11). For product catalogs of typical size, overwrite is simpler and perfectly adequate.

# COMMAND ----------

dim_product.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.gold.dim_product")

total_count = dim_product.count()
print(f"dim_product written to {catalog}.gold.dim_product")
print(f"Total rows: {total_count} (includes 1 unknown member)")
print(f"Real products: {total_count - 1}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Preview the Dimension
# MAGIC
# MAGIC Let's preview sorted by `category` and `subcategory`. This reveals the **product hierarchy** embedded in the dimension: Category → Subcategory → Product.
# MAGIC
# MAGIC In a snowflake schema, you'd normalize this into separate `dim_category` and `dim_subcategory` tables with their own foreign keys. In a star schema, you denormalize — all levels of the hierarchy live in a single flat row. This is what makes star schema queries fast and simple: no extra joins needed to traverse the hierarchy.

# COMMAND ----------

spark.sql(f"""
    SELECT
        product_key,
        source_product_id,
        sku,
        product_name,
        category,
        subcategory,
        unit_price
    FROM {catalog}.gold.dim_product
    ORDER BY category, subcategory
    LIMIT 12
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Concept | What We Did |
# MAGIC |---------|-------------|
# MAGIC | Surrogate key | `md5(concat_ws('|', product_id, sku))` — deterministic, distributed |
# MAGIC | Natural key preserved | `source_product_id` kept for traceability |
# MAGIC | Unknown member | Row with `product_key = 'unknown'` absorbs unresolvable FK references |
# MAGIC | Hierarchy denormalized | Category → Subcategory in same row (star schema style) |
# MAGIC | Write mode | Overwrite — safe because MD5 keys are deterministic |
# MAGIC
# MAGIC **Next notebook:** `10_dim_location.py` — Building dim_location (same pattern, simpler dimension).
