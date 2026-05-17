# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — dim_product
# MAGIC Build the product dimension with an **MD5 hash surrogate key**.
# MAGIC
# MAGIC **MD5 surrogate key advantages:**
# MAGIC - Deterministic: same input always produces the same key
# MAGIC - Distributed-friendly: no sequence generator, no bottleneck in Spark
# MAGIC - Portable: works identically on any platform
# MAGIC
# MAGIC **Hierarchy:** Category → Subcategory → Product Name → SKU

# COMMAND ----------

from pyspark.sql.functions import md5, concat_ws, col, lit
from pyspark.sql import Row

catalog = "workspace"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load from Bronze

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate MD5 surrogate key
# MAGIC
# MAGIC Hash the natural business key (product_id + sku) so the key is stable
# MAGIC even if source system IDs change across environments.

# COMMAND ----------

dim_product = bronze_products.withColumn(
    "product_key",
    md5(concat_ws("|",
        col("source_product_id").cast("string"),
        col("sku")
    ))
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unknown member row
# MAGIC
# MAGIC Every dimension needs an "unknown" row with a fixed key.
# MAGIC When a fact arrives referencing a product that doesn't exist yet
# MAGIC (late-arriving dimension), the fact joins to this row instead of failing.

# COMMAND ----------

unknown = spark.createDataFrame([{
    "product_key":        "unknown",
    "source_product_id":  -1,
    "sku":                "UNKNOWN",
    "product_name":       "Unknown Product",
    "category":           "Unknown",
    "subcategory":        "Unknown",
    "brand":              "Unknown",
    "unit_price":         0.0,
    "cost_price":         0.0,
}])

dim_product = unknown.unionByName(dim_product)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold

# COMMAND ----------

(dim_product.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.gold.dim_product"))

print(f"dim_product built: {dim_product.count()} rows (including 1 unknown member)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview — show the hierarchy

# COMMAND ----------

spark.sql(f"""
    SELECT product_key, source_product_id, sku, product_name,
           category, subcategory, brand, unit_price
    FROM {catalog}.gold.dim_product
    ORDER BY category, subcategory
    LIMIT 10
""").show(truncate=False)
