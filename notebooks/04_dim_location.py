# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — dim_location
# MAGIC Build the location dimension with MD5 surrogate key.
# MAGIC No SCD needed — store locations rarely change meaningfully for analytics.

# COMMAND ----------

from pyspark.sql.functions import md5, concat_ws, col

catalog = "workspace"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load from Bronze

# COMMAND ----------

bronze_locations = spark.table(f"{catalog}.bronze.locations").select(
    col("location_id").cast("int").alias("source_location_id"),
    col("city"),
    col("state"),
    col("country"),
    col("region"),
    col("postal_code"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate MD5 surrogate key

# COMMAND ----------

dim_location = bronze_locations.withColumn(
    "location_key",
    md5(col("source_location_id").cast("string"))
).select(
    "location_key",
    "source_location_id",
    "city",
    "state",
    "country",
    "region",
    "postal_code",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unknown member row

# COMMAND ----------

unknown = spark.createDataFrame([{
    "location_key":       "unknown",
    "source_location_id": -1,
    "city":               "Unknown",
    "state":              "Unknown",
    "country":            "Unknown",
    "region":             "Unknown",
    "postal_code":        "00000",
}])

dim_location = unknown.unionByName(dim_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold

# COMMAND ----------

(dim_location.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.gold.dim_location"))

print(f"dim_location built: {dim_location.count()} rows (including 1 unknown member)")

# COMMAND ----------

spark.sql(f"""
    SELECT location_key, city, state, region, country
    FROM {catalog}.gold.dim_location
    ORDER BY region, state
""").show(truncate=False)
