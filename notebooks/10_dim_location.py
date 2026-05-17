# Databricks notebook source

# MAGIC %md
# MAGIC # Building dim_location
# MAGIC
# MAGIC **Notebook 10 of the Data Modeling Masterclass**
# MAGIC
# MAGIC In notebook 09 we introduced surrogate keys and the unknown member pattern while building `dim_product`. This notebook applies exactly the same pattern to `dim_location`. The goal is to reinforce the pattern through repetition — by the time you build your third dimension, the approach should feel automatic.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why dim_location is a Simple Dimension
# MAGIC
# MAGIC Not all dimensions require the same complexity. `dim_location` is a good example of a **static dimension** — one where history tracking (SCD Type 2) adds no analytical value.
# MAGIC
# MAGIC Think about what a location represents: a city, state, and region. When does a location "change"? In rare edge cases — a city gets re-zoned, or a region boundary shifts. But for analytics purposes:
# MAGIC
# MAGIC - You don't need to know what region Austin was in "at the time of the order." Austin is still in the South region.
# MAGIC - If a location record gets updated (e.g., a typo in the state is corrected), you want the correction reflected everywhere — past and future.
# MAGIC
# MAGIC This is the hallmark of a dimension that only needs **SCD Type 1 (overwrite)** — or in our case, even simpler: a full **overwrite rebuild** every time, because the dimension is small and static.
# MAGIC
# MAGIC **Design decision:** `dim_location` uses the same MD5 surrogate key pattern as `dim_product`, plus the unknown member row for referential integrity. No SCD complexity needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Bronze Locations and Generate Surrogate Keys
# MAGIC
# MAGIC We read from `workspace.bronze.locations` and generate a surrogate key by hashing `source_location_id`. Since location IDs are already unique within a single source system, hashing just the ID is sufficient. We don't need to include additional columns in the hash like we did for products.
# MAGIC
# MAGIC The resulting `location_key` is a deterministic 32-character hex string — same output on every run.

# COMMAND ----------

from pyspark.sql.functions import md5, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

catalog = "workspace"

bronze_locations = spark.table(f"{catalog}.bronze.locations").select(
    col("location_id").cast("int").alias("source_location_id"),
    col("city"),
    col("state"),
    col("country"),
    col("region"),
    col("postal_code"),
)

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

print(f"Bronze locations loaded: {bronze_locations.count()} rows")
print("Sample surrogate keys:")
dim_location.select("location_key", "source_location_id", "city", "state", "region").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Add the Unknown Member Row
# MAGIC
# MAGIC Same pattern as `dim_product`: we insert a sentinel "Unknown" row with `location_key = 'unknown'`. When a fact row references a `location_id` that doesn't exist in this dimension table, the lookup JOIN returns `'unknown'` instead of NULL or a dropped row.
# MAGIC
# MAGIC Notice `postal_code = '00000'` — a placeholder that is clearly not a real postal code. Good unknown member rows use values that are obviously synthetic, so analysts don't mistake them for real data.

# COMMAND ----------

unknown_schema = StructType([
    StructField("location_key", StringType(), True),
    StructField("source_location_id", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("region", StringType(), True),
    StructField("postal_code", StringType(), True),
])

unknown = spark.createDataFrame([{
    "location_key": "unknown",
    "source_location_id": -1,
    "city": "Unknown",
    "state": "Unknown",
    "country": "Unknown",
    "region": "Unknown",
    "postal_code": "00000",
}], schema=unknown_schema)

dim_location = unknown.unionByName(dim_location)

print("Unknown member added.")
dim_location.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Write to Gold
# MAGIC
# MAGIC Full overwrite — we rebuild the entire dimension on every run. Safe because:
# MAGIC 1. The dimension is small (hundreds or low thousands of rows)
# MAGIC 2. MD5 keys are deterministic — same location always gets the same key, so fact table joins remain valid even after a rebuild

# COMMAND ----------

dim_location.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.gold.dim_location")

total_count = dim_location.count()
print(f"dim_location written to {catalog}.gold.dim_location")
print(f"Total rows: {total_count} (includes 1 unknown member)")
print(f"Real locations: {total_count - 1}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Preview Grouped by Region
# MAGIC
# MAGIC Let's verify the data looks correct by viewing all locations grouped by region. This also gives us a quick sanity check on data quality — are all regions populated? Are there any unexpected values?

# COMMAND ----------

spark.sql(f"""
    SELECT
        region,
        COUNT(*) AS location_count,
        COLLECT_LIST(city) AS cities
    FROM {catalog}.gold.dim_location
    WHERE location_key != 'unknown'
    GROUP BY region
    ORDER BY region
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Full Dimension Preview
# MAGIC
# MAGIC A flat view of all rows, ordered by region and state. This is what downstream BI tools and fact table joins will see.

# COMMAND ----------

spark.sql(f"""
    SELECT
        location_key,
        source_location_id,
        city,
        state,
        country,
        region,
        postal_code
    FROM {catalog}.gold.dim_location
    ORDER BY region, state, city
""").show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Concept | Applied |
# MAGIC |---------|---------|
# MAGIC | Surrogate key | `md5(source_location_id)` — single-column hash is sufficient here |
# MAGIC | Natural key preserved | `source_location_id` retained for traceability and debugging |
# MAGIC | Unknown member | `location_key = 'unknown'` handles unresolvable FK references |
# MAGIC | SCD complexity | None needed — locations don't change in analytically meaningful ways |
# MAGIC | Write mode | Full overwrite — dimension is small and keys are deterministic |
# MAGIC
# MAGIC **Pattern reinforcement:** `dim_product` and `dim_location` use the identical 3-step pattern:
# MAGIC 1. Load bronze → cast types
# MAGIC 2. Generate MD5 surrogate key
# MAGIC 3. Add unknown member → union → write to Gold
# MAGIC
# MAGIC This pattern will feel automatic by the time you build your 10th dimension.
# MAGIC
# MAGIC **Next notebook:** `11_dim_customer_scd1.py` — Slowly Changing Dimensions Type 1 (Overwrite). This is where dimension loading gets interesting: what happens when a customer changes their email address?
