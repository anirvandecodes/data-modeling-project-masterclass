# Databricks notebook source
# MAGIC %md
# MAGIC # 05a — dim_customer: SCD Type 1 (Overwrite)
# MAGIC
# MAGIC **SCD Type 1 = overwrite.** When a value changes, the old value is gone forever.
# MAGIC No history is preserved. Use when:
# MAGIC - Correcting a data entry error (wrong email, typo in a name)
# MAGIC - The old value has no analytical meaning
# MAGIC - Simplicity matters more than historical accuracy
# MAGIC
# MAGIC **Implementation:** A single MERGE statement handles both inserts and updates.

# COMMAND ----------

from pyspark.sql.functions import md5, col

catalog = "workspace"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the SCD Type 1 table

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_customer_scd1 (
        customer_key        STRING       NOT NULL,
        source_customer_id  INT          NOT NULL,
        first_name          STRING,
        last_name           STRING,
        email               STRING,
        phone               STRING,
        address             STRING,
        city                STRING,
        state               STRING,
        country             STRING,
        channel             STRING
    )
    USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and transform Bronze → Silver

# COMMAND ----------

incoming = spark.table(f"{catalog}.bronze.customers").select(
    md5(col("customer_id").cast("string")).alias("customer_key"),
    col("customer_id").cast("int").alias("source_customer_id"),
    col("first_name"),
    col("last_name"),
    col("email"),
    col("phone"),
    col("address"),
    col("city"),
    col("state"),
    col("country"),
    col("channel"),
)

incoming.createOrReplaceTempView("incoming_customers_scd1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE — overwrite any changed attributes
# MAGIC
# MAGIC This is the entirety of SCD Type 1: if the row exists → update all fields in-place.
# MAGIC If it's new → insert. No history columns needed.

# COMMAND ----------

spark.sql(f"""
    MERGE INTO {catalog}.gold.dim_customer_scd1 AS target
    USING incoming_customers_scd1 AS source
    ON target.source_customer_id = source.source_customer_id
    WHEN MATCHED THEN UPDATE SET
        target.customer_key        = source.customer_key,
        target.first_name          = source.first_name,
        target.last_name           = source.last_name,
        target.email               = source.email,
        target.phone               = source.phone,
        target.address             = source.address,
        target.city                = source.city,
        target.state               = source.state,
        target.country             = source.country,
        target.channel             = source.channel
    WHEN NOT MATCHED THEN INSERT *
""")

count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {catalog}.gold.dim_customer_scd1").collect()[0]["cnt"]
print(f"dim_customer_scd1 loaded: {count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview
# MAGIC
# MAGIC **Demo moment:** Run this notebook once. Then change an email address in the source data,
# MAGIC re-run, and verify the old email value is gone — replaced by the new one.
# MAGIC That's SCD Type 1 in action.

# COMMAND ----------

spark.sql(f"""
    SELECT customer_key, source_customer_id, first_name, last_name,
           email, city, channel
    FROM {catalog}.gold.dim_customer_scd1
    ORDER BY source_customer_id
    LIMIT 10
""").show(truncate=False)
