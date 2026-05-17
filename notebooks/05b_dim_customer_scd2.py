# Databricks notebook source
# MAGIC %md
# MAGIC # 05b — dim_customer: SCD Type 2 (Full History)
# MAGIC
# MAGIC **SCD Type 2 = full history.** Every change creates a new row. The old row is
# MAGIC closed with an `end_date` and `is_current = false`. The new row is open with
# MAGIC `end_date = 9999-12-31` and `is_current = true`.
# MAGIC
# MAGIC Use when you need to answer: *"What city was this customer in when they placed this order?"*
# MAGIC
# MAGIC **Tracked attributes (changes trigger a new version):**
# MAGIC - address, city, state, country, channel
# MAGIC
# MAGIC **Non-tracked attributes (Type 1 within Type 2 — overwrite in place):**
# MAGIC - email, phone (corrections, not meaningful history)
# MAGIC
# MAGIC **Implementation:** Two-step MERGE pattern.
# MAGIC - Step 1: Expire rows where a tracked attribute changed
# MAGIC - Step 2: Insert the new current version

# COMMAND ----------

from pyspark.sql.functions import md5, col, current_date, to_date, lit
from pyspark.sql import Row

catalog = "workspace"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the SCD Type 2 table

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_customer (
        customer_key        STRING   NOT NULL,
        source_customer_id  INT      NOT NULL,
        first_name          STRING,
        last_name           STRING,
        email               STRING,
        phone               STRING,
        address             STRING,
        city                STRING,
        state               STRING,
        country             STRING,
        channel             STRING,
        start_date          DATE,
        end_date            DATE,
        is_current          BOOLEAN
    )
    USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insert the unknown member row
# MAGIC
# MAGIC This row absorbs any fact that arrives with a customer_id that doesn't exist
# MAGIC in the dimension yet (late-arriving dimension scenario).
# MAGIC It is always `is_current = true` and never expires.

# COMMAND ----------

spark.sql(f"""
    MERGE INTO {catalog}.gold.dim_customer AS target
    USING (SELECT 'unknown' AS customer_key) AS src
    ON target.customer_key = src.customer_key
    WHEN NOT MATCHED THEN INSERT (
        customer_key, source_customer_id, first_name, last_name,
        email, phone, address, city, state, country, channel,
        start_date, end_date, is_current
    ) VALUES (
        'unknown', -1, 'Unknown', 'Unknown',
        'unknown@unknown.com', '000-000-0000',
        'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown',
        DATE '1900-01-01', DATE '9999-12-31', true
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and transform Bronze → incoming view

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

incoming.createOrReplaceTempView("incoming_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Expire rows where a tracked attribute changed
# MAGIC
# MAGIC Close the old version: set `end_date = today`, `is_current = false`.
# MAGIC The WHERE clause checks only the tracked attributes (address, city, state, country, channel).

# COMMAND ----------

spark.sql(f"""
    MERGE INTO {catalog}.gold.dim_customer AS target
    USING incoming_customers AS source
    ON  target.source_customer_id = source.source_customer_id
    AND target.is_current = true
    WHEN MATCHED AND (
        target.address != source.address OR
        target.city    != source.city    OR
        target.state   != source.state   OR
        target.country != source.country OR
        target.channel != source.channel
    )
    THEN UPDATE SET
        target.end_date   = current_date(),
        target.is_current = false
""")

print("Step 1 complete — expired changed rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Insert new current version
# MAGIC
# MAGIC For every incoming row that is either:
# MAGIC - Brand new (no existing dim record), OR
# MAGIC - Changed (old record was just expired in Step 1)
# MAGIC
# MAGIC Insert a fresh row with `start_date = today`, `end_date = 9999-12-31`, `is_current = true`.

# COMMAND ----------

spark.sql(f"""
    MERGE INTO {catalog}.gold.dim_customer AS target
    USING (
        SELECT
            src.customer_key,
            src.source_customer_id,
            src.first_name,
            src.last_name,
            src.email,
            src.phone,
            src.address,
            src.city,
            src.state,
            src.country,
            src.channel,
            current_date()        AS start_date,
            DATE '9999-12-31'     AS end_date,
            true                  AS is_current
        FROM incoming_customers src
        LEFT JOIN {catalog}.gold.dim_customer tgt
            ON  src.source_customer_id = tgt.source_customer_id
            AND tgt.is_current = true
        WHERE
            tgt.source_customer_id IS NULL
            OR src.address != tgt.address
            OR src.city    != tgt.city
            OR src.state   != tgt.state
            OR src.country != tgt.country
            OR src.channel != tgt.channel
    ) AS new_rows
    ON  target.source_customer_id = new_rows.source_customer_id
    AND target.start_date         = new_rows.start_date
    WHEN NOT MATCHED THEN INSERT *
""")

count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {catalog}.gold.dim_customer").collect()[0]["cnt"]
print(f"Step 2 complete — dim_customer total rows: {count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview
# MAGIC
# MAGIC **Demo moment:**
# MAGIC Run once → all customers have 1 row each with `is_current = true`.
# MAGIC Then change `city` for a few customers in Bronze (simulate a move),
# MAGIC re-run this notebook, and query below — you'll see two rows per changed customer:
# MAGIC the old row with `is_current = false` and the new row with `is_current = true`.

# COMMAND ----------

spark.sql(f"""
    SELECT
        source_customer_id,
        first_name,
        city,
        channel,
        start_date,
        end_date,
        is_current
    FROM {catalog}.gold.dim_customer
    WHERE source_customer_id != -1
    ORDER BY source_customer_id, start_date
    LIMIT 20
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to join to dim_customer from a fact table
# MAGIC
# MAGIC Always filter on `is_current = true` to get the latest version of each customer.
# MAGIC
# MAGIC ```sql
# MAGIC JOIN workspace.gold.dim_customer dc
# MAGIC   ON fact.customer_key = dc.customer_key
# MAGIC  AND dc.is_current = true
# MAGIC ```
# MAGIC
# MAGIC To get the customer's state **at the time of the order**, join on the date range instead:
# MAGIC
# MAGIC ```sql
# MAGIC JOIN workspace.gold.dim_customer dc
# MAGIC   ON fact.customer_key = dc.customer_key
# MAGIC  AND fact_order_date BETWEEN dc.start_date AND dc.end_date
# MAGIC ```
