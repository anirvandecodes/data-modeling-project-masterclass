# Databricks notebook source

# MAGIC %md
# MAGIC # Slowly Changing Dimensions — Type 2 (Full History)
# MAGIC
# MAGIC **Notebook 12 of the Data Modeling Masterclass**
# MAGIC
# MAGIC SCD Type 1 (notebook 11) discards history. SCD Type 2 preserves it — completely. This is the most powerful and most commonly used SCD type in enterprise data warehouses. It's also the most complex to implement correctly.

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2: Full History
# MAGIC
# MAGIC **Rule:** When a tracked attribute changes, close the old record and insert a new one. Every version of a customer's attributes is preserved as a separate row.
# MAGIC
# MAGIC ```
# MAGIC BEFORE customer moves from Austin to Denver:
# MAGIC customer_key | cust_id | city    | is_current | start_date  | end_date
# MAGIC abc123       | 42      | Austin  | true       | 2022-01-15  | 9999-12-31
# MAGIC
# MAGIC AFTER customer moves (SCD2 update):
# MAGIC customer_key | cust_id | city    | is_current | start_date  | end_date
# MAGIC abc123       | 42      | Austin  | false      | 2022-01-15  | 2024-03-10  ← closed
# MAGIC def456       | 42      | Denver  | true       | 2024-03-10  | 9999-12-31  ← new current row
# MAGIC ```
# MAGIC
# MAGIC Now you can answer: "Where was customer 42 when they placed an order in February 2023?" → Austin. "Where are they now?" → Denver.
# MAGIC
# MAGIC **The three new columns Type 2 adds:**
# MAGIC - `start_date` — the date this version became active
# MAGIC - `end_date` — the date this version was superseded (`9999-12-31` means still active)
# MAGIC - `is_current` — boolean flag, `true` for the most recent version (makes queries simpler than filtering by date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## When to Use SCD Type 2
# MAGIC
# MAGIC Use Type 2 when the historical value of an attribute matters for analysis:
# MAGIC
# MAGIC | Attribute | Use Type 2? | Reason |
# MAGIC |-----------|------------|--------|
# MAGIC | customer `city` | YES | "Where was the customer when they placed this order?" |
# MAGIC | customer `channel` | YES | "Was this order placed as a Free or Premium customer?" |
# MAGIC | customer `address` | YES | Shipping geography analysis |
# MAGIC | customer `email` | NO | Error corrections only — no one asks "what was the old email?" |
# MAGIC | customer `phone` | NO | Same as email |
# MAGIC
# MAGIC **Tracked attributes (Type 2 — changes create new rows):** `address`, `city`, `state`, `country`, `channel`
# MAGIC
# MAGIC **Non-tracked attributes (Type 1 — overwrite in the existing row):** `email`, `phone`
# MAGIC
# MAGIC This mixed approach is common: apply Type 1 logic to some columns and Type 2 to others, all within the same MERGE operation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Two-Step MERGE Pattern
# MAGIC
# MAGIC Implementing SCD Type 2 requires two MERGE operations in sequence:
# MAGIC
# MAGIC **Step 1 — Expire changed records:**
# MAGIC Find rows where a tracked attribute has changed. Set `end_date = today`, `is_current = false`. The old version is now "closed."
# MAGIC
# MAGIC **Step 2 — Insert new versions + new customers:**
# MAGIC - For customers whose tracked attributes changed: insert a new row with new values, `start_date = today`, `is_current = true`
# MAGIC - For brand new customers: insert their first row the same way
# MAGIC
# MAGIC This two-step approach is necessary because Delta Lake MERGE can't both UPDATE and INSERT for the same source row in a single pass.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create the Target Table
# MAGIC
# MAGIC The schema includes the three SCD Type 2 metadata columns: `start_date`, `end_date`, `is_current`. Everything else is the same as the Type 1 table.

# COMMAND ----------

catalog = "workspace"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_customer (
        customer_key        STRING  NOT NULL,
        source_customer_id  INT     NOT NULL,
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
    ) USING DELTA
""")

print(f"Table {catalog}.gold.dim_customer is ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Insert the Unknown Member Row
# MAGIC
# MAGIC The unknown member row is inserted once and never updated. We use `INSERT INTO ... WHERE NOT EXISTS` to make this idempotent — it only inserts if the row doesn't already exist.
# MAGIC
# MAGIC Note the unknown member's SCD2 metadata: `start_date = 1900-01-01` (the beginning of time), `end_date = 9999-12-31` (never expires), `is_current = true`. It is always current, because "unknown" is always a valid state.

# COMMAND ----------

spark.sql(f"""
    INSERT INTO {catalog}.gold.dim_customer
    SELECT
        'unknown'    AS customer_key,
        -1           AS source_customer_id,
        'Unknown'    AS first_name,
        'Unknown'    AS last_name,
        'unknown@unknown.com' AS email,
        '000-000-0000' AS phone,
        'Unknown'    AS address,
        'Unknown'    AS city,
        'Unknown'    AS state,
        'Unknown'    AS country,
        'Unknown'    AS channel,
        DATE('1900-01-01') AS start_date,
        DATE('9999-12-31') AS end_date,
        true         AS is_current
    WHERE NOT EXISTS (
        SELECT 1 FROM {catalog}.gold.dim_customer WHERE customer_key = 'unknown'
    )
""")

print("Unknown member row inserted (or already existed).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Prepare Incoming Data
# MAGIC
# MAGIC We load from bronze and generate the surrogate key. The key is based only on the natural key (`customer_id`) — NOT on any mutable attributes.
# MAGIC
# MAGIC We also register a temp view for use in the MERGE SQL.

# COMMAND ----------

from pyspark.sql.functions import md5, col, current_date, to_date, lit

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

print(f"Incoming customers: {incoming.count()} rows")
incoming.select("customer_key", "source_customer_id", "first_name", "city", "channel").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: MERGE Step 1 — Expire Changed Records
# MAGIC
# MAGIC This MERGE finds existing `is_current = true` rows where any **tracked** attribute has changed, and closes them by:
# MAGIC - Setting `end_date = today`
# MAGIC - Setting `is_current = false`
# MAGIC
# MAGIC The condition checks all tracked attributes: `address`, `city`, `state`, `country`, `channel`.
# MAGIC
# MAGIC Notice: `email` and `phone` are NOT in the change-detection condition. If only the email changes, we will NOT create a new row — we'll just overwrite the email in place (that's the Type 1 behavior we want for non-tracked attributes).
# MAGIC
# MAGIC Also notice: `WHEN NOT MATCHED` is not included here. We only expire existing rows in this step.

# COMMAND ----------

spark.sql(f"""
    MERGE INTO {catalog}.gold.dim_customer AS target
    USING incoming_customers AS source
    ON target.source_customer_id = source.source_customer_id
       AND target.is_current = true
    WHEN MATCHED AND (
        target.address != source.address OR
        target.city    != source.city    OR
        target.state   != source.state   OR
        target.country != source.country OR
        target.channel != source.channel
    ) THEN UPDATE SET
        target.email      = source.email,
        target.phone      = source.phone,
        target.end_date   = current_date(),
        target.is_current = false
""")

print("Step 1 complete: expired records where tracked attributes changed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: MERGE Step 2 — Insert New Current Versions
# MAGIC
# MAGIC Now we insert new rows for:
# MAGIC 1. **Changed customers** — customers whose old row was just expired in Step 1. They match `source_customer_id` in the target, but have no `is_current = true` row anymore.
# MAGIC 2. **New customers** — customers who have never appeared in the dimension before.
# MAGIC
# MAGIC Both cases are handled by `WHEN NOT MATCHED` — because after Step 1, changed customers no longer have a matching `is_current = true` row.
# MAGIC
# MAGIC The new row gets:
# MAGIC - A fresh `start_date = today`
# MAGIC - `end_date = 9999-12-31` (far future = still active)
# MAGIC - `is_current = true`

# COMMAND ----------

spark.sql(f"""
    MERGE INTO {catalog}.gold.dim_customer AS target
    USING incoming_customers AS source
    ON target.source_customer_id = source.source_customer_id
       AND target.is_current = true
    WHEN NOT MATCHED THEN INSERT (
        customer_key, source_customer_id, first_name, last_name,
        email, phone, address, city, state, country, channel,
        start_date, end_date, is_current
    ) VALUES (
        source.customer_key,
        source.source_customer_id,
        source.first_name,
        source.last_name,
        source.email,
        source.phone,
        source.address,
        source.city,
        source.state,
        source.country,
        source.channel,
        current_date(),
        DATE('9999-12-31'),
        true
    )
""")

print("Step 2 complete: inserted new current versions for changed and new customers.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify the Results

# COMMAND ----------

total = spark.sql(f"SELECT COUNT(*) AS cnt FROM {catalog}.gold.dim_customer").collect()[0]["cnt"]
current_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {catalog}.gold.dim_customer WHERE is_current = true").collect()[0]["cnt"]
historical = total - current_count

print(f"Total rows in dim_customer: {total}")
print(f"  Current rows (is_current=true): {current_count}")
print(f"  Historical rows (is_current=false): {historical}")

# COMMAND ----------

spark.sql(f"""
    SELECT
        customer_key,
        source_customer_id,
        first_name,
        last_name,
        email,
        city,
        channel,
        start_date,
        end_date,
        is_current
    FROM {catalog}.gold.dim_customer
    WHERE is_current = true
    AND source_customer_id != -1
    ORDER BY source_customer_id
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo Moment: See Type 2 in Action
# MAGIC
# MAGIC > **Try this to see SCD Type 2 preserve history:**
# MAGIC >
# MAGIC > 1. Run this notebook. Note customer `source_customer_id = 3`'s current city.
# MAGIC > 2. Update 3 customers' cities in the bronze table:
# MAGIC >    ```sql
# MAGIC >    UPDATE workspace.bronze.customers
# MAGIC >    SET city = 'San Francisco', state = 'CA'
# MAGIC >    WHERE customer_id IN (3, 7, 12)
# MAGIC >    ```
# MAGIC > 3. Re-run this notebook from Step 3 onward.
# MAGIC > 4. Query the dimension:
# MAGIC >    ```sql
# MAGIC >    SELECT customer_key, city, is_current, start_date, end_date
# MAGIC >    FROM workspace.gold.dim_customer
# MAGIC >    WHERE source_customer_id = 3
# MAGIC >    ORDER BY start_date
# MAGIC >    ```
# MAGIC > 5. You'll see **two rows** for customer 3:
# MAGIC >    - Row 1: old city, `is_current = false`, `end_date = today`
# MAGIC >    - Row 2: San Francisco, `is_current = true`, `end_date = 9999-12-31`
# MAGIC >
# MAGIC > The old city is preserved. History is maintained. That's SCD Type 2.

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to Join from Fact Tables
# MAGIC
# MAGIC When querying the fact table and you want **current** customer attributes:
# MAGIC ```sql
# MAGIC SELECT fo.order_date_key, dc.city, SUM(fo.total_amount) AS revenue
# MAGIC FROM workspace.gold.fact_orders fo
# MAGIC JOIN workspace.gold.dim_customer dc
# MAGIC     ON fo.customer_key = dc.customer_key
# MAGIC     AND dc.is_current = true    -- only the current version
# MAGIC GROUP BY fo.order_date_key, dc.city
# MAGIC ```
# MAGIC
# MAGIC When you want **the customer's attributes at the time of the order** (point-in-time correct):
# MAGIC ```sql
# MAGIC -- The fact_orders.customer_key was set at load time to the THEN-current customer_key.
# MAGIC -- So joining directly on customer_key (without is_current filter) gives you the
# MAGIC -- exact version of the customer that was active when the order was placed.
# MAGIC SELECT fo.order_date_key, dc.city, SUM(fo.total_amount) AS revenue
# MAGIC FROM workspace.gold.fact_orders fo
# MAGIC JOIN workspace.gold.dim_customer dc
# MAGIC     ON fo.customer_key = dc.customer_key   -- point-in-time correct, no is_current needed
# MAGIC GROUP BY fo.order_date_key, dc.city
# MAGIC ```
# MAGIC
# MAGIC This is the power of Type 2: by storing the surrogate key at the time of the fact, the fact table automatically "remembers" which version of the customer was active then.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Concept | Implementation |
# MAGIC |---------|---------------|
# MAGIC | SCD Type 2 | New row per change; old row closed with `end_date` |
# MAGIC | Three metadata columns | `start_date`, `end_date`, `is_current` |
# MAGIC | Two-step MERGE | Step 1: expire changed rows. Step 2: insert new current rows |
# MAGIC | Tracked attributes | `address`, `city`, `state`, `country`, `channel` |
# MAGIC | Non-tracked (Type 1) | `email`, `phone` — overwrite in place |
# MAGIC | Unknown member | `is_current = true` always, `end_date = 9999-12-31` |
# MAGIC | Point-in-time queries | Join on `customer_key` directly — key encodes the version |
# MAGIC
# MAGIC **Next notebook:** `13_fact_design_and_types.py` — Fact table design fundamentals. Grain declaration, types of measures, and all four fact table types.
