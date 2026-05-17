# Databricks notebook source

# MAGIC %md
# MAGIC # Slowly Changing Dimensions — Type 1 (Overwrite)
# MAGIC
# MAGIC **Notebook 11 of the Data Modeling Masterclass**
# MAGIC
# MAGIC In notebooks 09 and 10 we built static dimensions — tables that get rebuilt wholesale on every load. Now we tackle something more realistic: **dimensions that change over time.** This is the subject of Slowly Changing Dimensions (SCDs).

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is a Slowly Changing Dimension?
# MAGIC
# MAGIC A **Slowly Changing Dimension (SCD)** is any dimension where attributes can change over time, and you need a deliberate strategy for handling those changes.
# MAGIC
# MAGIC Consider customers. When a customer record first arrives, you insert it into `dim_customer`. Simple. But what happens next week when:
# MAGIC - The customer updates their email address?
# MAGIC - They move to a different city?
# MAGIC - They upgrade from a Free to a Premium channel?
# MAGIC
# MAGIC You have a choice to make: **do you overwrite the old value, or keep both the old and the new?**
# MAGIC
# MAGIC The answer depends on the business question you need to answer:
# MAGIC - "What is the customer's current email?" → Overwrite is fine.
# MAGIC - "What city was the customer in when they placed this order?" → You need history.
# MAGIC
# MAGIC Ralph Kimball formalized this into **SCD Types 0–7**. The three you'll use in 95% of real projects are:
# MAGIC - **Type 1:** Overwrite — simple, no history
# MAGIC - **Type 2:** Full history — new row per change (covered in notebook 12)
# MAGIC - **Type 3:** Previous value column — limited history (rarely used)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 1: Overwrite
# MAGIC
# MAGIC **Rule:** When an attribute changes, update the existing row. The old value is gone forever.
# MAGIC
# MAGIC ```
# MAGIC BEFORE update:
# MAGIC customer_key | customer_id | email               | city
# MAGIC abc123       | 42          | alice@old.com       | Austin
# MAGIC
# MAGIC AFTER customer changes email:
# MAGIC customer_key | customer_id | email               | city
# MAGIC abc123       | 42          | alice@new.com       | Austin   ← updated, old value lost
# MAGIC ```
# MAGIC
# MAGIC **When to use Type 1:**
# MAGIC - Correcting errors (typo in name, wrong phone number)
# MAGIC - Attributes where historical values have no analytical meaning (email address — no one asks "what was the email 3 years ago?")
# MAGIC - Non-tracked attributes that are just reference data
# MAGIC
# MAGIC **When NOT to use Type 1:**
# MAGIC - Customer location (you might need to know where they lived when they placed each order)
# MAGIC - Customer segment / tier (upgrading from Free to Premium — was this order placed as a Free user?)
# MAGIC - Pricing tiers, account status, contract level
# MAGIC
# MAGIC **Implementation:** A single MERGE (upsert) statement. If the row exists, UPDATE it. If it's new, INSERT it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create the Target Table
# MAGIC
# MAGIC We use `CREATE TABLE IF NOT EXISTS` so this step is idempotent — safe to run multiple times. On the first run it creates the table. On subsequent runs it's a no-op, preserving existing data.
# MAGIC
# MAGIC Note: We don't add `start_date`, `end_date`, or `is_current` columns here — those belong to SCD Type 2. Type 1 is just a regular table with no temporal metadata.

# COMMAND ----------

catalog = "workspace"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_customer_scd1 (
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
        channel             STRING
    ) USING DELTA
""")

print(f"Table {catalog}.gold.dim_customer_scd1 is ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Prepare the Incoming Data
# MAGIC
# MAGIC We load from `workspace.bronze.customers` and apply the MD5 surrogate key. Then we register this as a temporary view so we can reference it in the MERGE SQL statement.
# MAGIC
# MAGIC **Important:** The surrogate key is `md5(customer_id)` — we only hash the natural business key, not any mutable attributes. This ensures the key stays stable across updates. If we included `email` in the hash and the email changed, the key would change — breaking all historical fact table joins.

# COMMAND ----------

from pyspark.sql.functions import md5, col

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

print(f"Incoming customers prepared: {incoming.count()} rows")
incoming.select("customer_key", "source_customer_id", "first_name", "email", "city").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: MERGE (Upsert)
# MAGIC
# MAGIC The MERGE statement is the workhorse of SCD Type 1:
# MAGIC
# MAGIC - `WHEN MATCHED THEN UPDATE SET ...` — customer already exists, overwrite ALL attribute columns with the new values
# MAGIC - `WHEN NOT MATCHED THEN INSERT *` — new customer, insert the full row
# MAGIC
# MAGIC We match on `source_customer_id` (the natural key), not on `customer_key`. Why? Because if the key derivation logic ever changes, matching on the surrogate key would cause duplicates. Natural key matching is more robust.
# MAGIC
# MAGIC **Type 1 characteristic:** The UPDATE clause sets every attribute column to the new value. The old values are permanently replaced. There is no condition — any change to any field triggers an overwrite.

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
print(f"dim_customer_scd1 loaded successfully: {count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify the Results

# COMMAND ----------

spark.sql(f"""
    SELECT
        customer_key,
        source_customer_id,
        first_name,
        last_name,
        email,
        city,
        channel
    FROM {catalog}.gold.dim_customer_scd1
    ORDER BY source_customer_id
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Channel Distribution Check
# MAGIC
# MAGIC A quick aggregate to verify the data loaded correctly and see channel distribution:

# COMMAND ----------

spark.sql(f"""
    SELECT
        channel,
        COUNT(*) AS customer_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
    FROM {catalog}.gold.dim_customer_scd1
    GROUP BY channel
    ORDER BY customer_count DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo Moment: Run It Twice
# MAGIC
# MAGIC > **Try this to see Type 1 in action:**
# MAGIC >
# MAGIC > 1. Run this notebook end to end. Note a customer's email — for example, customer `source_customer_id = 5`.
# MAGIC > 2. Manually update that customer's email in the bronze table:
# MAGIC >    ```sql
# MAGIC >    UPDATE workspace.bronze.customers SET email = 'changed@example.com' WHERE customer_id = 5
# MAGIC >    ```
# MAGIC > 3. Run this notebook again.
# MAGIC > 4. Query `dim_customer_scd1` for customer 5. The email is now `changed@example.com`. The old email is **gone forever.**
# MAGIC >
# MAGIC > That's SCD Type 1. Simple, destructive, no history.
# MAGIC
# MAGIC If you need to preserve the old email — so you can ask "what was the customer's email when they placed their order in 2023?" — you need **SCD Type 2**. That's the next notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Concept | Implementation |
# MAGIC |---------|---------------|
# MAGIC | SCD Type 1 | MERGE: UPDATE on match, INSERT if new |
# MAGIC | Surrogate key | `md5(customer_id)` — hash only the stable natural key |
# MAGIC | Match condition | `source_customer_id` (natural key), not the surrogate |
# MAGIC | History | None — old values are permanently overwritten |
# MAGIC | Best for | Error corrections, non-analytical attributes |
# MAGIC | Idempotency | MERGE is safe to run multiple times |
# MAGIC
# MAGIC **Next notebook:** `12_dim_customer_scd2.py` — SCD Type 2 (Full History). Every change creates a new row. The fact table can ask "who was the customer at the time of each order" — and get the right answer.
