# Databricks notebook source

# MAGIC %md
# MAGIC # Fact Table Design — Grain, Measures & All Four Types
# MAGIC
# MAGIC **Notebook 13 of the Data Modeling Masterclass**
# MAGIC
# MAGIC We've built all four dimensions. Now it's time to design and build the fact tables — the heart of the star schema. Before writing a single line of code, we need to make the most important design decision in dimensional modeling: **the grain**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Most Important Decision: Declaring the Grain
# MAGIC
# MAGIC **The grain is the answer to: "One row in this fact table represents one _what_?"**
# MAGIC
# MAGIC This sounds simple. It is deceptively hard to get right. And it's the decision that determines everything else about the table: which dimensions attach to it, which measures it contains, and whether future business questions can be answered.
# MAGIC
# MAGIC ### Getting the Grain Wrong
# MAGIC
# MAGIC **Too coarse (one row per order):**
# MAGIC - You aggregate quantity and amount at the order level when loading
# MAGIC - A week later, someone asks "what was the most purchased product in each order?"
# MAGIC - Answer: impossible. The line-item detail is gone.
# MAGIC
# MAGIC **Too fine (one row per order × product × promotion × coupon):**
# MAGIC - Most rows are sparse — most orders don't have promotions or coupons
# MAGIC - You end up with many NULLs and a confusing join structure
# MAGIC
# MAGIC ### Our Grain for fact_orders
# MAGIC
# MAGIC > **One row = one order line item**
# MAGIC
# MAGIC This is the most atomic unit of business activity — one customer buying one product on one order. You can always roll up to order level, to day level, to customer level. You can never disaggregate further. **Always choose the most atomic grain you have.**
# MAGIC
# MAGIC Write it down before coding:
# MAGIC ```
# MAGIC GRAIN: fact_orders
# MAGIC One row per order line item.
# MAGIC Identified by: order_id + line_item_id
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## What Belongs in a Fact Table
# MAGIC
# MAGIC Once you've declared the grain, three types of columns belong in the fact table:
# MAGIC
# MAGIC ### 1. Foreign Keys (one per dimension)
# MAGIC Surrogate keys pointing to the dimension tables:
# MAGIC - `customer_key` → `dim_customer`
# MAGIC - `product_key` → `dim_product`
# MAGIC - `location_key` → `dim_location`
# MAGIC - `order_date_key` → `dim_date`
# MAGIC - `ship_date_key` → `dim_date` (role-playing — same table, two aliases)
# MAGIC
# MAGIC ### 2. Degenerate Dimensions
# MAGIC Identifiers that have no dimension table because they carry no additional attributes — they're just identifiers. `order_number` is the classic example. No one needs to look up anything else about an order number; it's just a label.
# MAGIC
# MAGIC ### 3. Measures
# MAGIC The numeric facts you aggregate: `quantity`, `unit_price`, `discount_amount`, `total_amount`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Three Types of Measures
# MAGIC
# MAGIC Not all numbers behave the same way in aggregations. Understanding this prevents incorrect analytics.
# MAGIC
# MAGIC ### Additive Measures
# MAGIC Can be SUMmed across **every dimension** — time, geography, product, customer. These are the workhorses of analytics.
# MAGIC - `total_amount` — sum across any combination of dimensions = always valid
# MAGIC - `quantity` — sum across any combination = always valid
# MAGIC - `discount_amount` — sum = total discounts given
# MAGIC
# MAGIC ### Semi-Additive Measures
# MAGIC Can be SUMmed across some dimensions but **not across time**. Most commonly found in periodic snapshot fact tables.
# MAGIC - `account_balance` — sum of all account balances on January 31st = total assets (valid). But sum of one account's balance across 12 months = meaningless number.
# MAGIC - `inventory_quantity` — same pattern
# MAGIC
# MAGIC Use AVG or LAST() across time, SUM across other dimensions.
# MAGIC
# MAGIC ### Non-Additive Measures
# MAGIC **Cannot be SUMmed across any dimension**. Aggregating them produces a meaningless result.
# MAGIC - `unit_price` — the price of a single unit. Summing prices across orders makes no sense.
# MAGIC - Ratios (return rate, margin %) — always compute from additive components: `SUM(return_amount) / SUM(total_amount)`
# MAGIC - Percentages, averages stored as pre-computed values

# COMMAND ----------

# MAGIC %md
# MAGIC ### Our Measures in fact_orders

# COMMAND ----------

catalog = "workspace"

measures_data = [
    ("quantity",        "INT",    "Additive",        "SUM(quantity) — units sold across any cut"),
    ("unit_price",      "DOUBLE", "Non-additive",    "AVG(unit_price) — don't SUM prices"),
    ("discount_amount", "DOUBLE", "Additive",        "SUM(discount_amount) — total discounts"),
    ("total_amount",    "DOUBLE", "Additive",        "SUM(total_amount) — primary revenue metric"),
]

df_measures = spark.createDataFrame(measures_data, ["measure", "type", "additivity", "aggregation_rule"])
df_measures.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Four Fact Table Types
# MAGIC
# MAGIC Kimball identified four fundamental fact table types. Each solves a different analytical problem. Our star schema uses all four — we'll build a real example of each.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Type 1: Transactional Fact Table
# MAGIC
# MAGIC **Definition:** One row per discrete event (transaction). Rows are inserted, never updated. Events are atomic and timestamped.
# MAGIC
# MAGIC **Characteristics:**
# MAGIC - Most common fact table type
# MAGIC - Can be very large (millions/billions of rows)
# MAGIC - All measures are typically additive
# MAGIC - Grain = one event
# MAGIC
# MAGIC **Examples:** `fact_orders`, `fact_returns`, `fact_pageviews`, `fact_payments`
# MAGIC
# MAGIC This is what we build in notebooks 14 and 15. The schema looks like this:

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.fact_orders (
        order_line_key  STRING  NOT NULL,
        customer_key    STRING,
        product_key     STRING,
        location_key    STRING,
        order_date_key  INT,
        ship_date_key   INT,
        order_number    STRING,
        quantity        INT,
        unit_price      DOUBLE,
        discount_amount DOUBLE,
        total_amount    DOUBLE
    ) USING DELTA
""")

print(f"fact_orders table created (Transactional Fact — Type 1)")
print("Schema:")
spark.sql(f"DESCRIBE {catalog}.gold.fact_orders").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Type 2: Periodic Snapshot Fact Table
# MAGIC
# MAGIC **Definition:** One row per entity per time period. Records the **state** of something at regular intervals, regardless of whether any transaction occurred.
# MAGIC
# MAGIC **Characteristics:**
# MAGIC - Regular cadence: daily, weekly, monthly
# MAGIC - Semi-additive measures (valid to sum across entities, but not across time periods)
# MAGIC - Rows are inserted on schedule, not triggered by events
# MAGIC - "Silent periods" are explicitly recorded (a row with zeros, not a missing row)
# MAGIC
# MAGIC **Examples:** Monthly account balance, daily inventory levels, weekly active users
# MAGIC
# MAGIC **Our example:** Monthly customer spend summary — captures total spend per customer per month, even if the customer had zero orders that month.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.fact_customer_monthly_snapshot (
        snapshot_key        STRING  NOT NULL,
        customer_key        STRING,
        month_date_key      INT,
        order_count         INT,
        total_quantity      INT,
        total_spend         DOUBLE,
        total_discounts     DOUBLE,
        avg_order_value     DOUBLE
    ) USING DELTA
""")

print("fact_customer_monthly_snapshot table created (Periodic Snapshot — Type 2)")

# COMMAND ----------

# MAGIC %md
# MAGIC Let's populate the periodic snapshot from our transactional fact table — this is a common pattern: derive snapshots by aggregating transactions.

# COMMAND ----------

from pyspark.sql.functions import md5, concat_ws, col

spark.sql(f"TRUNCATE TABLE {catalog}.gold.fact_customer_monthly_snapshot")

spark.sql(f"""
    INSERT INTO {catalog}.gold.fact_customer_monthly_snapshot
    SELECT
        md5(concat_ws('|', fo.customer_key, cast(dd.year as string), cast(dd.month_of_year as string))) AS snapshot_key,
        fo.customer_key,
        MIN(fo.order_date_key)                          AS month_date_key,
        COUNT(DISTINCT fo.order_number)                 AS order_count,
        SUM(fo.quantity)                                AS total_quantity,
        ROUND(SUM(fo.total_amount), 2)                  AS total_spend,
        ROUND(SUM(fo.discount_amount), 2)               AS total_discounts,
        ROUND(AVG(fo.total_amount), 2)                  AS avg_order_value
    FROM {catalog}.gold.fact_orders fo
    JOIN {catalog}.gold.dim_date dd ON fo.order_date_key = dd.date_key
    WHERE fo.customer_key != 'unknown'
    GROUP BY fo.customer_key, dd.year, dd.month_of_year
""")

count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {catalog}.gold.fact_customer_monthly_snapshot").collect()[0]["cnt"]
print(f"Periodic snapshot populated: {count} rows (one per customer per active month)")
spark.sql(f"""
    SELECT customer_key, month_date_key, order_count, total_spend, avg_order_value
    FROM {catalog}.gold.fact_customer_monthly_snapshot
    ORDER BY total_spend DESC
    LIMIT 8
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC > **Semi-additive warning:** `total_spend` in this snapshot represents spend IN THAT MONTH.
# MAGIC > - `SUM(total_spend) GROUP BY customer` = lifetime value ✓ (valid — summing across months for one customer)
# MAGIC > - `SUM(total_spend) GROUP BY month` = total revenue that month ✓ (valid — summing across customers for one month)
# MAGIC > - `AVG(total_spend) GROUP BY customer` = average monthly spend ✓ (valid)
# MAGIC > - Storing a running cumulative balance here would be semi-additive — `SUM(balance) across months` is meaningless

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Type 3: Accumulating Snapshot Fact Table
# MAGIC
# MAGIC **Definition:** One row per process or pipeline, updated as the process advances through its stages. Multiple date keys capture different milestones.
# MAGIC
# MAGIC **Characteristics:**
# MAGIC - Rows are **updated**, not just inserted — unlike transactional facts
# MAGIC - Multiple date foreign keys in one row: `order_date_key`, `ship_date_key`, `delivery_date_key`
# MAGIC - Milestone dates are NULL until that stage is reached
# MAGIC - Measures often capture lag (days between stages)
# MAGIC
# MAGIC **Examples:** Order fulfillment pipeline, loan application process, software release lifecycle, hiring pipeline
# MAGIC
# MAGIC **Our example:** Order fulfillment tracker — tracks each order from placement to shipment.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.fact_order_fulfillment (
        order_key           STRING  NOT NULL,
        customer_key        STRING,
        product_key         STRING,
        order_date_key      INT,
        ship_date_key       INT,
        order_number        STRING,
        quantity            INT,
        total_amount        DOUBLE,
        days_to_ship        INT,
        fulfillment_status  STRING
    ) USING DELTA
""")

print("fact_order_fulfillment table created (Accumulating Snapshot — Type 3)")

# COMMAND ----------

spark.sql(f"TRUNCATE TABLE {catalog}.gold.fact_order_fulfillment")

spark.sql(f"""
    INSERT INTO {catalog}.gold.fact_order_fulfillment
    SELECT
        fo.order_line_key                                       AS order_key,
        fo.customer_key,
        fo.product_key,
        fo.order_date_key,
        fo.ship_date_key,
        fo.order_number,
        fo.quantity,
        fo.total_amount,
        CASE
            WHEN fo.ship_date_key IS NOT NULL AND fo.order_date_key IS NOT NULL
            THEN datediff(dd_ship.full_date, dd_order.full_date)
            ELSE NULL
        END AS days_to_ship,
        CASE
            WHEN fo.ship_date_key IS NULL THEN 'Pending Shipment'
            WHEN datediff(dd_ship.full_date, dd_order.full_date) <= 2 THEN 'On Time'
            WHEN datediff(dd_ship.full_date, dd_order.full_date) <= 5 THEN 'Slightly Delayed'
            ELSE 'Delayed'
        END AS fulfillment_status
    FROM {catalog}.gold.fact_orders fo
    LEFT JOIN {catalog}.gold.dim_date dd_order ON fo.order_date_key = dd_order.date_key
    LEFT JOIN {catalog}.gold.dim_date dd_ship  ON fo.ship_date_key  = dd_ship.full_date
""")

count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {catalog}.gold.fact_order_fulfillment").collect()[0]["cnt"]
print(f"fact_order_fulfillment populated: {count} rows")
spark.sql(f"""
    SELECT fulfillment_status, COUNT(*) AS cnt,
           ROUND(AVG(days_to_ship), 1) AS avg_days_to_ship
    FROM {catalog}.gold.fact_order_fulfillment
    GROUP BY fulfillment_status
    ORDER BY cnt DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Type 4: Factless Fact Table
# MAGIC
# MAGIC **Definition:** Records that an event **happened** — with no numeric measures. The row itself is the fact.
# MAGIC
# MAGIC **Why?** Some business events are important to track but have no natural numeric measure. "A promotion was active for this product on this day." There's no revenue, no quantity — just the existence of the relationship.
# MAGIC
# MAGIC **Examples:**
# MAGIC - Product promotions (which products were on promotion on which dates)
# MAGIC - Student enrollment (which student is enrolled in which course)
# MAGIC - Event attendance (who attended which event)
# MAGIC - Security access logs (which user accessed which resource)
# MAGIC
# MAGIC **Queries answered:**
# MAGIC - "Which products were promoted but had zero sales?" (outer join against fact_orders)
# MAGIC - "How many products were promoted in each category last month?"

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.fact_promotions (
        promotion_key   STRING  NOT NULL,
        product_key     STRING,
        date_key        INT,
        promotion_name  STRING,
        promotion_type  STRING
    ) USING DELTA
""")

print("fact_promotions table created (Factless Fact — Type 4)")

# COMMAND ----------

# Populate with synthetic promotion data to demonstrate the concept
# In a real system, this would come from a promotions/marketing source table

spark.sql(f"TRUNCATE TABLE {catalog}.gold.fact_promotions")

spark.sql(f"""
    INSERT INTO {catalog}.gold.fact_promotions
    SELECT
        md5(concat_ws('|', dp.product_key, cast(dd.date_key as string), 'SUMMER_SALE')) AS promotion_key,
        dp.product_key,
        dd.date_key,
        'Summer Sale 2024'                  AS promotion_name,
        'Percentage Discount'               AS promotion_type
    FROM {catalog}.gold.dim_product dp
    CROSS JOIN {catalog}.gold.dim_date dd
    WHERE dp.category IN ('Electronics', 'Clothing')
      AND dp.product_key != 'unknown'
      AND dd.full_date BETWEEN DATE('2024-06-01') AND DATE('2024-06-07')
""")

count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {catalog}.gold.fact_promotions").collect()[0]["cnt"]
print(f"fact_promotions populated: {count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC **The power of the factless fact:** Which promoted products had zero sales during the promotion period?

# COMMAND ----------

spark.sql(f"""
    SELECT
        dp.product_name,
        dp.category,
        fp.promotion_name,
        COALESCE(SUM(fo.total_amount), 0) AS sales_during_promotion,
        COUNT(fo.order_line_key)          AS order_lines_during_promotion
    FROM {catalog}.gold.fact_promotions fp
    JOIN {catalog}.gold.dim_product dp ON fp.product_key = dp.product_key
    JOIN {catalog}.gold.dim_date    dd ON fp.date_key    = dd.date_key
    LEFT JOIN {catalog}.gold.fact_orders fo
        ON fp.product_key = fo.product_key
        AND fo.order_date_key = fp.date_key
    GROUP BY dp.product_name, dp.category, fp.promotion_name
    ORDER BY sales_during_promotion ASC
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: The Four Fact Table Types

# COMMAND ----------

summary_data = [
    ("1. Transactional",        "One row per event",               "Inserted, never updated", "fact_orders, fact_returns",              "order, click, payment"),
    ("2. Periodic Snapshot",    "One row per entity per period",   "Inserted on schedule",    "fact_customer_monthly_snapshot",         "account balance, inventory level"),
    ("3. Accumulating Snapshot","One row per process",             "Updated at each milestone","fact_order_fulfillment",                 "order fulfillment, loan pipeline"),
    ("4. Factless Fact",        "Event with no numeric measure",   "Inserted when event occurs","fact_promotions",                      "enrollment, promotion, attendance"),
]

df_summary = spark.createDataFrame(summary_data, ["type", "grain", "row_behavior", "our_example", "real_world_examples"])
df_summary.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC We've defined the schemas and populated the supporting fact tables. The main event — building `fact_orders` and `fact_returns` with full lookup joins — is in the next two notebooks.
# MAGIC
# MAGIC | Notebook | Focus |
# MAGIC |----------|-------|
# MAGIC | 14_fact_orders.py | The Lookup Join Pattern — resolving all dimension keys |
# MAGIC | 15_fact_returns.py | Conformed Dimensions — sharing dims across fact tables |
# MAGIC
# MAGIC **Next notebook:** `14_fact_orders.py` — Building fact_orders with the full lookup join pattern.
