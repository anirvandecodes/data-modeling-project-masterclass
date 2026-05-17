# Databricks notebook source
# MAGIC %md
# MAGIC # 10 — Fact Table Types
# MAGIC
# MAGIC Kimball defines four types of fact tables. Choosing the right type depends on
# MAGIC the business process and the grain of measurement.
# MAGIC
# MAGIC | Type | Grain | When to use |
# MAGIC |---|---|---|
# MAGIC | Transactional | One row per event | Orders, clicks, logins, rides |
# MAGIC | Periodic Snapshot | One row per entity per time period | Daily balances, monthly storage |
# MAGIC | Accumulating Snapshot | One row per process instance, updated | Order fulfillment pipeline |
# MAGIC | Factless | Events with no measures | Attendance, eligibility, coverage |

# COMMAND ----------

catalog = "workspace"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Type 1: Transactional Fact
# MAGIC
# MAGIC **One row per atomic business event.** The most common type.
# MAGIC Each row is inserted once and never updated.
# MAGIC All measures are fully additive across all dimensions.
# MAGIC
# MAGIC **Example:** `fact_orders` — already built in notebook 06.
# MAGIC One row = one order line item. `total_amount` can be summed across
# MAGIC any combination of customer, product, date, or location.

# COMMAND ----------

spark.sql(f"""
    SELECT 'fact_orders' AS table_name, COUNT(*) AS row_count,
           ROUND(SUM(total_amount), 2) AS total_revenue
    FROM {catalog}.gold.fact_orders
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Type 2: Periodic Snapshot Fact
# MAGIC
# MAGIC **One row per entity per time period**, regardless of whether an event occurred.
# MAGIC Captures the STATE of something at a point in time.
# MAGIC
# MAGIC **Classic use case:** Bank account balances, inventory levels, storage usage.
# MAGIC
# MAGIC **Key characteristic:** Measures are often SEMI-ADDITIVE.
# MAGIC - ✅ SUM balance across all accounts on a single day = platform total
# MAGIC - ❌ SUM balance across days for one account = meaningless (double-counts)
# MAGIC - ✅ AVG balance across days for one account = average daily balance (meaningful)
# MAGIC
# MAGIC **Demo:** Build a monthly order snapshot per customer.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.fact_customer_monthly_snapshot (
        snapshot_key          STRING,
        customer_key          STRING,
        snapshot_year         INT,
        snapshot_month        INT,
        snapshot_date_key     INT,
        orders_placed         INT,
        units_purchased       INT,
        total_spend           DOUBLE,
        cumulative_spend      DOUBLE
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"TRUNCATE TABLE {catalog}.gold.fact_customer_monthly_snapshot")

spark.sql(f"""
    INSERT INTO {catalog}.gold.fact_customer_monthly_snapshot
    WITH monthly AS (
        SELECT
            f.customer_key,
            d.year                                AS snapshot_year,
            d.month                               AS snapshot_month,
            MAX(d.date_key)                       AS snapshot_date_key,
            COUNT(DISTINCT f.order_number)        AS orders_placed,
            SUM(f.quantity)                       AS units_purchased,
            ROUND(SUM(f.total_amount), 2)         AS total_spend
        FROM {catalog}.gold.fact_orders f
        JOIN {catalog}.gold.dim_date   d ON f.order_date_key = d.date_key
        WHERE f.customer_key != 'unknown'
        GROUP BY f.customer_key, d.year, d.month
    )
    SELECT
        md5(concat_ws('|', customer_key,
            cast(snapshot_year as string),
            cast(snapshot_month as string)
        ))                                        AS snapshot_key,
        customer_key,
        snapshot_year,
        snapshot_month,
        snapshot_date_key,
        orders_placed,
        units_purchased,
        total_spend,
        ROUND(SUM(total_spend) OVER (
            PARTITION BY customer_key
            ORDER BY snapshot_year, snapshot_month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 2)                                     AS cumulative_spend
    FROM monthly
""")

count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {catalog}.gold.fact_customer_monthly_snapshot").collect()[0]["cnt"]
print(f"Monthly snapshot loaded: {count} rows")

# COMMAND ----------

spark.sql(f"""
    SELECT
        c.first_name, c.last_name,
        s.snapshot_year, s.snapshot_month,
        s.orders_placed, s.total_spend, s.cumulative_spend
    FROM {catalog}.gold.fact_customer_monthly_snapshot s
    JOIN {catalog}.gold.dim_customer c
        ON s.customer_key = c.customer_key AND c.is_current = true
    WHERE c.source_customer_id = 1
    ORDER BY s.snapshot_year, s.snapshot_month
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Type 3: Accumulating Snapshot Fact
# MAGIC
# MAGIC **One row per process instance**, updated as the process moves through milestones.
# MAGIC Each milestone gets its own date key. The row is updated (not inserted) when a
# MAGIC milestone is reached.
# MAGIC
# MAGIC **Classic use case:** Order fulfillment pipeline — placed → picked → shipped → delivered.
# MAGIC
# MAGIC **Key characteristic:** Multiple date keys in one row — one per milestone.
# MAGIC Lag measures (e.g., days from order to ship) are derived from milestone dates.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.fact_order_fulfillment (
        order_key               STRING,
        customer_key            STRING,
        product_key             STRING,
        order_number            STRING,
        quantity                INT,
        total_amount            DOUBLE,
        order_date_key          INT,
        ship_date_key           INT,
        delivery_date_key       INT,
        days_to_ship            INT,
        days_to_deliver         INT,
        fulfillment_status      STRING
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"TRUNCATE TABLE {catalog}.gold.fact_order_fulfillment")

spark.sql(f"""
    INSERT INTO {catalog}.gold.fact_order_fulfillment
    SELECT
        md5(concat_ws('|',
            cast(src.order_id as string),
            cast(src.line_item_id as string)
        ))                                              AS order_key,
        coalesce(dc.customer_key, 'unknown')            AS customer_key,
        coalesce(dp.product_key,  'unknown')            AS product_key,
        src.order_number,
        cast(src.quantity as int)                       AS quantity,
        cast(src.quantity as int) * cast(src.unit_price as double)
            - coalesce(cast(src.discount_amount as double), 0.0)
                                                        AS total_amount,
        dd_order.date_key                               AS order_date_key,
        dd_ship.date_key                                AS ship_date_key,
        NULL                                            AS delivery_date_key,
        datediff(to_date(src.ship_date), to_date(src.order_date))
                                                        AS days_to_ship,
        NULL                                            AS days_to_deliver,
        CASE
            WHEN src.ship_date IS NOT NULL THEN 'Shipped'
            ELSE 'Processing'
        END                                             AS fulfillment_status

    FROM {catalog}.bronze.orders src

    LEFT JOIN {catalog}.gold.dim_customer dc
        ON src.customer_id = dc.source_customer_id AND dc.is_current = true
    LEFT JOIN {catalog}.gold.dim_product dp
        ON src.product_id = dp.source_product_id
    LEFT JOIN {catalog}.gold.dim_date dd_order
        ON to_date(src.order_date) = dd_order.full_date
    LEFT JOIN {catalog}.gold.dim_date dd_ship
        ON to_date(src.ship_date) = dd_ship.full_date
""")

count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {catalog}.gold.fact_order_fulfillment").collect()[0]["cnt"]
print(f"Accumulating snapshot loaded: {count} rows")

# COMMAND ----------

spark.sql(f"""
    SELECT
        fulfillment_status,
        COUNT(*)                        AS orders,
        ROUND(AVG(days_to_ship), 1)     AS avg_days_to_ship
    FROM {catalog}.gold.fact_order_fulfillment
    GROUP BY fulfillment_status
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Type 4: Factless Fact
# MAGIC
# MAGIC **An event with no numeric measures.** Records that something happened,
# MAGIC not how much of it happened.
# MAGIC
# MAGIC **Use cases:**
# MAGIC - Student attendance (present / absent — no numeric measure)
# MAGIC - Product promotion coverage (which products are on promotion on which days)
# MAGIC - Employee eligibility (which employees qualify for which benefits)
# MAGIC
# MAGIC **Demo:** Track which products were promoted on which dates.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.fact_promotions (
        promotion_key   STRING,
        product_key     STRING,
        date_key        INT,
        promotion_name  STRING
    )
    USING DELTA
""")

from pyspark.sql.functions import md5, concat_ws, col, lit
from datetime import date, timedelta

promos = []
promo_products = [1, 2, 5, 8, 11, 15]
for pid in promo_products:
    promo_start = date(2024, 11, 25)
    for day_offset in range(7):
        d = promo_start + timedelta(days=day_offset)
        promos.append({"product_id": pid, "promo_date": str(d), "promotion_name": "Black Friday 2024"})

promo_start2 = date(2024, 12, 26)
for pid in [3, 7, 12, 20]:
    for day_offset in range(5):
        d = promo_start2 + timedelta(days=day_offset)
        promos.append({"product_id": pid, "promo_date": str(d), "promotion_name": "Year-End Sale"})

promo_df = spark.createDataFrame(promos)

promo_df.createOrReplaceTempView("raw_promotions")

spark.sql(f"""
    INSERT INTO {catalog}.gold.fact_promotions
    SELECT
        md5(concat_ws('|',
            cast(p.product_id as string),
            p.promo_date
        ))              AS promotion_key,
        coalesce(dp.product_key, 'unknown') AS product_key,
        dd.date_key,
        p.promotion_name
    FROM raw_promotions p
    LEFT JOIN {catalog}.gold.dim_product dp ON p.product_id = dp.source_product_id
    LEFT JOIN {catalog}.gold.dim_date    dd ON to_date(p.promo_date) = dd.full_date
""")

count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {catalog}.gold.fact_promotions").collect()[0]["cnt"]
print(f"Factless fact loaded: {count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC **Factless fact query pattern:** Count events, not measures.
# MAGIC
# MAGIC "How many distinct products were promoted each week?"

# COMMAND ----------

spark.sql(f"""
    SELECT
        d.year,
        d.week_of_year,
        fp.promotion_name,
        COUNT(DISTINCT fp.product_key)  AS products_on_promo,
        COUNT(*)                        AS promo_product_days
    FROM {catalog}.gold.fact_promotions fp
    JOIN {catalog}.gold.dim_date        d  ON fp.date_key    = d.date_key
    GROUP BY d.year, d.week_of_year, fp.promotion_name
    ORDER BY d.year, d.week_of_year
""").show()
