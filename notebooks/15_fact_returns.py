# Databricks notebook source

# MAGIC %md
# MAGIC # Building fact_returns — Conformed Dimensions
# MAGIC
# MAGIC **Notebook 15 of the Data Modeling Masterclass**
# MAGIC
# MAGIC We now build the second fact table in our star schema: `fact_returns`. This notebook demonstrates one of the most powerful architectural concepts in dimensional modeling — **conformed dimensions**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What Are Conformed Dimensions?
# MAGIC
# MAGIC A **conformed dimension** is a dimension table that is **shared, unchanged, across multiple fact tables**.
# MAGIC
# MAGIC In our schema:
# MAGIC - `dim_customer` is shared by `fact_orders` AND `fact_returns`
# MAGIC - `dim_product` is shared by `fact_orders` AND `fact_returns`
# MAGIC - `dim_date` is shared by `fact_orders` AND `fact_returns`
# MAGIC
# MAGIC These are the same physical tables. Same `product_key = 'abc123'` refers to the same product in both fact tables. No duplication, no divergence.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why Conformed Dimensions Matter
# MAGIC
# MAGIC Imagine if each fact table had its own version of dim_product:
# MAGIC
# MAGIC ```
# MAGIC fact_orders → orders_dim_product (category='Electronics')
# MAGIC fact_returns → returns_dim_product (category='Consumer Tech')
# MAGIC ```
# MAGIC
# MAGIC Now try to answer: "What is the return rate by category?"
# MAGIC
# MAGIC ```sql
# MAGIC -- IMPOSSIBLE — 'Electronics' in one table, 'Consumer Tech' in the other
# MAGIC SELECT category, SUM(returns)/SUM(orders)
# MAGIC FROM ... JOIN ...  -- no common category to join on
# MAGIC ```
# MAGIC
# MAGIC With conformed dimensions:
# MAGIC ```sql
# MAGIC -- TRIVIAL — both fact tables use the same product_key from the same dim_product
# MAGIC SELECT p.category,
# MAGIC        SUM(o.total_amount) AS sales,
# MAGIC        SUM(r.return_amount) AS returns
# MAGIC FROM dim_product p
# MAGIC LEFT JOIN fact_orders o ON p.product_key = o.product_key
# MAGIC LEFT JOIN fact_returns r ON p.product_key = r.product_key
# MAGIC GROUP BY p.category
# MAGIC ```
# MAGIC
# MAGIC **Conformed dimensions are the glue that holds a data warehouse together.** They're what transforms a collection of isolated fact tables into an integrated analytical system.

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_returns Design
# MAGIC
# MAGIC **Grain:** One row per return transaction.
# MAGIC
# MAGIC **Dimensions used:**
# MAGIC - `dim_customer` — the customer who returned (same table as fact_orders)
# MAGIC - `dim_product` — the product returned (same table as fact_orders)
# MAGIC - `dim_date` — the date of the return (same table as fact_orders)
# MAGIC
# MAGIC **Note:** No `dim_location` here — we don't track which location the return was processed at in our source data. This is realistic — fact tables don't always use every dimension. They use the dimensions that describe their specific grain.
# MAGIC
# MAGIC **Measures:**
# MAGIC - `quantity_returned` — additive: total units returned
# MAGIC - `return_amount` — additive: total dollar value returned (quantity × unit_price at return time)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create the fact_returns Table

# COMMAND ----------

catalog = "workspace"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.fact_returns (
        return_key          STRING  NOT NULL,
        customer_key        STRING,
        product_key         STRING,
        return_date_key     INT,
        order_number        STRING,
        return_reason       STRING,
        quantity_returned   INT,
        return_amount       DOUBLE
    ) USING DELTA
""")

print(f"fact_returns table ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load fact_returns with the Lookup Join Pattern
# MAGIC
# MAGIC Same lookup join pattern as `fact_orders`. The key insight: we're referencing `dim_customer`, `dim_product`, and `dim_date` — the exact same tables used by `fact_orders`. No new dimensions needed.
# MAGIC
# MAGIC `return_reason` is a string attribute stored directly in the fact table — similar to a degenerate dimension. It has limited cardinality (a few categories) and no additional attributes, so it doesn't warrant its own dimension table. Alternatively, you could create a `dim_return_reason` if there were many attributes to store about each reason code.

# COMMAND ----------

spark.sql(f"TRUNCATE TABLE {catalog}.gold.fact_returns")

spark.sql(f"""
    INSERT INTO {catalog}.gold.fact_returns
    SELECT
        md5(cast(src.return_id as string))                  AS return_key,

        -- Conformed dimension keys — same tables as fact_orders
        coalesce(dc.customer_key, 'unknown')                AS customer_key,
        coalesce(dp.product_key,  'unknown')                AS product_key,

        -- dim_date (conformed — same table as fact_orders order_date join)
        dd.date_key                                         AS return_date_key,

        -- Degenerate dimension — links back to original order
        src.order_number,
        src.return_reason,

        -- Measures
        cast(src.quantity_returned as int)                  AS quantity_returned,
        cast(src.quantity_returned as int)
            * cast(src.unit_price as double)                AS return_amount

    FROM {catalog}.bronze.returns src

    -- Lookup join 1: dim_customer (conformed — shared with fact_orders)
    LEFT JOIN {catalog}.gold.dim_customer dc
        ON src.customer_id = dc.source_customer_id
        AND dc.is_current = true

    -- Lookup join 2: dim_product (conformed — shared with fact_orders)
    LEFT JOIN {catalog}.gold.dim_product dp
        ON src.product_id = dp.source_product_id

    -- Lookup join 3: dim_date (conformed — shared with fact_orders)
    LEFT JOIN {catalog}.gold.dim_date dd
        ON to_date(src.return_date) = dd.full_date
""")

print("fact_returns loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Verify Results

# COMMAND ----------

bronze_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {catalog}.bronze.returns").collect()[0]["cnt"]
gold_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {catalog}.gold.fact_returns").collect()[0]["cnt"]

print(f"Bronze source rows:   {bronze_count:,}")
print(f"fact_returns rows:    {gold_count:,}")
print(f"Row preservation:     {'✓ PERFECT MATCH' if bronze_count == gold_count else '✗ MISMATCH — investigate!'}")

# COMMAND ----------

# Return reasons distribution
spark.sql(f"""
    SELECT
        return_reason,
        COUNT(*)                            AS return_count,
        SUM(quantity_returned)              AS total_units_returned,
        ROUND(SUM(return_amount), 2)        AS total_return_value
    FROM {catalog}.gold.fact_returns
    GROUP BY return_reason
    ORDER BY total_return_value DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Preview with Dimension Labels
# MAGIC
# MAGIC Join back to the conformed dimensions to make the data readable:

# COMMAND ----------

spark.sql(f"""
    SELECT
        fr.order_number,
        dc.first_name || ' ' || dc.last_name    AS customer_name,
        dp.product_name,
        dp.category,
        dd.full_date                            AS return_date,
        fr.return_reason,
        fr.quantity_returned,
        fr.return_amount
    FROM {catalog}.gold.fact_returns fr
    JOIN {catalog}.gold.dim_customer dc ON fr.customer_key = dc.customer_key
    JOIN {catalog}.gold.dim_product  dp ON fr.product_key  = dp.product_key
    JOIN {catalog}.gold.dim_date     dd ON fr.return_date_key = dd.date_key
    WHERE dc.is_current = true
    AND   dc.source_customer_id != -1
    AND   dp.product_key != 'unknown'
    ORDER BY dd.full_date DESC
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Payoff: Cross-Fact-Table Analytics with Conformed Dimensions
# MAGIC
# MAGIC Now we arrive at the moment that justifies everything we've built. Because `dim_product` is a **conformed dimension** — shared unchanged between `fact_orders` and `fact_returns` — we can write a single query that spans both fact tables.
# MAGIC
# MAGIC **Business question:** "For each product category, what are our total sales, total returns, and return rate? Which categories have a return problem?"
# MAGIC
# MAGIC This query would be **impossible** if each fact table had its own product dimension with different keys.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales vs. Returns by Category

# COMMAND ----------

spark.sql(f"""
    SELECT
        p.category,
        ROUND(SUM(o.total_amount), 2)                   AS gross_sales,
        SUM(o.quantity)                                  AS units_sold,
        ROUND(COALESCE(SUM(r.return_amount), 0), 2)     AS total_returns,
        COALESCE(SUM(r.quantity_returned), 0)            AS units_returned,
        ROUND(
            COALESCE(SUM(r.return_amount), 0)
            / NULLIF(SUM(o.total_amount), 0) * 100,
        1)                                               AS return_rate_pct
    FROM {catalog}.gold.dim_product p
    LEFT JOIN {catalog}.gold.fact_orders  o ON p.product_key = o.product_key
    LEFT JOIN {catalog}.gold.fact_returns r ON p.product_key = r.product_key
    WHERE p.category != 'Unknown'
    GROUP BY p.category
    ORDER BY gross_sales DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Returns by Month (using conformed dim_date)

# COMMAND ----------

spark.sql(f"""
    SELECT
        dd.year,
        dd.month_of_year                                AS month,
        dd.month_name,
        COUNT(fr.return_key)                            AS return_count,
        SUM(fr.quantity_returned)                       AS units_returned,
        ROUND(SUM(fr.return_amount), 2)                 AS return_value
    FROM {catalog}.gold.fact_returns fr
    JOIN {catalog}.gold.dim_date dd ON fr.return_date_key = dd.date_key
    GROUP BY dd.year, dd.month_of_year, dd.month_name
    ORDER BY dd.year, dd.month_of_year
""").show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top Customers by Return Behavior (using conformed dim_customer)

# COMMAND ----------

spark.sql(f"""
    SELECT
        dc.first_name || ' ' || dc.last_name    AS customer_name,
        dc.channel,
        dc.city,
        SUM(fo.total_amount)                     AS lifetime_sales,
        ROUND(COALESCE(SUM(fr.return_amount), 0), 2) AS lifetime_returns,
        ROUND(
            COALESCE(SUM(fr.return_amount), 0)
            / NULLIF(SUM(fo.total_amount), 0) * 100,
        1)                                       AS return_rate_pct
    FROM {catalog}.gold.dim_customer dc
    JOIN {catalog}.gold.fact_orders  fo ON dc.customer_key = fo.customer_key
    LEFT JOIN {catalog}.gold.fact_returns fr ON dc.customer_key = fr.customer_key
    WHERE dc.is_current = true
    AND   dc.source_customer_id != -1
    GROUP BY customer_name, dc.channel, dc.city
    HAVING lifetime_returns > 0
    ORDER BY return_rate_pct DESC
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Concept | Implementation |
# MAGIC |---------|---------------|
# MAGIC | Conformed dimensions | `dim_customer`, `dim_product`, `dim_date` shared across fact tables |
# MAGIC | Cross-fact analytics | `LEFT JOIN fact_orders` AND `LEFT JOIN fact_returns` on same `product_key` |
# MAGIC | Grain | One row per return transaction |
# MAGIC | Row preservation | Bronze count = Gold count verified |
# MAGIC | Return reason | Stored as string in fact (no dim table needed — no additional attributes) |
# MAGIC | `order_number` | Degenerate dimension — links return back to originating order |
# MAGIC
# MAGIC **What conformed dimensions enable:**
# MAGIC - Single query spans multiple fact tables
# MAGIC - Consistent category names, customer names, dates across all reports
# MAGIC - "Sales vs. Returns by Category" becomes a simple LEFT JOIN
# MAGIC - New fact tables automatically inherit the existing dimension framework
# MAGIC
# MAGIC **Next notebook:** `16_analytics_and_metrics.py` — Building the full suite of business analytics on top of our completed star schema.
