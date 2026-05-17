# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Analytics Queries
# MAGIC
# MAGIC All four business questions answered with clean SQL on Gold tables only.
# MAGIC No Bronze, no Silver, no raw joins — analysts work entirely in Gold.
# MAGIC
# MAGIC **Business Questions:**
# MAGIC 1. What are total sales by product category per month?
# MAGIC 2. Which customers are our top buyers?
# MAGIC 3. How do sales vary by geography?
# MAGIC 4. What is the average order value by channel?
# MAGIC
# MAGIC **Bonus:** Sales vs Returns comparison using conformed dimensions

# COMMAND ----------

catalog = "workspace"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1: Total sales by product category per month

# COMMAND ----------

spark.sql(f"""
    SELECT
        d.year,
        d.month,
        d.month_name,
        p.category,
        COUNT(DISTINCT f.order_number)   AS total_orders,
        SUM(f.quantity)                  AS units_sold,
        ROUND(SUM(f.total_amount), 2)    AS total_sales
    FROM {catalog}.gold.fact_orders f
    JOIN {catalog}.gold.dim_date    d ON f.order_date_key = d.date_key
    JOIN {catalog}.gold.dim_product p ON f.product_key    = p.product_key
    WHERE p.category != 'Unknown'
    GROUP BY d.year, d.month, d.month_name, p.category
    ORDER BY d.year, d.month, total_sales DESC
""").show(30)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2: Top customers by lifetime value

# COMMAND ----------

spark.sql(f"""
    SELECT
        c.first_name,
        c.last_name,
        c.channel,
        c.city,
        c.state,
        COUNT(DISTINCT f.order_number)            AS total_orders,
        SUM(f.quantity)                           AS total_units,
        ROUND(SUM(f.total_amount), 2)             AS lifetime_value,
        ROUND(SUM(f.total_amount)
            / COUNT(DISTINCT f.order_number), 2)  AS avg_order_value
    FROM {catalog}.gold.fact_orders  f
    JOIN {catalog}.gold.dim_customer c
        ON f.customer_key = c.customer_key AND c.is_current = true
    WHERE c.source_customer_id != -1
    GROUP BY c.first_name, c.last_name, c.channel, c.city, c.state
    ORDER BY lifetime_value DESC
    LIMIT 20
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3: Sales by geography (region → state → city)

# COMMAND ----------

spark.sql(f"""
    SELECT
        l.region,
        l.state,
        l.city,
        COUNT(DISTINCT f.order_number)   AS total_orders,
        SUM(f.quantity)                  AS units_sold,
        ROUND(SUM(f.total_amount), 2)    AS total_sales
    FROM {catalog}.gold.fact_orders  f
    JOIN {catalog}.gold.dim_location l ON f.location_key = l.location_key
    WHERE l.source_location_id != -1
    GROUP BY l.region, l.state, l.city
    ORDER BY total_sales DESC
""").show(30)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q4: Average order value by channel

# COMMAND ----------

spark.sql(f"""
    SELECT
        c.channel,
        COUNT(DISTINCT f.order_number)                    AS total_orders,
        ROUND(SUM(f.total_amount), 2)                     AS total_revenue,
        ROUND(SUM(f.total_amount)
            / COUNT(DISTINCT f.order_number), 2)          AS avg_order_value,
        ROUND(AVG(f.discount_amount), 2)                  AS avg_discount
    FROM {catalog}.gold.fact_orders  f
    JOIN {catalog}.gold.dim_customer c
        ON f.customer_key = c.customer_key AND c.is_current = true
    WHERE c.source_customer_id != -1
    GROUP BY c.channel
    ORDER BY avg_order_value DESC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus: Sales vs Returns by category (conformed dimension payoff)
# MAGIC
# MAGIC This query spans two fact tables (`fact_orders` and `fact_returns`) using the
# MAGIC shared `dim_product`. This is only possible because the dimensions are **conformed**.

# COMMAND ----------

spark.sql(f"""
    SELECT
        p.category,
        ROUND(SUM(o.total_amount), 2)                            AS gross_sales,
        ROUND(COALESCE(SUM(r.return_amount), 0), 2)              AS total_returns,
        ROUND(SUM(o.total_amount)
            - COALESCE(SUM(r.return_amount), 0), 2)              AS net_sales,
        ROUND(COALESCE(SUM(r.return_amount), 0)
            / SUM(o.total_amount) * 100, 1)                      AS return_rate_pct
    FROM {catalog}.gold.dim_product p
    LEFT JOIN {catalog}.gold.fact_orders  o ON p.product_key = o.product_key
    LEFT JOIN {catalog}.gold.fact_returns r ON p.product_key = r.product_key
    WHERE p.category != 'Unknown'
    GROUP BY p.category
    ORDER BY gross_sales DESC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus: Year-over-year sales growth by category

# COMMAND ----------

spark.sql(f"""
    WITH yearly AS (
        SELECT
            d.year,
            p.category,
            ROUND(SUM(f.total_amount), 2) AS total_sales
        FROM {catalog}.gold.fact_orders f
        JOIN {catalog}.gold.dim_date    d ON f.order_date_key = d.date_key
        JOIN {catalog}.gold.dim_product p ON f.product_key    = p.product_key
        WHERE p.category != 'Unknown'
        GROUP BY d.year, p.category
    )
    SELECT
        curr.year,
        curr.category,
        curr.total_sales,
        prev.total_sales                         AS prior_year_sales,
        ROUND((curr.total_sales - prev.total_sales)
            / prev.total_sales * 100, 1)         AS yoy_growth_pct
    FROM yearly curr
    LEFT JOIN yearly prev
        ON curr.category = prev.category
       AND curr.year     = prev.year + 1
    ORDER BY curr.category, curr.year
""").show(40)
