# Databricks notebook source

# MAGIC %md
# MAGIC # Analytics Queries & Business Metrics
# MAGIC
# MAGIC **Notebook 16 of the Data Modeling Masterclass**
# MAGIC
# MAGIC The star schema is complete. All dimensions are built, both fact tables are loaded. Now we answer the business questions that motivated this entire project — using only Gold tables, no Bronze, no raw joins. This is the payoff.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Rule: Gold Only
# MAGIC
# MAGIC Every query in this notebook reads **only from Gold tables**:
# MAGIC - `workspace.gold.fact_orders`
# MAGIC - `workspace.gold.fact_returns`
# MAGIC - `workspace.gold.dim_customer`
# MAGIC - `workspace.gold.dim_product`
# MAGIC - `workspace.gold.dim_location`
# MAGIC - `workspace.gold.dim_date`
# MAGIC
# MAGIC No reads from Bronze. No raw joins against source tables. No casting or cleaning logic in query time.
# MAGIC
# MAGIC **Why this matters:** Any analyst, data scientist, or BI tool connecting to this workspace can write simple, reliable queries without knowing anything about the source system's structure, data quality issues, or join complexity. That's what a well-built data warehouse enables.

# COMMAND ----------

catalog = "workspace"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Q1: Total Sales by Category per Month
# MAGIC
# MAGIC *"Show me revenue trends over time, broken down by product category."*
# MAGIC
# MAGIC This query uses three tables: `fact_orders`, `dim_product` (for category), and `dim_date` (for year/month). A classic star schema query — navigate from the fact table out to the relevant dimensions.

# COMMAND ----------

spark.sql(f"""
    SELECT
        dd.year,
        dd.month_of_year                        AS month,
        dd.month_name,
        dp.category,
        COUNT(fo.order_line_key)                AS line_items,
        SUM(fo.quantity)                        AS total_units,
        ROUND(SUM(fo.total_amount), 2)          AS total_revenue,
        ROUND(AVG(fo.total_amount), 2)          AS avg_line_value
    FROM {catalog}.gold.fact_orders fo
    JOIN {catalog}.gold.dim_product dp ON fo.product_key = dp.product_key
    JOIN {catalog}.gold.dim_date    dd ON fo.order_date_key = dd.date_key
    WHERE dp.category != 'Unknown'
    GROUP BY dd.year, dd.month_of_year, dd.month_name, dp.category
    ORDER BY dd.year, dd.month_of_year, total_revenue DESC
    LIMIT 24
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Q2: Top 10 Customers by Lifetime Value
# MAGIC
# MAGIC *"Who are our most valuable customers? How much have they spent in total?"*
# MAGIC
# MAGIC Lifetime value (LTV) is one of the most fundamental business metrics. Using `dim_customer` gives us name, channel, and location — we can immediately see if high-value customers skew toward a particular channel or region.

# COMMAND ----------

spark.sql(f"""
    SELECT
        dc.first_name || ' ' || dc.last_name    AS customer_name,
        dc.channel,
        dc.city,
        dc.state,
        COUNT(DISTINCT fo.order_number)         AS total_orders,
        SUM(fo.quantity)                        AS total_units,
        ROUND(SUM(fo.total_amount), 2)          AS lifetime_value,
        ROUND(SUM(fo.discount_amount), 2)       AS total_discounts,
        ROUND(SUM(fo.total_amount) / COUNT(DISTINCT fo.order_number), 2) AS avg_order_value
    FROM {catalog}.gold.fact_orders fo
    JOIN {catalog}.gold.dim_customer dc ON fo.customer_key = dc.customer_key
    WHERE dc.is_current = true
    AND   dc.source_customer_id != -1
    GROUP BY customer_name, dc.channel, dc.city, dc.state
    ORDER BY lifetime_value DESC
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Q3: Sales by Geography
# MAGIC
# MAGIC *"Which regions and states are driving the most revenue?"*
# MAGIC
# MAGIC This uses `dim_location` — a dimension we spent very little time building (notebook 10), but which enables powerful geographic slicing here.

# COMMAND ----------

spark.sql(f"""
    SELECT
        dl.region,
        dl.state,
        COUNT(fo.order_line_key)                AS line_items,
        COUNT(DISTINCT fo.customer_key)         AS unique_customers,
        ROUND(SUM(fo.total_amount), 2)          AS total_revenue,
        ROUND(SUM(fo.total_amount)
            / COUNT(DISTINCT fo.customer_key), 2) AS revenue_per_customer
    FROM {catalog}.gold.fact_orders fo
    JOIN {catalog}.gold.dim_location dl ON fo.location_key = dl.location_key
    WHERE dl.location_key != 'unknown'
    GROUP BY dl.region, dl.state
    ORDER BY total_revenue DESC
    LIMIT 15
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Q4: Average Order Value by Acquisition Channel
# MAGIC
# MAGIC *"Do customers acquired through different channels have different spending patterns?"*
# MAGIC
# MAGIC AOV (Average Order Value) is a key e-commerce metric. Channels with lower AOV might need different promotions to increase basket size. Channels with higher AOV are premium segments worth protecting.

# COMMAND ----------

spark.sql(f"""
    SELECT
        dc.channel,
        COUNT(DISTINCT fo.order_number)         AS total_orders,
        COUNT(DISTINCT dc.customer_key)         AS unique_customers,
        ROUND(SUM(fo.total_amount), 2)          AS total_revenue,
        ROUND(SUM(fo.total_amount)
            / COUNT(DISTINCT fo.order_number), 2) AS avg_order_value,
        ROUND(SUM(fo.quantity)
            / COUNT(DISTINCT fo.order_number), 1) AS avg_items_per_order,
        ROUND(AVG(fo.discount_amount), 2)       AS avg_discount_per_line
    FROM {catalog}.gold.fact_orders fo
    JOIN {catalog}.gold.dim_customer dc ON fo.customer_key = dc.customer_key
    WHERE dc.is_current = true
    AND   dc.source_customer_id != -1
    GROUP BY dc.channel
    ORDER BY avg_order_value DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Bonus: Year-over-Year Revenue Growth
# MAGIC
# MAGIC A more sophisticated query using a CTE to compute YoY growth. This requires only `fact_orders` and `dim_date` — two tables.
# MAGIC
# MAGIC The pattern: aggregate by year in the CTE, then self-join to compare current year to prior year.

# COMMAND ----------

spark.sql(f"""
    WITH annual_revenue AS (
        SELECT
            dd.year,
            dp.category,
            ROUND(SUM(fo.total_amount), 2) AS revenue
        FROM {catalog}.gold.fact_orders fo
        JOIN {catalog}.gold.dim_date    dd ON fo.order_date_key = dd.date_key
        JOIN {catalog}.gold.dim_product dp ON fo.product_key = dp.product_key
        WHERE dp.category != 'Unknown'
        GROUP BY dd.year, dp.category
    ),
    yoy AS (
        SELECT
            curr.year,
            curr.category,
            curr.revenue                        AS current_year_revenue,
            prev.revenue                        AS prior_year_revenue,
            ROUND(curr.revenue - COALESCE(prev.revenue, 0), 2) AS revenue_change,
            ROUND(
                (curr.revenue - COALESCE(prev.revenue, curr.revenue))
                / NULLIF(prev.revenue, 0) * 100,
            1)                                  AS yoy_growth_pct
        FROM annual_revenue curr
        LEFT JOIN annual_revenue prev
            ON curr.category = prev.category
            AND curr.year = prev.year + 1
    )
    SELECT *
    FROM yoy
    ORDER BY year DESC, current_year_revenue DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Business Metrics
# MAGIC
# MAGIC The four queries above answer the project brief. Now let's go further with industry-standard business metrics. Each metric is defined first, then implemented with SQL.
# MAGIC
# MAGIC These are the metrics you'd find in a product analytics dashboard or an investor report. The star schema makes all of them straightforward — no raw joins, no data cleaning, just clean SQL on clean Gold tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metric 1: Daily Active Buyers (DAB)
# MAGIC
# MAGIC **Definition:** The count of distinct customers who placed at least one order on a given calendar day.
# MAGIC
# MAGIC *Note: "DAU" (Daily Active Users) typically tracks logins or sessions. In e-commerce, the equivalent is "Daily Active Buyers" — users who transacted.*
# MAGIC
# MAGIC **Why it matters:** Tracks day-to-day health of the marketplace. Spikes indicate promotions, troughs indicate problems.

# COMMAND ----------

spark.sql(f"""
    SELECT
        dd.full_date,
        dd.day_name,
        dd.is_weekend,
        COUNT(DISTINCT fo.customer_key)     AS daily_active_buyers,
        COUNT(fo.order_line_key)            AS order_lines,
        ROUND(SUM(fo.total_amount), 2)      AS daily_revenue
    FROM {catalog}.gold.fact_orders fo
    JOIN {catalog}.gold.dim_date dd ON fo.order_date_key = dd.date_key
    WHERE dd.year = 2024 AND dd.month_of_year = 1
    AND   fo.customer_key != 'unknown'
    GROUP BY dd.full_date, dd.day_name, dd.is_weekend
    ORDER BY dd.full_date
    LIMIT 31
""").show(31, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metric 2: Monthly Active Buyers (MAB) + Stickiness
# MAGIC
# MAGIC **MAB Definition:** Count of distinct customers who placed at least one order in a given calendar month.
# MAGIC
# MAGIC **Stickiness Definition:** DAB / MAB — what fraction of monthly buyers also buy daily on average. Higher stickiness = more habitual purchasing behavior.
# MAGIC
# MAGIC *Classic product metric: DAU/MAU for apps = stickiness. For e-commerce, DAB/MAB.*

# COMMAND ----------

spark.sql(f"""
    WITH daily AS (
        SELECT
            dd.year,
            dd.month_of_year,
            dd.full_date,
            COUNT(DISTINCT fo.customer_key) AS dab
        FROM {catalog}.gold.fact_orders fo
        JOIN {catalog}.gold.dim_date dd ON fo.order_date_key = dd.date_key
        WHERE fo.customer_key != 'unknown'
        GROUP BY dd.year, dd.month_of_year, dd.full_date
    ),
    monthly AS (
        SELECT
            dd.year,
            dd.month_of_year,
            dd.month_name,
            COUNT(DISTINCT fo.customer_key)     AS mab,
            ROUND(SUM(fo.total_amount), 2)      AS monthly_revenue,
            COUNT(DISTINCT fo.order_number)     AS monthly_orders
        FROM {catalog}.gold.fact_orders fo
        JOIN {catalog}.gold.dim_date dd ON fo.order_date_key = dd.date_key
        WHERE fo.customer_key != 'unknown'
        GROUP BY dd.year, dd.month_of_year, dd.month_name
    )
    SELECT
        m.year,
        m.month_of_year,
        m.month_name,
        m.mab,
        ROUND(AVG(d.dab), 1)        AS avg_daily_active_buyers,
        ROUND(AVG(d.dab) / m.mab * 100, 1) AS stickiness_pct,
        m.monthly_revenue,
        m.monthly_orders,
        ROUND(m.monthly_revenue / m.monthly_orders, 2) AS monthly_aov
    FROM monthly m
    JOIN daily d ON m.year = d.year AND m.month_of_year = d.month_of_year
    GROUP BY m.year, m.month_of_year, m.month_name, m.mab, m.monthly_revenue, m.monthly_orders
    ORDER BY m.year, m.month_of_year
""").show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metric 3: Cohort Retention
# MAGIC
# MAGIC **Definition:** For customers who made their first purchase in a given month (their acquisition cohort), what fraction of them returned to purchase in each subsequent month?
# MAGIC
# MAGIC **Why it matters:** Retention is the most important metric in subscription and repeat-purchase businesses. A cohort that retains 30% in month 2 is fundamentally healthier than one that retains 5%, regardless of acquisition volume.
# MAGIC
# MAGIC **Implementation:** Two steps:
# MAGIC 1. Find each customer's first order month (acquisition cohort)
# MAGIC 2. Count how many of those customers also ordered in each subsequent month
# MAGIC
# MAGIC The `cohort_offset` is the number of months after acquisition. Offset 0 = 100% (everyone is "retained" in their acquisition month). Offset 1 = % who came back next month. Etc.

# COMMAND ----------

spark.sql(f"""
    WITH first_order AS (
        SELECT
            fo.customer_key,
            MIN(dd.year * 100 + dd.month_of_year)   AS cohort_ym,
            MIN(dd.year)                             AS cohort_year,
            MIN(dd.month_of_year)                    AS cohort_month
        FROM {catalog}.gold.fact_orders fo
        JOIN {catalog}.gold.dim_date dd ON fo.order_date_key = dd.date_key
        WHERE fo.customer_key != 'unknown'
        GROUP BY fo.customer_key
    ),
    subsequent_orders AS (
        SELECT
            fo.customer_key,
            dd.year * 100 + dd.month_of_year        AS order_ym,
            dd.year                                  AS order_year,
            dd.month_of_year                         AS order_month
        FROM {catalog}.gold.fact_orders fo
        JOIN {catalog}.gold.dim_date dd ON fo.order_date_key = dd.date_key
        WHERE fo.customer_key != 'unknown'
        GROUP BY fo.customer_key, dd.year, dd.month_of_year
    ),
    cohort_data AS (
        SELECT
            f.cohort_year,
            f.cohort_month,
            f.cohort_ym,
            s.order_ym,
            (s.order_year * 12 + s.order_month) -
            (f.cohort_year * 12 + f.cohort_month)   AS cohort_offset,
            COUNT(DISTINCT s.customer_key)           AS retained_customers
        FROM first_order f
        JOIN subsequent_orders s ON f.customer_key = s.customer_key
        GROUP BY f.cohort_year, f.cohort_month, f.cohort_ym, s.order_ym,
                 s.order_year, s.order_month
    ),
    cohort_size AS (
        SELECT cohort_ym, COUNT(*) AS cohort_customer_count
        FROM first_order
        GROUP BY cohort_ym
    )
    SELECT
        cd.cohort_year,
        cd.cohort_month,
        cs.cohort_customer_count,
        cd.cohort_offset                            AS months_after_acquisition,
        cd.retained_customers,
        ROUND(cd.retained_customers * 100.0 / cs.cohort_customer_count, 1) AS retention_rate_pct
    FROM cohort_data cd
    JOIN cohort_size cs ON cd.cohort_ym = cs.cohort_ym
    WHERE cd.cohort_offset BETWEEN 0 AND 3
    ORDER BY cd.cohort_year, cd.cohort_month, cd.cohort_offset
    LIMIT 40
""").show(40, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metric 4: Return Rate by Category
# MAGIC
# MAGIC **Definition:** The percentage of gross sales value that was returned, broken down by product category.
# MAGIC
# MAGIC **Why it matters:** A high return rate signals product quality issues, misleading descriptions, or sizing problems. Electronics with >15% return rate is an industry alert. Clothing at 30% might be acceptable.
# MAGIC
# MAGIC This metric requires joining BOTH fact tables against the conformed `dim_product` — exactly the scenario that conformed dimensions were designed for.

# COMMAND ----------

spark.sql(f"""
    SELECT
        dp.category,
        dp.subcategory,
        ROUND(SUM(fo.total_amount), 2)                  AS gross_sales,
        SUM(fo.quantity)                                 AS units_sold,
        ROUND(COALESCE(SUM(fr.return_amount), 0), 2)    AS total_returns,
        COALESCE(SUM(fr.quantity_returned), 0)           AS units_returned,
        ROUND(
            COALESCE(SUM(fr.return_amount), 0)
            / NULLIF(SUM(fo.total_amount), 0) * 100,
        2)                                               AS return_rate_pct,
        ROUND(
            COALESCE(SUM(fr.quantity_returned), 0) * 100.0
            / NULLIF(SUM(fo.quantity), 0),
        2)                                               AS unit_return_rate_pct
    FROM {catalog}.gold.dim_product dp
    LEFT JOIN {catalog}.gold.fact_orders  fo ON dp.product_key = fo.product_key
    LEFT JOIN {catalog}.gold.fact_returns fr ON dp.product_key = fr.product_key
    WHERE dp.category != 'Unknown'
    GROUP BY dp.category, dp.subcategory
    ORDER BY return_rate_pct DESC
    LIMIT 20
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metric 5: Average Order Value (AOV) by Channel and Period
# MAGIC
# MAGIC **Definition:** Total revenue divided by number of distinct orders. Computed by channel and time period.
# MAGIC
# MAGIC AOV is foundational: `AOV × Order Frequency × Margin = Profitability per Customer`. Improving AOV by even 10% can be more impactful than a 10% increase in customer count.

# COMMAND ----------

spark.sql(f"""
    SELECT
        dc.channel,
        dd.year,
        dd.quarter,
        COUNT(DISTINCT fo.order_number)                 AS order_count,
        ROUND(SUM(fo.total_amount), 2)                  AS total_revenue,
        ROUND(SUM(fo.total_amount)
            / COUNT(DISTINCT fo.order_number), 2)       AS aov,
        ROUND(SUM(fo.discount_amount)
            / COUNT(DISTINCT fo.order_number), 2)       AS avg_discount_per_order,
        ROUND((SUM(fo.total_amount) + SUM(fo.discount_amount))
            / COUNT(DISTINCT fo.order_number), 2)       AS gross_aov_before_discount
    FROM {catalog}.gold.fact_orders fo
    JOIN {catalog}.gold.dim_customer dc ON fo.customer_key = dc.customer_key
    JOIN {catalog}.gold.dim_date     dd ON fo.order_date_key = dd.date_key
    WHERE dc.is_current = true
    AND   dc.source_customer_id != -1
    GROUP BY dc.channel, dd.year, dd.quarter
    ORDER BY dd.year, dd.quarter, dc.channel
""").show(30, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## The Payoff: Simple SQL on a Clean Foundation
# MAGIC
# MAGIC Look back at the queries in this notebook. Every one of them is straightforward SQL:
# MAGIC - A `FROM` clause pointing to a Gold table
# MAGIC - `JOIN`s to dimension tables (never more than 3-4 hops)
# MAGIC - `GROUP BY` on dimension attributes
# MAGIC - `SUM()`, `COUNT()`, `AVG()` on fact measures
# MAGIC
# MAGIC No CTEs to decode business logic from raw schemas. No `CASE WHEN status = 2 AND type_flag = 'A'` buried in 500-line queries. No joins across 12 normalized OLTP tables.
# MAGIC
# MAGIC **This is what good data modeling delivers:** a clean, queryable foundation where the hard work of transformation, normalization, and key resolution has already been done once — at load time — so that every subsequent query can be simple, fast, and correct.
# MAGIC
# MAGIC **Next (and final) notebook:** `17_pipeline_orchestration.py` — How to load all of this in the right order, automatically, on a schedule.
