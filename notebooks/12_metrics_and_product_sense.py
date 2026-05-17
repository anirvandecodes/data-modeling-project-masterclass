# Databricks notebook source
# MAGIC %md
# MAGIC # 12 — Business Metrics & Product-Sense Data Modeling
# MAGIC
# MAGIC ## The Right Order: Metrics First, Model Second
# MAGIC
# MAGIC A common interview mistake: jumping straight into table design.
# MAGIC The right order is:
# MAGIC
# MAGIC 1. **Understand the product** (what does it do, who uses it)
# MAGIC 2. **Define the metrics** (what does success look like)
# MAGIC 3. **Design the model** (what tables do you need to compute those metrics)
# MAGIC 4. **Write the SQL** (prove the model can answer the questions)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Core Business Metrics
# MAGIC
# MAGIC | Metric | Definition | Fact Table Column |
# MAGIC |---|---|---|
# MAGIC | DAU | Distinct active users in a day | COUNT(DISTINCT user_key) per date |
# MAGIC | MAU | Distinct active users in 30 days | COUNT(DISTINCT user_key) per 30-day window |
# MAGIC | DAU/MAU | Stickiness ratio | DAU / MAU — higher = more engaging product |
# MAGIC | Daily Sales | SUM(revenue) per day | SUM(total_amount) per order_date_key |
# MAGIC | AOV | Average Order Value | SUM(revenue) / COUNT(DISTINCT orders) |
# MAGIC | Sessions | Distinct sessions per user per day | COUNT(DISTINCT session_id) |
# MAGIC | Time Spent | Total time in the product | SUM(duration_seconds) per user per day |
# MAGIC | Conversion Rate | % of sessions that result in purchase | orders / sessions |
# MAGIC | Retention D1/D7/D30 | % of new users active N days later | cohort analysis |
# MAGIC | Churn Rate | % of users who stop using the product | 1 - retention rate |
# MAGIC | LTV | Lifetime value of a customer | SUM(total_amount) per customer |
# MAGIC | CAC | Cost to acquire a customer | marketing_spend / new_customers |
# MAGIC | Return Rate | % of revenue returned | SUM(returns) / SUM(sales) |

# COMMAND ----------

catalog = "workspace"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo: Compute Core Metrics on Our E-Commerce Model
# MAGIC
# MAGIC We'll compute several of these metrics using `fact_orders`, `fact_returns`,
# MAGIC and the Gold dimension tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daily Active Buyers (DAB)
# MAGIC Distinct customers who placed at least one order per day.

# COMMAND ----------

spark.sql(f"""
    SELECT
        d.full_date,
        COUNT(DISTINCT f.customer_key)  AS daily_active_buyers,
        COUNT(DISTINCT f.order_number)  AS daily_orders,
        ROUND(SUM(f.total_amount), 2)   AS daily_revenue
    FROM {catalog}.gold.fact_orders f
    JOIN {catalog}.gold.dim_date    d ON f.order_date_key = d.date_key
    WHERE f.customer_key != 'unknown'
      AND d.year = 2024
      AND d.month = 1
    GROUP BY d.full_date
    ORDER BY d.full_date
""").show(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monthly Active Buyers (MAB) and Stickiness
# MAGIC MAB = distinct buyers in the month.
# MAGIC Stickiness = avg(DAB) / MAB — what % of monthly buyers engage daily on average.

# COMMAND ----------

spark.sql(f"""
    WITH daily AS (
        SELECT
            d.year, d.month,
            d.full_date,
            COUNT(DISTINCT f.customer_key) AS dab
        FROM {catalog}.gold.fact_orders f
        JOIN {catalog}.gold.dim_date    d ON f.order_date_key = d.date_key
        WHERE f.customer_key != 'unknown'
        GROUP BY d.year, d.month, d.full_date
    ),
    monthly AS (
        SELECT
            d.year, d.month,
            COUNT(DISTINCT f.customer_key) AS mab
        FROM {catalog}.gold.fact_orders f
        JOIN {catalog}.gold.dim_date    d ON f.order_date_key = d.date_key
        WHERE f.customer_key != 'unknown'
        GROUP BY d.year, d.month
    )
    SELECT
        m.year,
        m.month,
        m.mab,
        ROUND(AVG(d.dab), 1)                    AS avg_dab,
        ROUND(AVG(d.dab) / m.mab * 100, 2)      AS stickiness_pct
    FROM monthly m
    JOIN daily d ON m.year = d.year AND m.month = d.month
    GROUP BY m.year, m.month, m.mab
    ORDER BY m.year, m.month
""").show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Average Order Value (AOV) by Month and Channel

# COMMAND ----------

spark.sql(f"""
    SELECT
        d.year,
        d.month_name,
        c.channel,
        COUNT(DISTINCT f.order_number)                    AS orders,
        ROUND(SUM(f.total_amount), 2)                     AS revenue,
        ROUND(SUM(f.total_amount)
            / COUNT(DISTINCT f.order_number), 2)          AS aov
    FROM {catalog}.gold.fact_orders  f
    JOIN {catalog}.gold.dim_date     d ON f.order_date_key = d.date_key
    JOIN {catalog}.gold.dim_customer c
        ON f.customer_key = c.customer_key AND c.is_current = true
    WHERE c.source_customer_id != -1
    GROUP BY d.year, d.month, d.month_name, c.channel
    ORDER BY d.year, d.month, aov DESC
""").show(30)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer Cohort Retention
# MAGIC
# MAGIC Group customers by their first-purchase month (acquisition cohort).
# MAGIC Then measure how many return in subsequent months.

# COMMAND ----------

spark.sql(f"""
    WITH first_purchase AS (
        SELECT
            f.customer_key,
            MIN(d.year_month) AS cohort_month
        FROM {catalog}.gold.fact_orders f
        JOIN {catalog}.gold.dim_date    d ON f.order_date_key = d.date_key
        WHERE f.customer_key != 'unknown'
        GROUP BY f.customer_key
    ),
    activity AS (
        SELECT DISTINCT
            f.customer_key,
            d.year_month AS activity_month
        FROM {catalog}.gold.fact_orders f
        JOIN {catalog}.gold.dim_date    d ON f.order_date_key = d.date_key
        WHERE f.customer_key != 'unknown'
    )
    SELECT
        fp.cohort_month,
        a.activity_month,
        COUNT(DISTINCT a.customer_key)           AS active_buyers,
        COUNT(DISTINCT fp.customer_key)          AS cohort_size,
        ROUND(COUNT(DISTINCT a.customer_key)
            / COUNT(DISTINCT fp.customer_key) * 100, 1) AS retention_pct
    FROM first_purchase fp
    JOIN activity a ON fp.customer_key = a.customer_key
    GROUP BY fp.cohort_month, a.activity_month
    ORDER BY fp.cohort_month, a.activity_month
    LIMIT 30
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Return Rate by Category
# MAGIC
# MAGIC Return rate = returns / gross sales. Uses conformed dim_product across both fact tables.

# COMMAND ----------

spark.sql(f"""
    SELECT
        p.category,
        ROUND(SUM(o.total_amount), 2)                    AS gross_sales,
        ROUND(COALESCE(SUM(r.return_amount), 0), 2)      AS returns,
        ROUND(COALESCE(SUM(r.return_amount), 0)
            / SUM(o.total_amount) * 100, 1)              AS return_rate_pct,
        COUNT(DISTINCT o.order_number)                   AS orders,
        COUNT(DISTINCT r.return_key)                     AS returns_count
    FROM {catalog}.gold.dim_product  p
    LEFT JOIN {catalog}.gold.fact_orders  o ON p.product_key = o.product_key
    LEFT JOIN {catalog}.gold.fact_returns r ON p.product_key = r.product_key
    WHERE p.category != 'Unknown'
    GROUP BY p.category
    ORDER BY return_rate_pct DESC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product-Sense Interview Template
# MAGIC
# MAGIC When asked to design a data model for a product in an interview, use this structure:
# MAGIC
# MAGIC ```
# MAGIC Step 1 — Clarify the business process
# MAGIC   "Is this for a mobile ride-hailing app or dispatch system?"
# MAGIC   "Are we modeling ride requests or completed rides?"
# MAGIC
# MAGIC Step 2 — Define metrics first
# MAGIC   "The key metrics I'd want are: completion rate, average fare, driver utilization,
# MAGIC    and DAU for riders. Let me design the model to compute those."
# MAGIC
# MAGIC Step 3 — Declare the grain
# MAGIC   "One row per completed ride. Cancelled rides go into a separate fact table."
# MAGIC
# MAGIC Step 4 — List dimensions (who, what, when, where)
# MAGIC   "dim_user (rider + driver — same table, role-playing keys in fact),
# MAGIC    dim_vehicle, dim_location (pickup + dropoff — role-playing),
# MAGIC    dim_date (shared calendar)"
# MAGIC
# MAGIC Step 5 — List measures
# MAGIC   "distance_miles (additive), duration_minutes (additive),
# MAGIC    total_fare (additive), surge_multiplier (non-additive — use AVG not SUM)"
# MAGIC
# MAGIC Step 6 — Write SQL to prove the model works
# MAGIC   SELECT d.full_date, COUNT(DISTINCT rider_key) AS dau
# MAGIC   FROM fact_rides JOIN dim_date ON ...
# MAGIC   GROUP BY d.full_date
# MAGIC ```
