# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — fact_returns
# MAGIC
# MAGIC **Grain:** One row per return transaction.
# MAGIC
# MAGIC **Concept: Conformed Dimensions**
# MAGIC `dim_customer`, `dim_product`, and `dim_date` are shared unchanged from `fact_orders`.
# MAGIC This is what makes cross-subject analysis possible — the dimensions are conformed.
# MAGIC A customer or product means the same thing in both fact tables.

# COMMAND ----------

catalog = "workspace"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create fact_returns table

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.fact_returns (
        return_key         STRING   NOT NULL,
        customer_key       STRING,
        product_key        STRING,
        return_date_key    INT,
        order_number       STRING,
        return_reason      STRING,
        quantity_returned  INT,
        return_amount      DOUBLE
    )
    USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load fact_returns
# MAGIC
# MAGIC Same lookup join pattern as fact_orders, same conformed dimensions.
# MAGIC `dim_date` is joined once here (return_date only).

# COMMAND ----------

spark.sql(f"TRUNCATE TABLE {catalog}.gold.fact_returns")

spark.sql(f"""
    INSERT INTO {catalog}.gold.fact_returns
    SELECT
        md5(cast(src.return_id as string))              AS return_key,

        coalesce(dc.customer_key,  'unknown')           AS customer_key,
        coalesce(dp.product_key,   'unknown')           AS product_key,
        dd.date_key                                     AS return_date_key,

        src.order_number,
        src.return_reason,
        cast(src.quantity_returned as int)              AS quantity_returned,
        cast(src.quantity_returned as int)
            * cast(src.unit_price as double)            AS return_amount

    FROM {catalog}.bronze.returns src

    LEFT JOIN {catalog}.gold.dim_customer dc
        ON  src.customer_id = dc.source_customer_id
        AND dc.is_current = true

    LEFT JOIN {catalog}.gold.dim_product dp
        ON  src.product_id = dp.source_product_id

    LEFT JOIN {catalog}.gold.dim_date dd
        ON  to_date(src.return_date) = dd.full_date
""")

count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {catalog}.gold.fact_returns").collect()[0]["cnt"]
print(f"fact_returns loaded: {count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview

# COMMAND ----------

spark.sql(f"""
    SELECT
        r.return_key,
        r.order_number,
        c.first_name || ' ' || c.last_name  AS customer,
        p.product_name,
        p.category,
        d.full_date                          AS return_date,
        r.return_reason,
        r.quantity_returned,
        r.return_amount
    FROM {catalog}.gold.fact_returns r
    JOIN {catalog}.gold.dim_customer c
        ON r.customer_key = c.customer_key AND c.is_current = true
    JOIN {catalog}.gold.dim_product  p ON r.product_key    = p.product_key
    JOIN {catalog}.gold.dim_date     d ON r.return_date_key = d.date_key
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales vs Returns — conformed dimension payoff
# MAGIC
# MAGIC Because both fact tables use the same `dim_product`, we can compare them directly.

# COMMAND ----------

spark.sql(f"""
    SELECT
        p.category,
        ROUND(SUM(o.total_amount), 2)    AS gross_sales,
        ROUND(SUM(r.return_amount), 2)   AS total_returns,
        ROUND(SUM(o.total_amount)
            - SUM(r.return_amount), 2)   AS net_sales,
        ROUND(SUM(r.return_amount)
            / SUM(o.total_amount) * 100, 1) AS return_rate_pct
    FROM {catalog}.gold.dim_product p
    LEFT JOIN {catalog}.gold.fact_orders  o ON p.product_key = o.product_key
    LEFT JOIN {catalog}.gold.fact_returns r ON p.product_key = r.product_key
    WHERE p.category != 'Unknown'
    GROUP BY p.category
    ORDER BY gross_sales DESC
""").show()
