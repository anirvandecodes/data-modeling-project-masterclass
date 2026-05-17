# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — fact_orders
# MAGIC
# MAGIC **Grain:** One row per order line item (one product on one order).
# MAGIC
# MAGIC **Concepts demonstrated:**
# MAGIC - Lookup join pattern: surrogate keys resolved from dimension tables at load time
# MAGIC - Role-playing dimensions: `dim_date` aliased as `order_date` and `ship_date`
# MAGIC - Degenerate dimension: `order_number` stored directly in the fact (no dim table)
# MAGIC - Unknown member: LEFT JOIN so missing keys fall to 'unknown', never drop rows
# MAGIC - MD5 surrogate key on the fact itself for idempotent loads

# COMMAND ----------

catalog = "workspace"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create fact_orders table

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.fact_orders (
        order_line_key   STRING   NOT NULL,
        customer_key     STRING,
        product_key      STRING,
        location_key     STRING,
        order_date_key   INT,
        ship_date_key    INT,
        order_number     STRING,
        quantity         INT,
        unit_price       DOUBLE,
        discount_amount  DOUBLE,
        total_amount     DOUBLE
    )
    USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load fact_orders using the Lookup Join Pattern
# MAGIC
# MAGIC Every foreign key in the fact table is resolved by joining to its dimension.
# MAGIC `dim_date` is joined **twice** — once for `order_date`, once for `ship_date`.
# MAGIC This is the **role-playing dimension** pattern.
# MAGIC
# MAGIC `order_number` is a **degenerate dimension**: it's an identifier with no attributes,
# MAGIC so it lives directly in the fact table with no separate dimension table.

# COMMAND ----------

spark.sql(f"TRUNCATE TABLE {catalog}.gold.fact_orders")

spark.sql(f"""
    INSERT INTO {catalog}.gold.fact_orders
    SELECT
        md5(concat_ws('|',
            cast(src.order_id    as string),
            cast(src.line_item_id as string)
        ))                                          AS order_line_key,

        coalesce(dc.customer_key,  'unknown')       AS customer_key,
        coalesce(dp.product_key,   'unknown')       AS product_key,
        coalesce(dl.location_key,  'unknown')       AS location_key,

        dd_order.date_key                           AS order_date_key,
        dd_ship.date_key                            AS ship_date_key,

        src.order_number,

        cast(src.quantity       as int)             AS quantity,
        cast(src.unit_price     as double)          AS unit_price,
        coalesce(cast(src.discount_amount as double), 0.0)
                                                    AS discount_amount,
        (cast(src.quantity as int) * cast(src.unit_price as double))
            - coalesce(cast(src.discount_amount as double), 0.0)
                                                    AS total_amount

    FROM {catalog}.bronze.orders src

    LEFT JOIN {catalog}.gold.dim_customer dc
        ON  src.customer_id = dc.source_customer_id
        AND dc.is_current = true

    LEFT JOIN {catalog}.gold.dim_product dp
        ON  src.product_id = dp.source_product_id

    LEFT JOIN {catalog}.gold.dim_location dl
        ON  src.location_id = dl.source_location_id

    LEFT JOIN {catalog}.gold.dim_date dd_order
        ON  to_date(src.order_date) = dd_order.full_date

    LEFT JOIN {catalog}.gold.dim_date dd_ship
        ON  to_date(src.ship_date) = dd_ship.full_date
""")

count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {catalog}.gold.fact_orders").collect()[0]["cnt"]
print(f"fact_orders loaded: {count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview

# COMMAND ----------

spark.sql(f"""
    SELECT
        f.order_line_key,
        f.order_number,
        c.first_name || ' ' || c.last_name  AS customer,
        p.product_name,
        p.category,
        l.city,
        d_order.full_date                   AS order_date,
        d_ship.full_date                    AS ship_date,
        f.quantity,
        f.unit_price,
        f.discount_amount,
        f.total_amount
    FROM {catalog}.gold.fact_orders f
    JOIN {catalog}.gold.dim_customer c
        ON f.customer_key = c.customer_key AND c.is_current = true
    JOIN {catalog}.gold.dim_product  p  ON f.product_key  = p.product_key
    JOIN {catalog}.gold.dim_location l  ON f.location_key = l.location_key
    JOIN {catalog}.gold.dim_date d_order ON f.order_date_key = d_order.date_key
    JOIN {catalog}.gold.dim_date d_ship  ON f.ship_date_key  = d_ship.date_key
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify no dropped rows
# MAGIC
# MAGIC A LEFT JOIN means missing dim keys fall to 'unknown', not NULL.
# MAGIC This confirms no orders were silently lost.

# COMMAND ----------

spark.sql(f"""
    SELECT
        COUNT(*)                                                      AS total_lines,
        SUM(CASE WHEN customer_key = 'unknown' THEN 1 ELSE 0 END)    AS unknown_customers,
        SUM(CASE WHEN product_key  = 'unknown' THEN 1 ELSE 0 END)    AS unknown_products,
        SUM(CASE WHEN location_key = 'unknown' THEN 1 ELSE 0 END)    AS unknown_locations
    FROM {catalog}.gold.fact_orders
""").show()
