# Databricks notebook source

# MAGIC %md
# MAGIC # Building fact_orders — The Lookup Join Pattern
# MAGIC
# MAGIC **Notebook 14 of the Data Modeling Masterclass**
# MAGIC
# MAGIC This notebook builds `fact_orders` — the central table in our star schema. Everything we've built so far (four dimensions, surrogate keys, SCD2) leads to this moment. The design decisions made here determine the quality and usability of every report and dashboard that depends on this data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_orders Is the Centerpiece
# MAGIC
# MAGIC The fact table is where business events are recorded. Every time a customer places an order, a row is written here. Analysts use this table to answer questions like:
# MAGIC - Total revenue by category this quarter
# MAGIC - Top 10 customers by lifetime value
# MAGIC - Average order value by acquisition channel
# MAGIC - Regional sales performance vs. last year
# MAGIC
# MAGIC **Grain declared:** One row per order line item — the most atomic unit of a purchase event.
# MAGIC
# MAGIC Every foreign key in this table is a surrogate key resolved from a dimension table. Raw source IDs never appear in this table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Lookup Join Pattern
# MAGIC
# MAGIC When loading facts, we JOIN to every dimension at load time to resolve surrogate keys. This is called the **Lookup Join Pattern**:
# MAGIC
# MAGIC ```
# MAGIC source: bronze.orders has customer_id = 42
# MAGIC dim_customer has customer_id=42 → customer_key='abc123'
# MAGIC fact_orders stores customer_key='abc123'
# MAGIC ```
# MAGIC
# MAGIC Why do this at load time instead of query time?
# MAGIC - **Performance:** The join runs once at load. Every subsequent query joins on a pre-resolved hash key — much faster.
# MAGIC - **Consistency:** All downstream queries use the same resolved key. No risk of different queries resolving keys differently.
# MAGIC - **SCD correctness:** We capture the CURRENT version of the customer at load time. If the customer moves to a new city next month, this order still points to the Austin version of the customer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Role-Playing Dimensions
# MAGIC
# MAGIC Notice that `dim_date` is joined TWICE in our fact table:
# MAGIC - Once as `dd_order` to get `order_date_key`
# MAGIC - Once as `dd_ship` to get `ship_date_key`
# MAGIC
# MAGIC The same physical table plays two different **roles** in the same query. This is a **role-playing dimension**. Common examples:
# MAGIC - A single `dim_date` used for order_date, ship_date, return_date, due_date
# MAGIC - A single `dim_location` used for ship_from_location and ship_to_location
# MAGIC - A single `dim_employee` used for salesperson and manager
# MAGIC
# MAGIC You don't create duplicate dimension tables. You simply alias the JOIN twice (or more) in your SQL.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Degenerate Dimensions
# MAGIC
# MAGIC `order_number` lives directly in the fact table with no corresponding dimension table. Why?
# MAGIC
# MAGIC An order number is just a label. There are no other attributes to store about it. If you created `dim_order_number`, it would have exactly one column (the order number itself) and no lookup value.
# MAGIC
# MAGIC **Degenerate dimensions** are identifiers that carry no additional descriptive information. They come straight from the source into the fact table. They're useful for:
# MAGIC - Drilling to the source transaction
# MAGIC - Grouping line items that belong to the same order
# MAGIC - Operational lookups ("show me everything on order ORD-20240115-001")

# COMMAND ----------

# MAGIC %md
# MAGIC ## LEFT JOIN — Never Lose a Row
# MAGIC
# MAGIC Every dimension join uses `LEFT JOIN`, not `INNER JOIN`. Here's why this matters:
# MAGIC
# MAGIC If we used `INNER JOIN` and any source row had a `product_id` that doesn't exist in `dim_product`, that row would be **silently dropped** from the fact table. Your revenue total would be wrong, and you wouldn't know why.
# MAGIC
# MAGIC With `LEFT JOIN + COALESCE`:
# MAGIC ```sql
# MAGIC LEFT JOIN dim_product dp ON src.product_id = dp.source_product_id
# MAGIC COALESCE(dp.product_key, 'unknown') AS product_key
# MAGIC ```
# MAGIC The row is preserved. The product key falls to `'unknown'`. The data quality issue is visible in the "Unknown" category of any product report. A data engineer can investigate and fix it. **No data is lost.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Ensure the Table Exists
# MAGIC
# MAGIC This was created in notebook 13, but we include `CREATE TABLE IF NOT EXISTS` here to make this notebook self-contained and runnable standalone.

# COMMAND ----------

catalog = "workspace"

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

print(f"fact_orders table ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load — The Full Lookup Join
# MAGIC
# MAGIC This is the complete load query. Read it carefully — every element here implements a concept we've covered:
# MAGIC
# MAGIC - `md5(concat_ws(...))` — surrogate key for the fact row itself (makes the row uniquely addressable)
# MAGIC - `COALESCE(dc.customer_key, 'unknown')` — LEFT JOIN fallback to unknown member
# MAGIC - `dd_order` and `dd_ship` — role-playing dim_date (two aliases of the same table)
# MAGIC - `src.order_number` — degenerate dimension (stored as-is, no lookup)
# MAGIC - `(quantity * unit_price) - discount_amount` — derived additive measure computed at load time
# MAGIC
# MAGIC We TRUNCATE before INSERT to make this a full reload. For incremental loads (production pattern), you would use MERGE with `order_line_key` as the match key.

# COMMAND ----------

spark.sql(f"TRUNCATE TABLE {catalog}.gold.fact_orders")

spark.sql(f"""
    INSERT INTO {catalog}.gold.fact_orders
    SELECT
        md5(concat_ws('|',
            cast(src.order_id    as string),
            cast(src.line_item_id as string)
        ))                                                  AS order_line_key,

        -- Dimension foreign keys (resolved via lookup joins)
        coalesce(dc.customer_key, 'unknown')                AS customer_key,
        coalesce(dp.product_key,  'unknown')                AS product_key,
        coalesce(dl.location_key, 'unknown')                AS location_key,

        -- Role-playing dim_date (same table, two roles)
        dd_order.date_key                                   AS order_date_key,
        dd_ship.date_key                                    AS ship_date_key,

        -- Degenerate dimension (no dim table — just the identifier)
        src.order_number,

        -- Measures
        cast(src.quantity as int)                           AS quantity,
        cast(src.unit_price as double)                      AS unit_price,
        coalesce(cast(src.discount_amount as double), 0.0)  AS discount_amount,
        (cast(src.quantity as int) * cast(src.unit_price as double))
            - coalesce(cast(src.discount_amount as double), 0.0) AS total_amount

    FROM {catalog}.bronze.orders src

    -- Lookup join 1: dim_customer (SCD2 — must filter is_current=true)
    LEFT JOIN {catalog}.gold.dim_customer dc
        ON src.customer_id = dc.source_customer_id
        AND dc.is_current = true

    -- Lookup join 2: dim_product
    LEFT JOIN {catalog}.gold.dim_product dp
        ON src.product_id = dp.source_product_id

    -- Lookup join 3: dim_location
    LEFT JOIN {catalog}.gold.dim_location dl
        ON src.location_id = dl.source_location_id

    -- Lookup join 4a: dim_date for order date (role: order date)
    LEFT JOIN {catalog}.gold.dim_date dd_order
        ON to_date(src.order_date) = dd_order.full_date

    -- Lookup join 4b: dim_date for ship date (role: ship date — same table, different alias)
    LEFT JOIN {catalog}.gold.dim_date dd_ship
        ON to_date(src.ship_date) = dd_ship.full_date
""")

print("fact_orders loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Row Count and Completeness Check
# MAGIC
# MAGIC Always verify that the fact table row count matches the source. Any discrepancy means rows were dropped — investigate immediately.
# MAGIC
# MAGIC We also check for `NULL` values in key columns (null keys indicate a lookup join found no match AND no unknown member was available — this would be a bug).

# COMMAND ----------

bronze_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {catalog}.bronze.orders").collect()[0]["cnt"]
gold_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {catalog}.gold.fact_orders").collect()[0]["cnt"]

print(f"Bronze source rows:        {bronze_count:,}")
print(f"fact_orders rows:          {gold_count:,}")
print(f"Row preservation:          {'✓ PERFECT MATCH' if bronze_count == gold_count else '✗ MISMATCH — investigate!'}")

# COMMAND ----------

# Check for any NULL keys — these would indicate a broken lookup
null_check = spark.sql(f"""
    SELECT
        SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) AS null_customer_keys,
        SUM(CASE WHEN product_key  IS NULL THEN 1 ELSE 0 END) AS null_product_keys,
        SUM(CASE WHEN location_key IS NULL THEN 1 ELSE 0 END) AS null_location_keys,
        SUM(CASE WHEN order_date_key IS NULL THEN 1 ELSE 0 END) AS null_order_date_keys
    FROM {catalog}.gold.fact_orders
""")
null_check.show(truncate=False)

# COMMAND ----------

# Check how many rows fell to 'unknown' keys — should ideally be 0 for good data
unknown_check = spark.sql(f"""
    SELECT
        SUM(CASE WHEN customer_key = 'unknown' THEN 1 ELSE 0 END) AS unknown_customers,
        SUM(CASE WHEN product_key  = 'unknown' THEN 1 ELSE 0 END) AS unknown_products,
        SUM(CASE WHEN location_key = 'unknown' THEN 1 ELSE 0 END) AS unknown_locations
    FROM {catalog}.gold.fact_orders
""")
print("Rows that fell to unknown member (data quality indicator):")
unknown_check.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Rich Preview — Join Back to Dimensions
# MAGIC
# MAGIC The fact table alone is not readable (just keys and numbers). Let's join back to the dimensions to see what the data actually represents. This is exactly what BI tools do when they build reports.

# COMMAND ----------

spark.sql(f"""
    SELECT
        fo.order_number,
        dc.first_name || ' ' || dc.last_name   AS customer_name,
        dc.channel,
        dp.product_name,
        dp.category,
        dl.city,
        dl.region,
        dd.full_date                            AS order_date,
        fo.quantity,
        fo.unit_price,
        fo.discount_amount,
        fo.total_amount
    FROM {catalog}.gold.fact_orders fo
    JOIN {catalog}.gold.dim_customer dc ON fo.customer_key = dc.customer_key
    JOIN {catalog}.gold.dim_product  dp ON fo.product_key  = dp.product_key
    JOIN {catalog}.gold.dim_location dl ON fo.location_key = dl.location_key
    JOIN {catalog}.gold.dim_date     dd ON fo.order_date_key = dd.date_key
    WHERE dc.is_current = true
    AND   dc.source_customer_id != -1
    AND   dp.product_key != 'unknown'
    ORDER BY dd.full_date DESC
    LIMIT 12
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Quick Sanity Aggregations
# MAGIC
# MAGIC Before declaring the fact table done, run a few aggregations to make sure the numbers look reasonable.

# COMMAND ----------

spark.sql(f"""
    SELECT
        dp.category,
        COUNT(fo.order_line_key)        AS line_items,
        SUM(fo.quantity)                AS total_units_sold,
        ROUND(SUM(fo.total_amount), 2)  AS total_revenue,
        ROUND(AVG(fo.total_amount), 2)  AS avg_line_item_value
    FROM {catalog}.gold.fact_orders fo
    JOIN {catalog}.gold.dim_product dp ON fo.product_key = dp.product_key
    WHERE dp.product_key != 'unknown'
    GROUP BY dp.category
    ORDER BY total_revenue DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Concept | Implementation |
# MAGIC |---------|---------------|
# MAGIC | Grain | One row per order line item |
# MAGIC | Surrogate key | `md5(order_id \| line_item_id)` |
# MAGIC | Lookup Join Pattern | LEFT JOIN to each dim, resolve key at load time |
# MAGIC | Role-playing dimension | `dim_date` joined twice: `order_date_key` and `ship_date_key` |
# MAGIC | Degenerate dimension | `order_number` stored directly (no dim table) |
# MAGIC | Unknown member | `COALESCE(key, 'unknown')` — no row dropped, no NULL key |
# MAGIC | SCD2 join | `AND dc.is_current = true` — captures current customer version |
# MAGIC | Derived measure | `total_amount = (quantity × unit_price) - discount` |
# MAGIC | Row preservation | Bronze count = Gold count — verified |
# MAGIC
# MAGIC **Next notebook:** `15_fact_returns.py` — Building fact_returns using the same dimensions (conformed dimensions). Learn why sharing dimensions across fact tables is one of the most powerful capabilities of the star schema.
