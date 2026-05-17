# Practical Data Modeling Masterclass — Single Video Plan

## Overview

- **Format:** Single long-form video (~120–130 min)
- **Domain:** E-commerce / Retail
- **Platform:** Databricks (concepts are cloud-agnostic)
- **Audience:** Beginners and Intermediate
- **Modeling Paradigm:** Dimensional Modeling (Kimball)
- **Architecture:** Medallion (Bronze → Silver → Gold)
- **Surrogate Keys:** Hash-based (MD5)
- **SCDs:** Type 1 (overwrite) + Type 2 (full history) — both demonstrated

---

## Final Schema

```
dim_date ──────────────────────────────────────────────────────┐
  (order_date_key, ship_date_key — role-playing)               │
                                                               │
dim_customer (SCD Type 2) ──────── fact_orders ──── dim_product
                                    │  degenerate: order_number
dim_location ───────────────────────┘

dim_customer (conformed) ────────── fact_returns ─── dim_product (conformed)
dim_date     (conformed) ───────────┘
```

---

## Video Structure

| Segment | Topic | Time |
|---------|-------|------|
| 1 | Intro — what we're building and why | 5 min |
| 2 | Foundations — OLTP vs OLAP, star schema, medallion | 10 min |
| 3 | Setup — catalog, schema, volumes | 5 min |
| 4 | Bronze — ingest raw CSVs to Delta | 5 min |
| 5 | `dim_date` — build with all flags | 10 min |
| 6 | `dim_product` — MD5 surrogate key, hierarchies | 10 min |
| 7 | `dim_location` — MD5 surrogate key | 5 min |
| 8 | `dim_customer` SCD Type 1 — MERGE overwrite | 10 min |
| 9 | `dim_customer` SCD Type 2 — full history, is_current | 15 min |
| 10 | `fact_orders` — grain, lookup join, role-playing, degenerate dim | 15 min |
| 11 | `fact_returns` — conformed dimensions payoff | 10 min |
| 12 | Orchestration — load order, Databricks Workflow DAG | 10 min |
| 13 | Analytics — answer all 4 business questions | 10 min |
| 14 | Wrap-up — recap, cloud-agnostic note | 5 min |
| **Total** | | **~125 min** |

---

## Business Questions (answered at the end)

1. What are total sales by product category per month?
2. Which customers are our top buyers?
3. How do sales vary by geography?
4. What is the average order value by channel?

---

## Folder Structure

```
data-modeling-project-masterclass/
├── MASTERCLASS_PLAN.md
├── data/
│   └── raw/
│       ├── customers.csv
│       ├── products.csv
│       ├── locations.csv
│       ├── orders.csv
│       └── returns.csv
├── notebooks/
│   ├── 00_setup.py
│   ├── 01_bronze_ingest.py
│   ├── 02_dim_date.py
│   ├── 03_dim_product.py
│   ├── 04_dim_location.py
│   ├── 05a_dim_customer_scd1.py
│   ├── 05b_dim_customer_scd2.py
│   ├── 06_fact_orders.py
│   ├── 07_fact_returns.py
│   └── 08_analytics_queries.py
└── workflow/
    └── pipeline_definition.yml
```

---

## Segment 1 — Intro (5 min)

**Talking points:**
- We're building a production-style data model from scratch on Databricks
- Domain: an e-commerce company with customers, products, orders, and returns
- By the end: a complete star schema that answers real business questions with clean SQL
- Show the four business questions upfront so the viewer has a goal to chase

---

## Segment 2 — Foundations (10 min)

**Talking points:**
- OLTP (row-optimized, normalized, write-fast) vs OLAP (column-optimized, denormalized, read-fast)
- The Medallion architecture: Bronze (raw) → Silver (cleaned) → Gold (modeled)
- Star schema: one fact table at the center, dimension tables radiating out
- Key rules: declare the grain, keep facts narrow, make dimensions wide and readable
- Whiteboard the final schema before touching any code

---

## Segment 3 — Setup: `00_setup.py` (5 min)

```python
# Create catalog, schemas, and volume
catalog = "masterclass"
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.bronze.raw_data")

print("Catalog, schemas, and volume ready.")
```

---

## Segment 4 — Bronze Ingest: `01_bronze_ingest.py` (5 min)

**Concept:** Land raw CSVs as-is into Bronze Delta tables. No transformations — just durability.

```python
from pyspark.sql import SparkSession

catalog = "masterclass"
volume_path = f"/Volumes/{catalog}/bronze/raw_data"

tables = ["customers", "products", "locations", "orders", "returns"]

for table in tables:
    df = spark.read.option("header", True).option("inferSchema", True) \
             .csv(f"{volume_path}/{table}.csv")
    df.write.format("delta").mode("overwrite") \
      .saveAsTable(f"{catalog}.bronze.{table}")
    print(f"Loaded {table}: {df.count()} rows")
```

---

## Segment 5 — Date Dimension: `02_dim_date.py` (10 min)

**Concept:** Never store raw dates in fact tables. Build a date dim once, reference it everywhere.

**Columns:** `date_key`, `full_date`, `year`, `quarter`, `month`, `month_name`, `week_of_year`,
`day_of_month`, `day_of_week`, `day_name`, `is_weekend`, `is_month_end`, `is_quarter_end`,
`fiscal_year`, `fiscal_quarter`

```python
from pyspark.sql.functions import (
    explode, sequence, to_date, col, year, quarter, month, date_format,
    weekofyear, dayofmonth, dayofweek, last_day, when, date_trunc, lit,
    date_format, expr
)
from pyspark.sql.types import DateType

catalog = "masterclass"

# Generate one row per date for the full range of the dataset
date_range = spark.sql("""
    SELECT explode(sequence(
        to_date('2020-01-01'),
        to_date('2025-12-31'),
        interval 1 day
    )) AS full_date
""")

dim_date = date_range.select(
    date_format(col("full_date"), "yyyyMMdd").cast("int").alias("date_key"),
    col("full_date"),
    year("full_date").alias("year"),
    quarter("full_date").alias("quarter"),
    month("full_date").alias("month"),
    date_format(col("full_date"), "MMMM").alias("month_name"),
    weekofyear("full_date").alias("week_of_year"),
    dayofmonth("full_date").alias("day_of_month"),
    dayofweek("full_date").alias("day_of_week"),
    date_format(col("full_date"), "EEEE").alias("day_name"),
    when(dayofweek("full_date").isin(1, 7), True).otherwise(False).alias("is_weekend"),
    when(col("full_date") == last_day(col("full_date")), True).otherwise(False).alias("is_month_end"),
    when(
        (month("full_date").isin(3, 6, 9, 12)) & (col("full_date") == last_day(col("full_date"))),
        True
    ).otherwise(False).alias("is_quarter_end"),
    year("full_date").alias("fiscal_year"),
    quarter("full_date").alias("fiscal_quarter")
)

dim_date.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{catalog}.gold.dim_date")

print(f"dim_date built: {dim_date.count()} rows")
spark.sql(f"SELECT * FROM {catalog}.gold.dim_date LIMIT 5").show()
```

---

## Segment 6 — Product Dimension: `03_dim_product.py` (10 min)

**Concept:** MD5 surrogate key — deterministic, distributed-friendly, no sequence generator needed.
Wide table with natural hierarchy: `category → subcategory → product_name → sku`.

```python
from pyspark.sql.functions import md5, concat_ws, col, current_timestamp, lit

catalog = "masterclass"

silver_products = spark.table(f"{catalog}.bronze.products").select(
    col("product_id").alias("source_product_id"),
    col("sku"),
    col("product_name"),
    col("category"),
    col("subcategory"),
    col("brand"),
    col("unit_price"),
    col("cost_price")
)

# MD5 surrogate key — hash the natural business key
dim_product = silver_products.withColumn(
    "product_key",
    md5(concat_ws("|", col("source_product_id").cast("string"), col("sku")))
).select(
    "product_key",
    "source_product_id",
    "sku",
    "product_name",
    "category",
    "subcategory",
    "brand",
    "unit_price",
    "cost_price"
)

# Unknown member — handles late-arriving facts that reference a missing product
from pyspark.sql import Row
unknown = spark.createDataFrame([Row(
    product_key="unknown",
    source_product_id=-1,
    sku="UNKNOWN",
    product_name="Unknown Product",
    category="Unknown",
    subcategory="Unknown",
    brand="Unknown",
    unit_price=0.0,
    cost_price=0.0
)])

dim_product = unknown.union(dim_product)

dim_product.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{catalog}.gold.dim_product")

print(f"dim_product built: {dim_product.count()} rows")
spark.sql(f"SELECT product_key, sku, product_name, category FROM {catalog}.gold.dim_product LIMIT 5").show()
```

---

## Segment 7 — Location Dimension: `04_dim_location.py` (5 min)

**Concept:** Same MD5 pattern, fast to build — no SCD needed (locations rarely change meaningfully).

```python
from pyspark.sql.functions import md5, concat_ws, col
from pyspark.sql import Row

catalog = "masterclass"

bronze_locations = spark.table(f"{catalog}.bronze.locations").select(
    col("location_id").alias("source_location_id"),
    col("city"),
    col("state"),
    col("country"),
    col("region"),
    col("postal_code")
)

dim_location = bronze_locations.withColumn(
    "location_key",
    md5(concat_ws("|", col("source_location_id").cast("string")))
).select(
    "location_key",
    "source_location_id",
    "city",
    "state",
    "country",
    "region",
    "postal_code"
)

unknown = spark.createDataFrame([Row(
    location_key="unknown",
    source_location_id=-1,
    city="Unknown",
    state="Unknown",
    country="Unknown",
    region="Unknown",
    postal_code="00000"
)])

dim_location = unknown.union(dim_location)

dim_location.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{catalog}.gold.dim_location")

print(f"dim_location built: {dim_location.count()} rows")
spark.sql(f"SELECT * FROM {catalog}.gold.dim_location LIMIT 5").show()
```

---

## Segment 8 — SCD Type 1: `05a_dim_customer_scd1.py` (10 min)

**Concept:** Type 1 = overwrite. Use when history does not matter — e.g., fixing a typo in an email
address or phone number. Old value is gone forever. Simple MERGE.

**When to use:** Non-analytical attributes. Corrections. Reference data.

```python
catalog = "masterclass"

# Create the table on first run
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_customer_scd1 (
        customer_key      STRING,
        source_customer_id INT,
        first_name        STRING,
        last_name         STRING,
        email             STRING,
        phone             STRING,
        address           STRING,
        city              STRING,
        state             STRING,
        country           STRING,
        channel           STRING
    )
    USING DELTA
""")

# Load incoming data from Bronze
incoming = spark.table(f"{catalog}.bronze.customers").selectExpr(
    "md5(cast(customer_id as string)) AS customer_key",
    "customer_id AS source_customer_id",
    "first_name", "last_name", "email", "phone",
    "address", "city", "state", "country", "channel"
)

incoming.createOrReplaceTempView("incoming_customers")

# MERGE: update existing rows in-place, insert new ones
spark.sql(f"""
    MERGE INTO {catalog}.gold.dim_customer_scd1 AS target
    USING incoming_customers AS source
    ON target.source_customer_id = source.source_customer_id
    WHEN MATCHED THEN UPDATE SET
        target.customer_key       = source.customer_key,
        target.first_name         = source.first_name,
        target.last_name          = source.last_name,
        target.email              = source.email,
        target.phone              = source.phone,
        target.address            = source.address,
        target.city               = source.city,
        target.state              = source.state,
        target.country            = source.country,
        target.channel            = source.channel
    WHEN NOT MATCHED THEN INSERT *
""")

print("dim_customer_scd1 loaded")
spark.sql(f"SELECT * FROM {catalog}.gold.dim_customer_scd1 LIMIT 5").show()
```

**Demo moment:** Run once, then modify an email in the source CSV, run again — show the old value is gone.

---

## Segment 9 — SCD Type 2: `05b_dim_customer_scd2.py` (15 min)

**Concept:** Type 2 = full history. Every change creates a new row. `start_date`, `end_date`,
`is_current` flag track which version is active. Natural key + `is_current = true` always
points to the latest record.

**When to use:** Anything you need to report "as of" a point in time — customer address,
customer segment, account tier.

```python
from pyspark.sql.functions import md5, concat_ws, col, lit, current_date, to_date
from pyspark.sql import Row

catalog = "masterclass"

# Create the SCD Type 2 table with history columns
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_customer (
        customer_key       STRING,
        source_customer_id INT,
        first_name         STRING,
        last_name          STRING,
        email              STRING,
        phone              STRING,
        address            STRING,
        city               STRING,
        state              STRING,
        country            STRING,
        channel            STRING,
        start_date         DATE,
        end_date           DATE,
        is_current         BOOLEAN
    )
    USING DELTA
""")

# Insert the unknown member row (surrogate = 'unknown', is_current = true always)
spark.sql(f"""
    MERGE INTO {catalog}.gold.dim_customer AS target
    USING (SELECT 'unknown' AS customer_key) AS src
    ON target.customer_key = src.customer_key
    WHEN NOT MATCHED THEN INSERT (
        customer_key, source_customer_id, first_name, last_name,
        email, phone, address, city, state, country, channel,
        start_date, end_date, is_current
    ) VALUES (
        'unknown', -1, 'Unknown', 'Unknown',
        'unknown@unknown.com', '000-000-0000', 'Unknown', 'Unknown',
        'Unknown', 'Unknown', 'Unknown',
        '1900-01-01', '9999-12-31', true
    )
""")

# Load incoming source data
incoming = spark.table(f"{catalog}.bronze.customers").selectExpr(
    "md5(cast(customer_id as string)) AS customer_key",
    "customer_id AS source_customer_id",
    "first_name", "last_name", "email", "phone",
    "address", "city", "state", "country", "channel"
)

incoming.createOrReplaceTempView("incoming_customers")

# Step 1: Expire rows where a tracked attribute has changed (close old version)
spark.sql(f"""
    MERGE INTO {catalog}.gold.dim_customer AS target
    USING incoming_customers AS source
    ON target.source_customer_id = source.source_customer_id
       AND target.is_current = true
    WHEN MATCHED AND (
        target.address != source.address OR
        target.city    != source.city    OR
        target.state   != source.state   OR
        target.country != source.country OR
        target.channel != source.channel
    )
    THEN UPDATE SET
        target.end_date   = current_date(),
        target.is_current = false
""")

# Step 2: Insert new current version for changed + brand-new customers
spark.sql(f"""
    MERGE INTO {catalog}.gold.dim_customer AS target
    USING (
        SELECT
            src.customer_key,
            src.source_customer_id,
            src.first_name, src.last_name, src.email, src.phone,
            src.address, src.city, src.state, src.country, src.channel,
            current_date() AS start_date,
            to_date('9999-12-31') AS end_date,
            true AS is_current
        FROM incoming_customers src
        LEFT JOIN {catalog}.gold.dim_customer tgt
            ON src.source_customer_id = tgt.source_customer_id
           AND tgt.is_current = true
        WHERE tgt.source_customer_id IS NULL
           OR src.address != tgt.address
           OR src.city    != tgt.city
           OR src.state   != tgt.state
           OR src.country != tgt.country
           OR src.channel != tgt.channel
    ) AS new_rows
    ON target.source_customer_id = new_rows.source_customer_id
       AND target.start_date = new_rows.start_date
    WHEN NOT MATCHED THEN INSERT *
""")

print("dim_customer (SCD Type 2) loaded")
spark.sql(f"""
    SELECT source_customer_id, first_name, city, channel,
           start_date, end_date, is_current
    FROM {catalog}.gold.dim_customer
    ORDER BY source_customer_id, start_date
""").show(20)
```

**Demo moment:** Simulate a customer moving cities — change address in the source CSV, re-run.
Show two rows for the same customer: old row with `is_current = false`, new row with `is_current = true`.

---

## Segment 10 — Fact Orders: `06_fact_orders.py` (15 min)

**Concepts demonstrated:**
- Grain: one row per order line item
- Lookup join pattern: resolve all surrogate keys from dim tables
- Role-playing dimensions: `dim_date` aliased as both `order_date` and `ship_date`
- Degenerate dimension: `order_number` stored directly in the fact (no dim table needed)
- Measures: `quantity`, `unit_price`, `total_amount`, `discount_amount`

```python
catalog = "masterclass"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.fact_orders (
        order_line_key     STRING,
        customer_key       STRING,
        product_key        STRING,
        location_key       STRING,
        order_date_key     INT,
        ship_date_key      INT,
        order_number       STRING,
        quantity           INT,
        unit_price         DOUBLE,
        discount_amount    DOUBLE,
        total_amount       DOUBLE
    )
    USING DELTA
""")

spark.sql(f"""
    INSERT INTO {catalog}.gold.fact_orders
    SELECT
        md5(concat_ws('|',
            cast(src.order_id as string),
            cast(src.line_item_id as string)
        ))                                          AS order_line_key,

        coalesce(dc.customer_key,  'unknown')       AS customer_key,
        coalesce(dp.product_key,   'unknown')       AS product_key,
        coalesce(dl.location_key,  'unknown')       AS location_key,

        dd_order.date_key                           AS order_date_key,
        dd_ship.date_key                            AS ship_date_key,

        src.order_number,                           -- degenerate dimension

        src.quantity,
        src.unit_price,
        coalesce(src.discount_amount, 0.0)          AS discount_amount,
        src.quantity * src.unit_price
            - coalesce(src.discount_amount, 0.0)    AS total_amount

    FROM {catalog}.bronze.orders src

    LEFT JOIN {catalog}.gold.dim_customer dc
        ON src.customer_id = dc.source_customer_id
       AND dc.is_current = true

    LEFT JOIN {catalog}.gold.dim_product dp
        ON src.product_id = dp.source_product_id

    LEFT JOIN {catalog}.gold.dim_location dl
        ON src.location_id = dl.source_location_id

    LEFT JOIN {catalog}.gold.dim_date dd_order
        ON src.order_date = dd_order.full_date

    LEFT JOIN {catalog}.gold.dim_date dd_ship
        ON src.ship_date = dd_ship.full_date
""")

print("fact_orders loaded")
spark.sql(f"""
    SELECT order_line_key, customer_key, product_key,
           order_date_key, ship_date_key, order_number,
           quantity, unit_price, total_amount
    FROM {catalog}.gold.fact_orders LIMIT 5
""").show()
```

**Explain in video:**
- `LEFT JOIN` to dims so a missing key goes to `'unknown'` not dropped
- Two joins to `dim_date` with different aliases = role-playing dimension
- `order_number` has no dimension table — it lives in the fact as a degenerate dim

---

## Segment 11 — Fact Returns: `07_fact_returns.py` (10 min)

**Concept:** Conformed dimensions — `dim_customer`, `dim_product`, `dim_date` are reused
unchanged from `fact_orders`. This is what makes cross-subject queries possible.

```python
catalog = "masterclass"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.fact_returns (
        return_key         STRING,
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

spark.sql(f"""
    INSERT INTO {catalog}.gold.fact_returns
    SELECT
        md5(cast(src.return_id as string))          AS return_key,

        coalesce(dc.customer_key, 'unknown')        AS customer_key,
        coalesce(dp.product_key,  'unknown')        AS product_key,
        dd.date_key                                 AS return_date_key,

        src.order_number,
        src.return_reason,
        src.quantity_returned,
        src.quantity_returned * src.unit_price      AS return_amount

    FROM {catalog}.bronze.returns src

    LEFT JOIN {catalog}.gold.dim_customer dc
        ON src.customer_id = dc.source_customer_id
       AND dc.is_current = true

    LEFT JOIN {catalog}.gold.dim_product dp
        ON src.product_id = dp.source_product_id

    LEFT JOIN {catalog}.gold.dim_date dd
        ON src.return_date = dd.full_date
""")

print("fact_returns loaded")
spark.sql(f"SELECT * FROM {catalog}.gold.fact_returns LIMIT 5").show()
```

---

## Segment 12 — Orchestration (10 min)

### Why Load Order Matters

Fact tables resolve surrogate keys from dimension tables at load time.
Load a fact before its dims → NULL keys → broken analytics.

**Rule: always load dimensions before facts.**

```
[Bronze — raw CSVs]
        │
        ├──► dim_date        (no dependencies)
        ├──► dim_product     (no dependencies)
        ├──► dim_location    (no dependencies)
        └──► dim_customer    (SCD logic — Type 1 or Type 2)
                   │
             [All dims ready]
                   │
        ├──► fact_orders     (joins all 4 dims)
        └──► fact_returns    (joins all 4 dims)
```

### Load Sequence

| Step | Task | Depends On |
|------|------|------------|
| 1 | Bronze ingest | — |
| 2 | `dim_date` | Bronze |
| 3 | `dim_product` | Bronze |
| 3 | `dim_location` | Bronze |
| 4 | `dim_customer` | Bronze |
| 5 | `fact_orders` | All dims |
| 6 | `fact_returns` | All dims |

### Late-Arriving Dimensions

When a fact row arrives before its dimension record exists:
- The `LEFT JOIN` falls through to `'unknown'`
- The fact is not dropped — it's stored with `customer_key = 'unknown'`
- When the dimension record arrives later, a backfill job re-resolves the key

This is why every dimension has an **unknown member row** (`source_*_id = -1`).

### Databricks Workflow DAG (`workflow/pipeline_definition.yml`)

Define each notebook as a task. Set `depends_on` to enforce the load order visually in the UI.

```yaml
resources:
  jobs:
    data_modeling_pipeline:
      name: "Data Modeling Masterclass Pipeline"
      tasks:
        - task_key: bronze_ingest
          notebook_task:
            notebook_path: /notebooks/01_bronze_ingest
        - task_key: dim_date
          depends_on: [{ task_key: bronze_ingest }]
          notebook_task:
            notebook_path: /notebooks/02_dim_date
        - task_key: dim_product
          depends_on: [{ task_key: bronze_ingest }]
          notebook_task:
            notebook_path: /notebooks/03_dim_product
        - task_key: dim_location
          depends_on: [{ task_key: bronze_ingest }]
          notebook_task:
            notebook_path: /notebooks/04_dim_location
        - task_key: dim_customer
          depends_on: [{ task_key: bronze_ingest }]
          notebook_task:
            notebook_path: /notebooks/05b_dim_customer_scd2
        - task_key: fact_orders
          depends_on:
            - task_key: dim_date
            - task_key: dim_product
            - task_key: dim_location
            - task_key: dim_customer
          notebook_task:
            notebook_path: /notebooks/06_fact_orders
        - task_key: fact_returns
          depends_on:
            - task_key: dim_date
            - task_key: dim_product
            - task_key: dim_location
            - task_key: dim_customer
          notebook_task:
            notebook_path: /notebooks/07_fact_returns
```

---

## Segment 13 — Analytics: `08_analytics_queries.py` (10 min)

All four business questions answered with clean SQL on Gold tables only.

### Q1: Total sales by product category per month

```sql
SELECT
    d.year,
    d.month_name,
    p.category,
    SUM(f.total_amount)     AS total_sales,
    SUM(f.quantity)         AS units_sold
FROM masterclass.gold.fact_orders f
JOIN masterclass.gold.dim_date    d ON f.order_date_key = d.date_key
JOIN masterclass.gold.dim_product p ON f.product_key    = p.product_key
GROUP BY d.year, d.month, d.month_name, p.category
ORDER BY d.year, d.month, total_sales DESC
```

### Q2: Top customers by lifetime value

```sql
SELECT
    c.first_name,
    c.last_name,
    c.channel,
    COUNT(DISTINCT f.order_number) AS total_orders,
    SUM(f.total_amount)            AS lifetime_value,
    AVG(f.total_amount)            AS avg_order_value
FROM masterclass.gold.fact_orders  f
JOIN masterclass.gold.dim_customer c ON f.customer_key = c.customer_key AND c.is_current = true
GROUP BY c.first_name, c.last_name, c.channel
ORDER BY lifetime_value DESC
LIMIT 20
```

### Q3: Sales by geography

```sql
SELECT
    l.country,
    l.state,
    l.region,
    SUM(f.total_amount)  AS total_sales,
    COUNT(*)             AS order_lines
FROM masterclass.gold.fact_orders  f
JOIN masterclass.gold.dim_location l ON f.location_key = l.location_key
GROUP BY l.country, l.state, l.region
ORDER BY total_sales DESC
```

### Q4: Average order value by channel

```sql
SELECT
    c.channel,
    COUNT(DISTINCT f.order_number)  AS total_orders,
    SUM(f.total_amount)             AS total_revenue,
    SUM(f.total_amount)
        / COUNT(DISTINCT f.order_number) AS avg_order_value
FROM masterclass.gold.fact_orders  f
JOIN masterclass.gold.dim_customer c ON f.customer_key = c.customer_key AND c.is_current = true
GROUP BY c.channel
ORDER BY avg_order_value DESC
```

### Bonus: Sales vs Returns by product category

```sql
SELECT
    p.category,
    SUM(o.total_amount)     AS gross_sales,
    SUM(r.return_amount)    AS total_returns,
    SUM(o.total_amount)
        - SUM(r.return_amount) AS net_sales,
    ROUND(SUM(r.return_amount)
        / SUM(o.total_amount) * 100, 2) AS return_rate_pct
FROM masterclass.gold.dim_product p
LEFT JOIN masterclass.gold.fact_orders  o ON p.product_key = o.product_key
LEFT JOIN masterclass.gold.fact_returns r ON p.product_key = r.product_key
GROUP BY p.category
ORDER BY gross_sales DESC
```

---

## Segment 14 — Wrap-Up (5 min)

**Recap checklist:**
- [x] Star schema design — fact at center, dims radiating out
- [x] Grain declaration — one row per order line item
- [x] MD5 surrogate keys — deterministic, distributed-friendly
- [x] Unknown member pattern — no broken joins on late-arriving data
- [x] `dim_date` with flags — never store raw dates in facts
- [x] SCD Type 1 — MERGE overwrite when history doesn't matter
- [x] SCD Type 2 — `start_date` / `end_date` / `is_current` for full history
- [x] Role-playing dimensions — one `dim_date` aliased as order date and ship date
- [x] Degenerate dimensions — `order_number` lives in the fact, no dim table needed
- [x] Conformed dimensions — same dims shared across `fact_orders` and `fact_returns`
- [x] Load order — always dims before facts
- [x] Lookup join pattern — LEFT JOIN to dims at fact load time
- [x] Databricks Workflow — DAG enforces load order visually
- [x] Four business questions answered with clean SQL on Gold

**Cloud-agnostic note:** Every concept shown here — star schema, SCDs, conformed dims,
the medallion layers — works identically on Snowflake, BigQuery, and Redshift.
The SQL is standard; only the catalog/schema syntax changes slightly.
