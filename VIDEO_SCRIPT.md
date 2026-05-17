# Data Modeling Masterclass — Video Script

**Format:** Single long-form video (~120 min)
**Style:** Conversational, fast-paced, no fluff
**Tone:** Like a senior engineer explaining to a smart friend

---

## HOOK (0:00 – 1:30)

**[SCREEN: Black. Then one line of text fades in:]**
> *"Why do some data teams move fast and others drown in dashboards that don't work?"*

**[Cut to face cam — direct, no intro music yet]**

Here's the thing nobody tells you when you start working with data.

You can know Python. You can know Spark. You can know SQL.
And your dashboards will still be wrong.
Your queries will still be slow.
Your analysts will still be asking "why don't these numbers match?"

And the answer — almost every single time — is bad data modeling.

Data modeling is the invisible foundation under every good data product.
Get it right, and everything downstream is easy.
Get it wrong, and you're spending your Fridays debugging why sales in one dashboard
don't match sales in another dashboard — for the same date range, same product, same everything.

Today, we're fixing that.

I'm going to take you from zero to a complete, production-grade data model
on Databricks — in one video.

We're going to build a real e-commerce data platform from scratch.
Raw CSVs in, clean star schema out, business questions answered with SQL.
No hand-waving. Every line of code runs. You can clone this repo right now and follow along.

By the end of this video, you will understand:
- Why star schemas exist and how to design one from scratch
- How to build fact tables and dimension tables the right way
- What slowly changing dimensions are and how to implement both Type 1 and Type 2
- How role-playing dimensions, degenerate dimensions, and conformed dimensions work
- How to orchestrate a full pipeline on Databricks using serverless compute

And most importantly — you'll understand *how to think* about data modeling,
which means you can apply this to any system, on any platform.

Let's go.

**[Intro music — 5 seconds]**

---

## SEGMENT 1: THE PROBLEM WITH MOST DATA (1:30 – 6:00)

**[Screen: a messy spreadsheet or a raw database diagram with 40 normalized tables]**

Let me paint you a picture.

Imagine you join a company as a data engineer or analyst.
The data exists — somewhere. There's a Postgres database, maybe a MySQL instance,
some CSVs from a third-party tool, maybe some Kafka events.

You need to answer one question: **"What were our top-selling products last month?"**

Simple question. Should take 10 minutes. Right?

Except the product data is in one table, the orders are in three separate tables
joined by foreign keys, the dates are stored as timestamps in UTC but the business
runs on US Eastern, and the "product category" column is in yet another table
that someone normalized six years ago when they thought they were being clever.

Your simple question is now a 15-table JOIN.

And here's the worst part: every analyst on the team writes their own version
of that JOIN. And they all get slightly different numbers.

**This is the OLTP vs OLAP problem.** And it's the most important mental shift in data engineering.

**[Screen: Two-column comparison]**

OLTP — Online Transaction Processing.
This is your production database. It's designed for writes.
Normalized, row-oriented, optimized to INSERT a single order record in milliseconds.
It's not designed for you to scan 50 million rows and group by category.

OLAP — Online Analytical Processing.
This is your analytics layer. It's designed for reads.
Denormalized, column-oriented, optimized so that an analyst can answer
"total sales by category by month" in seconds, not hours.

The solution to this problem has been around since the 1990s.
It's called **dimensional modeling**, and it was formalized by Ralph Kimball.

And it still wins today — on Databricks, on Snowflake, on BigQuery, on Redshift.
The platforms change. The principles don't.

---

## SEGMENT 2: THE ARCHITECTURE (6:00 – 11:00)

**[Screen: Medallion architecture diagram — Bronze, Silver, Gold]**

Before we talk tables, let me show you the architecture we're building.

We're using the **Medallion Architecture**. Three layers.

**Bronze** — this is raw data. We land our source files here exactly as they arrive.
No transformations. No cleaning. Just durability.
If something breaks downstream, we can always reprocess from Bronze.

**Silver** — this is cleaned, validated data. Nulls handled, data types cast,
duplicates removed. In today's project, we're going light on Silver
because our source data is already clean — but in production, Silver is where
you put your data quality logic.

**Gold** — this is the modeled layer. This is where the star schema lives.
This is what your analysts, your BI tools, your dashboards query.
Clean, fast, business-friendly.

**[Screen: Star schema diagram]**

And the Gold layer is built as a **star schema**.

One fact table at the center. Dimension tables surrounding it.
Like a star. Hence the name.

The fact table stores what *happened* — orders, transactions, events.
Dimension tables store the *context* — who, what, when, where.

This is the structure we're building today:

- `fact_orders` at the center
- `dim_customer` — who bought
- `dim_product` — what they bought
- `dim_location` — where
- `dim_date` — when

And then a second fact table, `fact_returns`, that reuses the same dimensions.
That's a concept called **conformed dimensions** — and it's what lets you do
"sales vs returns by category" in a single SQL query.

Simple. Powerful. Let's build it.

---

## SEGMENT 3: THE DATA (11:00 – 14:00)

**[Screen: repo file tree, then terminal]**

Here's our scenario. We're the data team at an e-commerce company.
We sell electronics, clothing, home goods, sports gear, books.
We have customers buying through four channels: web, mobile, in-store, phone.
Orders come in, products ship, sometimes things get returned.

Our source data is five CSV files:
- **customers** — 200 customers, each with a name, email, address, and channel
- **products** — 40 products across 7 categories
- **locations** — 30 store locations across the US
- **orders** — 5,000+ order line items spanning 3 years
- **returns** — 300 return transactions

These represent what a real source system extract looks like.
Raw, flat, with natural keys like `customer_id = 42`.

**[Terminal: run generate_data.py]**

```bash
python3 data/generate_data.py
```

5,052 order lines generated. Realistic distributions.
Some customers buy often, some rarely. Some products have high return rates.
The data tells a story — and by the end, we'll be reading it.

Now let's get this into Databricks.

---

## SEGMENT 4: SETUP & BRONZE (14:00 – 20:00)

**[Screen: Databricks workspace — notebook 00_setup]**

First, setup. We're working in the `workspace` catalog, which already exists
in our Unity Catalog environment.

We need three schemas: Bronze, Silver, Gold.
And a **Volume** — which is Unity Catalog's managed file storage.
This is where our CSVs will live.

**[Run 00_setup — show schemas being created]**

Done. Now let's upload the CSVs to the volume.

```bash
databricks fs cp data/raw/orders.csv \
  dbfs:/Volumes/workspace/bronze/raw_data/orders.csv --overwrite
```

**[Screen: notebook 01_bronze_ingest]**

Now the Bronze ingest. This is dead simple — and intentionally so.
We read each CSV with `inferSchema = true` and write it as a Delta table.
No transformations. No business logic.

```python
for table in ["customers", "products", "locations", "orders", "returns"]:
    df = spark.read.option("header", True).option("inferSchema", True) \
             .csv(f"{volume_path}/{table}.csv")
    df.write.format("delta").mode("overwrite") \
      .saveAsTable(f"workspace.bronze.{table}")
```

**[Run — show row counts]**

Bronze is our safety net. Whatever happens in Silver or Gold,
we can always come back here and reprocess.

---

## SEGMENT 5: THE DATE DIMENSION (20:00 – 30:00)

**[Screen: notebook 02_dim_date]**

Now we start building Gold. And we always — **always** — build dimensions before facts.

I'll explain why in a moment. First, the simplest and most important dimension:
the **date dimension**.

Here's a question: why not just store dates as dates in your fact table?
`order_date = '2024-03-15'`. Simple. Right?

**[Beat. Then:]**

Because the moment a business analyst asks "show me sales for all weekends in Q1"
or "flag all transactions on public holidays"
or "group by fiscal quarter, not calendar quarter" —

a raw date column gives you nothing.
You're writing `DAYOFWEEK()` and `DATEADD()` logic inside every single query.
Every analyst repeats that logic. They all get it slightly different.

A date dimension solves this once, permanently.

**[Screen: dim_date columns]**

We generate one row per day from 2020 to 2025.
`date_key` is an integer in `YYYYMMDD` format — `20240315`.
Integer keys are faster to join on than dates.

Then we pre-compute every attribute a business will ever need:
- `year`, `quarter`, `month`, `month_name`
- `week_of_year`, `day_of_week`, `day_name`
- `is_weekend` — Boolean, computed once
- `is_month_end`, `is_quarter_end`
- `fiscal_year`, `fiscal_quarter`

```python
dim_date = date_range.select(
    date_format(col("full_date"), "yyyyMMdd").cast("int").alias("date_key"),
    col("full_date"),
    year("full_date").alias("year"),
    when(dayofweek("full_date").isin(1, 7), True)
        .otherwise(False).alias("is_weekend"),
    ...
)
```

**[Run — show preview with weekend/month-end rows highlighted]**

2,192 rows. One per day. Generated once. Referenced by every fact table forever.

Now instead of writing date logic in queries, analysts just do:

```sql
WHERE dim_date.is_weekend = true
AND   dim_date.fiscal_quarter = 2
```

Clean. Fast. No mistakes.

---

## SEGMENT 6: SURROGATE KEYS — THE MD5 APPROACH (30:00 – 39:00)

**[Screen: notebook 03_dim_product — focus on surrogate key section]**

Before we build the product dimension, I need to talk about **surrogate keys**.
This is one of the most misunderstood concepts in data modeling.

Every dimension table has two types of keys:

**Natural key** — the business identifier. `customer_id = 42`. `sku = "PB14"`.
This comes from your source system.

**Surrogate key** — a system-generated identifier that *you* control.
This is the key that fact tables reference.

Why not just use the natural key directly?

**[Screen: three bullet points appearing one by one]**

**Reason 1: Source systems change.**
Your e-commerce platform migrates to a new system. Customer IDs get reset.
Customer 42 in the old system is customer 10042 in the new system.
If your fact table stored the raw `customer_id`, your history is broken.
With surrogate keys, the dimension table absorbs the change. The fact table doesn't care.

**Reason 2: Multiple sources.**
You merge with another company. Now you have two customer tables.
Both have a `customer_id = 1`. Surrogate keys make them unambiguous.

**Reason 3: SCD Type 2.**
When a customer changes their address, you want to keep the old record
and create a new one. The fact table needs to point to the *right version*.
Surrogate keys make this possible. Natural keys don't.

**OK so how do you generate them?**

There are three common approaches. Let me show you all three and then tell you which one to use.

**[Screen: three approaches side by side]**

**Approach 1: ROW_NUMBER()**
```sql
ROW_NUMBER() OVER (ORDER BY product_id) AS product_key
```
Simple. But not idempotent — run it twice and you get different numbers
if rows are added in between.

**Approach 2: IDENTITY / AUTOINCREMENT**
```sql
product_key BIGINT GENERATED ALWAYS AS IDENTITY
```
Database assigns it automatically. Works well on single-node systems.
Terrible in distributed Spark jobs — creates a bottleneck.

**Approach 3: MD5 Hash** ← this is what we use
```python
md5(concat_ws("|", col("product_id").cast("string"), col("sku")))
```

Hash the natural business key. Same input always produces the same output.
Deterministic — run it a thousand times, get the same key.
Distributed-friendly — every Spark executor can compute it independently.
Portable — works on any platform.

**[Show the output — a 32-character hex string]**

Yes, it's a string not an integer. Modern columnar storage handles string keys efficiently.
The determinism is worth it.

**[Screen: notebook — full dim_product build]**

```python
dim_product = bronze_products.withColumn(
    "product_key",
    md5(concat_ws("|",
        col("source_product_id").cast("string"),
        col("sku")
    ))
)
```

One more thing — the **unknown member row**.

Every dimension needs one. It's a special row with `product_key = 'unknown'`
and `source_product_id = -1`.

When a fact record arrives referencing a product that doesn't exist in the dimension yet
— what we call a late-arriving dimension —
the lookup join falls through to this row instead of dropping the fact record or failing.

```python
unknown = spark.createDataFrame([{
    "product_key": "unknown",
    "source_product_id": -1,
    "product_name": "Unknown Product",
    ...
}])
dim_product = unknown.unionByName(dim_product)
```

**[Run — show final dim_product with unknown row at top]**

---

## SEGMENT 7: DIMENSION TABLE DESIGN (39:00 – 47:00)

**[Screen: notebook 04_dim_location — quick build, then whiteboard-style diagram]**

Location dimension is the same pattern. MD5 key, unknown member, done.
I'll run through it fast.

**[Run 04_dim_location — show output]**

But let me use this moment to talk about what makes a good dimension table.

**Rule 1: Make it wide.**
Pack every descriptive attribute into the dimension.
Don't normalize `region` into a separate table.
Don't make analysts join four tables to find out what city a customer is in.
The value of a dimension is that it's self-contained and readable.

**Rule 2: Natural hierarchies are your friend.**
Our product dimension has: `category → subcategory → product_name → sku`.
That hierarchy lets analysts drill down without any extra JOINs.

```sql
GROUP BY category           -- roll up to category
GROUP BY category, subcategory   -- drill to subcategory
GROUP BY product_name       -- all the way to product
```

One table. Three levels of aggregation. No joins needed.

**Rule 3: Surrogate key in the fact, source key in the dimension.**
The fact table stores `product_key` — the MD5 hash.
The dimension table stores both `product_key` AND `source_product_id`.
So you can always trace back to the source system when you need to.

---

## SEGMENT 8: SCD TYPE 1 — OVERWRITE (47:00 – 55:00)

**[Screen: notebook 05a_dim_customer_scd1]**

Now we get to one of the most important topics in all of data modeling.
**Slowly Changing Dimensions.** SCDs.

A slowly changing dimension is a dimension where the attributes change over time.
Customers move. Products get recategorized. Prices change.

The question is: what do you do when that happens?

There are two main strategies. Let's do Type 1 first — it's simpler.

**SCD Type 1 = Overwrite.**

When an attribute changes, you update the existing row. The old value is gone forever.

When do you use this? When the old value has no analytical meaning.
If a customer typed their email wrong and you correct it — that's Type 1.
You don't care that the email was wrong before. You just want it correct now.

**[Screen: MERGE statement]**

The implementation is a single MERGE statement:

```sql
MERGE INTO gold.dim_customer_scd1 AS target
USING incoming_customers AS source
ON target.source_customer_id = source.source_customer_id
WHEN MATCHED THEN UPDATE SET
    target.email  = source.email,
    target.city   = source.city,
    ...
WHEN NOT MATCHED THEN INSERT *
```

If the row exists → update every field in place.
If it's new → insert it.
That's it.

**[Run the notebook — show results]**

**[Demo moment — this is crucial for the video]**

Watch this. I'm going to change Alice Smith's email in the source data.
Run it again.

**[Show the table — old email gone, new email in place]**

The old email is gone. Replaced. No trace of what it was.
That's SCD Type 1. Simple, destructive, exactly right for corrections.

But what if you need to know *where the customer was when they placed the order*?
That's a different problem. And it needs a different solution.

---

## SEGMENT 9: SCD TYPE 2 — FULL HISTORY (55:00 – 1:07:00)

**[Screen: notebook 05b_dim_customer_scd2]**

**SCD Type 2 = Full history.**

Instead of overwriting, you close the old record and insert a new one.
Every version of every customer is preserved.

Three new columns:
- `start_date` — when this version became active
- `end_date` — when it was replaced (`9999-12-31` means still current)
- `is_current` — Boolean flag, `true` for the latest version

**[Screen: example table showing two rows for same customer]**

```
customer_id | city         | start_date | end_date   | is_current
42          | New York     | 2022-01-01 | 2024-03-14 | false
42          | Los Angeles  | 2024-03-15 | 9999-12-31 | true
```

Now I can ask: "What city was this customer in when they placed their order in January 2023?"
Join on the date range. Answer: New York. Correct.

With Type 1, that answer would be "Los Angeles" — wrong.

**When do you use Type 2?**
Any time you need historical accuracy. Customer location, customer tier, customer segment.
Anything where the old value has analytical meaning.

**[Screen: two-step MERGE pattern]**

The implementation is a two-step MERGE. This is the pattern.

**Step 1: Expire old rows where a tracked attribute changed.**

```sql
MERGE INTO gold.dim_customer AS target
USING incoming_customers AS source
ON target.source_customer_id = source.source_customer_id
AND target.is_current = true
WHEN MATCHED AND (
    target.address != source.address OR
    target.city    != source.city
)
THEN UPDATE SET
    target.end_date   = current_date(),
    target.is_current = false
```

**Step 2: Insert the new current version.**

```sql
MERGE INTO gold.dim_customer AS target
USING (
    SELECT src.*, current_date() AS start_date,
           DATE '9999-12-31' AS end_date, true AS is_current
    FROM incoming_customers src
    LEFT JOIN gold.dim_customer tgt
        ON src.source_customer_id = tgt.source_customer_id
       AND tgt.is_current = true
    WHERE tgt.source_customer_id IS NULL
       OR src.city != tgt.city OR src.address != tgt.address
) AS new_rows
ON target.source_customer_id = new_rows.source_customer_id
AND target.start_date = new_rows.start_date
WHEN NOT MATCHED THEN INSERT *
```

**[Run Step 1, run Step 2 — show the table]**

All customers have exactly one current row. Good.

**[Demo moment — the key one]**

Now watch. I'm going to simulate three customers moving to a new city.
Re-run the notebook.

**[Show the result — those customers now have two rows]**

```
customer_id | city         | is_current
5           | Chicago      | false      ← old record, now closed
5           | Denver       | true       ← new record, active today
```

The history is there. The current state is clear.
And critically — any `fact_orders` row that was loaded before the move
still points to the right version of that customer, because we loaded the fact
using the surrogate key at the time of the event.

**That is the power of SCD Type 2.**

---

## SEGMENT 10: BUILDING FACT_ORDERS (1:07:00 – 1:20:00)

**[Screen: notebook 06_fact_orders — grain declaration at top]**

Now the fact table. The center of the star.

Before writing a single line of code, you must declare the grain.

**The grain is: one row per order line item.**

Not one row per order. One row per *line item* on an order.
A single order can have four products. That's four rows in `fact_orders`.

Why this grain? Because it's the most atomic level of detail.
You can always aggregate up to order level, monthly level, category level.
You can never disaggregate. Start at the lowest grain you'll ever need.

**[Screen: fact_orders columns]**

The fact table has:
- **Foreign keys** — `customer_key`, `product_key`, `location_key`, `order_date_key`, `ship_date_key`
- **One degenerate dimension** — `order_number`
- **Measures** — `quantity`, `unit_price`, `discount_amount`, `total_amount`

Let me explain two of these in detail.

**Role-Playing Dimensions.**

Look at `order_date_key` and `ship_date_key`. Both are keys into `dim_date`.
The *same* dimension table, aliased twice.

This is called a role-playing dimension. `dim_date` is playing two roles:
once as the order date, once as the ship date.

In the query, you join to `dim_date` twice with different aliases:

```sql
LEFT JOIN dim_date dd_order ON src.order_date = dd_order.full_date
LEFT JOIN dim_date dd_ship  ON src.ship_date  = dd_ship.full_date
```

One table. Two roles. Clean.

**Degenerate Dimensions.**

`order_number` — `ORD-000001`. It's an identifier.
But there's no "order dimension table" with interesting attributes.
The order number itself is all you need — for auditing, for customer service, for lookups.

An attribute that has no dimension table lives directly in the fact.
That's a degenerate dimension.

**[Screen: the full INSERT statement]**

```sql
INSERT INTO workspace.gold.fact_orders
SELECT
    md5(concat_ws('|',
        cast(src.order_id    as string),
        cast(src.line_item_id as string)
    ))                              AS order_line_key,
    coalesce(dc.customer_key,  'unknown')  AS customer_key,
    coalesce(dp.product_key,   'unknown')  AS product_key,
    coalesce(dl.location_key,  'unknown')  AS location_key,
    dd_order.date_key               AS order_date_key,
    dd_ship.date_key                AS ship_date_key,
    src.order_number,
    src.quantity,
    src.unit_price,
    src.quantity * src.unit_price
        - coalesce(src.discount_amount, 0) AS total_amount
FROM bronze.orders src
LEFT JOIN gold.dim_customer  dc ON src.customer_id  = dc.source_customer_id AND dc.is_current = true
LEFT JOIN gold.dim_product   dp ON src.product_id   = dp.source_product_id
LEFT JOIN gold.dim_location  dl ON src.location_id  = dl.source_location_id
LEFT JOIN gold.dim_date dd_order ON to_date(src.order_date) = dd_order.full_date
LEFT JOIN gold.dim_date dd_ship  ON to_date(src.ship_date)  = dd_ship.full_date
```

This is the **lookup join pattern**. The fact table resolves all surrogate keys
by joining to the dimension tables at load time.

Notice it's all `LEFT JOIN`. This is intentional.
If a customer key doesn't exist in the dimension, `coalesce(..., 'unknown')`
sends that row to the unknown member. The fact row is never dropped.

**[Run — show 5,052 rows loaded, then verify no dropped rows]**

```
total_lines | unknown_customers | unknown_products | unknown_locations
5052        | 0                 | 0                | 0
```

Zero unknown keys. Every row resolved.

---

## SEGMENT 11: CONFORMED DIMENSIONS & FACT_RETURNS (1:20:00 – 1:30:00)

**[Screen: notebook 07_fact_returns]**

Here's where the architecture pays off.

`fact_returns` needs `dim_customer`, `dim_product`, and `dim_date`.
Those tables already exist. We built them for `fact_orders`.

We don't build new dimension tables. We reuse the existing ones.
Unchanged. As-is.

This is **conformed dimensions**. The most powerful concept in Kimball modeling.

Because both fact tables share the same dimensions, you can query across them:

```sql
SELECT
    p.category,
    SUM(o.total_amount)   AS gross_sales,
    SUM(r.return_amount)  AS total_returns,
    SUM(r.return_amount) / SUM(o.total_amount) * 100 AS return_rate_pct
FROM dim_product p
LEFT JOIN fact_orders  o ON p.product_key = o.product_key
LEFT JOIN fact_returns r ON p.product_key = r.product_key
GROUP BY p.category
```

This query spans two fact tables and a shared dimension.
It works because the dimension is conformed.

**[Run — show Sales vs Returns by category]**

Electronics: $180K in sales, 8% return rate.
Books: $12K in sales, 3% return rate.

That's a business insight. Built on top of a well-designed model.

---

## SEGMENT 12: FOUR TYPES OF FACT TABLES (1:30:00 – 1:45:00)

**[Screen: notebook 10_fact_table_types — title slide with four types]**

`fact_orders` and `fact_returns` are **transactional facts**.
One row per event. You INSERT when the event happens. You never update.
Measures are fully additive.

But there are three other fact table types you need to know.

**Type 2: Periodic Snapshot**

Instead of one row per event, you capture the STATE of something at a fixed interval.
Daily, weekly, monthly.

Classic example: **bank account balances**.
You don't record a transaction every day. You record the balance every day.

```
customer_id | snapshot_date | account_balance
42          | 2024-01-31    | 4500.00
42          | 2024-02-29    | 4800.00
42          | 2024-03-31    | 3200.00
```

Key thing to understand: `account_balance` is **semi-additive**.

You CAN sum it across customers on a single day — that's total platform balance.
You CANNOT sum it across days for one customer — that double-counts.
You SHOULD average it across days — that's average daily balance.

**[Screen: demo — monthly customer snapshot with cumulative_spend]**

We build `fact_customer_monthly_snapshot`: one row per customer per month,
with the cumulative spend window function.

**Type 3: Accumulating Snapshot**

One row per business process. That row gets updated as the process moves through milestones.

Think **order fulfillment**: Placed → Picked → Packed → Shipped → Delivered.

Each milestone is a date key column. The row is updated when each milestone is reached.
Lag measures — days from order to ship, days from ship to delivery — are derived from the milestones.

```
order_id | order_date_key | ship_date_key | delivery_date_key | days_to_ship | status
1001     | 20240315       | 20240317      | NULL              | 2            | Shipped
```

When delivered, you UPDATE that row with `delivery_date_key` and `days_to_deliver`.

**[Screen: demo — fact_order_fulfillment]**

**Type 4: Factless Fact**

An event with no numeric measures. You're recording that something *happened*,
not how much of it happened.

Student attendance. Which products were on promotion on which dates.
Employee eligibility for a benefit.

```
dim_product → fact_promotions ← dim_date
                 promotion_name
```

No amount. No quantity. Just the relationship: this product was on promotion on this date.

**[Screen: demo — fact_promotions, then query counting products per promotion per week]**

---

## SEGMENT 13: STAR VS SNOWFLAKE (1:45:00 – 1:55:00)

**[Screen: notebook 11_star_vs_snowflake — two diagrams side by side]**

Let me answer the question I get asked the most:
*"Should I use a star schema or a snowflake schema?"*

**Star schema:** each dimension is a single flat wide table.
Category, subcategory, product name, SKU — all in one `dim_product` table.

**Snowflake schema:** normalize the hierarchies into separate tables.
`dim_product` references `dim_subcategory`, which references `dim_category`.

**[Screen: side-by-side query]**

Here's the same business question on both schemas.

Star schema:
```sql
SELECT p.category, p.subcategory, SUM(f.total_amount)
FROM fact_orders f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.category, p.subcategory
```
Two tables. One join.

Snowflake schema:
```sql
SELECT cat.category_name, sub.subcategory_name, SUM(f.total_amount)
FROM fact_orders f
JOIN dim_product_sf   p   ON f.product_key     = p.product_key
JOIN dim_subcategory  sub ON p.subcategory_key  = sub.subcategory_key
JOIN dim_category     cat ON sub.category_key   = cat.category_key
GROUP BY cat.category_name, sub.subcategory_name
```
Four tables. Three joins.

**[Run both — same result, more complexity for snowflake]**

The snowflake result is identical. The query is harder to write and harder to maintain.

**My answer:** Use star schema for analytics. Always.
Storage is cheap. Query simplicity and analyst productivity are expensive.
Normalize only if you have a specific, concrete reason — like a dimension table with
100 attributes that truly belong in a sub-table. Not because it feels cleaner.

---

## SEGMENT 14: LOAD ORDER & ORCHESTRATION (1:55:00 – 2:05:00)

**[Screen: Databricks Workflow DAG]**

Here's the rule you will violate at least once if nobody tells you:

**Always load dimensions before facts.**

The fact table resolves surrogate keys from dimensions at load time.
If `dim_customer` isn't built yet when `fact_orders` loads,
the lookup join finds nothing. You get `customer_key = 'unknown'` for every row.
Your entire fact table is broken.

The correct load order:

```
Bronze ingest
    │
    ├──► dim_date        (no dependencies — static calendar)
    ├──► dim_product     (no dependencies)
    ├──► dim_location    (no dependencies)
    └──► dim_customer    (SCD Type 2 logic)
              │
         [All dims ready]
              │
    ├──► fact_orders     (joins all 4 dims)
    └──► fact_returns    (joins 3 dims)
```

Dimensions can run in parallel. Facts can only run after dimensions complete.

**[Screen: databricks.yml — the workflow definition]**

We define this entire DAG in a Databricks Asset Bundle — a YAML file
that describes our pipeline as code.

```yaml
- task_key: fact_orders
  depends_on:
    - task_key: dim_date
    - task_key: dim_product
    - task_key: dim_location
    - task_key: dim_customer_scd2
  notebook_task:
    notebook_path: .../06_fact_orders
```

One command to deploy, one command to run.

```bash
databricks bundle deploy
databricks bundle run data_modeling_pipeline
```

**[Show the Workflow UI — DAG with all tasks, running on serverless]**

And it runs on **serverless compute**. Zero cluster startup time.
The entire 12-task pipeline — Bronze through analytics — runs in under 3 minutes.

---

## SEGMENT 15: ANSWERING BUSINESS QUESTIONS (2:05:00 – 2:15:00)

**[Screen: notebook 08_analytics_queries]**

Remember those four business questions from the beginning?
Let's answer them. Gold tables only. Clean SQL.

**Q1: Total sales by product category per month.**

```sql
SELECT d.year, d.month_name, p.category,
       SUM(f.total_amount) AS total_sales
FROM fact_orders f
JOIN dim_date    d ON f.order_date_key = d.date_key
JOIN dim_product p ON f.product_key    = p.product_key
GROUP BY d.year, d.month, d.month_name, p.category
ORDER BY d.year, d.month, total_sales DESC
```

**[Run — show results, highlight Electronics dominating]**

**Q2: Top customers by lifetime value.**

```sql
SELECT c.first_name, c.last_name, c.channel,
       COUNT(DISTINCT f.order_number)   AS total_orders,
       SUM(f.total_amount)              AS lifetime_value
FROM fact_orders f
JOIN dim_customer c ON f.customer_key = c.customer_key AND c.is_current = true
GROUP BY c.first_name, c.last_name, c.channel
ORDER BY lifetime_value DESC
LIMIT 20
```

**Q3: Sales by geography.**
**Q4: Average order value by channel.**

**[Run all four — show results]**

And the bonus — year-over-year growth using a CTE:

```sql
WITH yearly AS (...)
SELECT curr.category, curr.total_sales,
       ROUND((curr.total_sales - prev.total_sales)
           / prev.total_sales * 100, 1) AS yoy_growth_pct
FROM yearly curr
LEFT JOIN yearly prev ON curr.category = prev.category
                      AND curr.year = prev.year + 1
```

This is the whole point. You built the model *once*.
Now any analyst can answer any question with simple SQL.
No 15-table joins. No inconsistent numbers. No Friday debugging sessions.

---

## SEGMENT 16: METRICS & PRODUCT SENSE (2:15:00 – 2:22:00)

**[Screen: notebook 12_metrics_and_product_sense]**

One more concept before we wrap. This one is for anyone who interviews for
data analyst or analytics engineer roles.

**Product-sense data modeling.** Define the metrics before you design the model.

Here are the metrics every data professional should know cold:

**DAU** — Daily Active Users. Distinct users who performed any action today.
**MAU** — Monthly Active Users. Same, over 30 days.
**Stickiness** — DAU / MAU. What % of monthly users come back daily.
**AOV** — Average Order Value. Total revenue / distinct orders.
**Cohort retention** — of users who joined in month X, what % are still active in month X+N?
**Return rate** — returns / gross sales.

**[Screen: DAU query running]**

```sql
SELECT d.full_date,
       COUNT(DISTINCT f.customer_key) AS daily_active_buyers
FROM fact_orders f
JOIN dim_date    d ON f.order_date_key = d.date_key
GROUP BY d.full_date
ORDER BY d.full_date
```

These are computed on the Gold star schema we just built.
No extra tables. No extra complexity.

**[Show cohort retention output — the heatmap-style result]**

When you're in an interview and they ask you to design a data model for Uber —
don't start with tables. Start with:

"The key metrics are completion rate, average fare, driver utilization, and DAU.
Let me design the model so those are computable with simple SQL."

Then define your fact table grain. Then your dimensions.
Then prove it by writing the SQL.

That structured approach is what separates good data modelers from great ones.

---

## OUTRO (2:22:00 – 2:27:00)

**[Face cam — direct]**

Let me recap what we built.

From five CSV files, we built a complete star schema on Databricks:

- A date dimension with 2,192 rows and every flag an analyst will ever need
- Product, location, and customer dimensions with MD5 surrogate keys
- SCD Type 1 for simple overwrites — and SCD Type 2 for full customer history
- `fact_orders` with role-playing dimensions, a degenerate dimension, and the lookup join pattern
- `fact_returns` powered by conformed dimensions
- A Databricks Workflow that orchestrates the whole thing on serverless in 3 minutes
- Periodic snapshot, accumulating snapshot, and factless fact tables
- A star vs snowflake comparison with runnable SQL
- Business metrics: DAU, MAU, AOV, cohort retention, return rates

Everything you see here is in the repo. Link in the description.
Clone it. Run it. Break it and fix it. That's how you actually learn this.

If this helped you, subscribe — I make more videos like this.
If you have questions, drop them in the comments. I read every one.

And if you're using this to prep for interviews or to redesign a data model at work —
let me know how it goes.

I'll see you in the next one.

**[End card: 20 seconds — subscribe, repo link, next video teaser]**

---

## PRODUCTION NOTES

**Demo moments to rehearse:**
1. SCD Type 1 demo — change an email, re-run, show the old value is gone
2. SCD Type 2 demo — change a city for 3 customers, re-run, show two rows per customer with is_current flip
3. The Workflow DAG — show it running live in the Databricks UI with all tasks green

**B-roll suggestions:**
- Terminal showing the bundle deploy + run
- Databricks Workflow UI with the DAG diagram
- Query results scrolling (especially the Sales vs Returns comparison)
- The star schema whiteboard diagram animated

**Thumbnail concept:**
"I Redesigned Our Entire Data Pipeline In 2 Hours" over a before/after schema diagram
OR a split: messy 40-table OLTP schema on left, clean 6-table star schema on right

**Title options:**
- "The Only Data Modeling Video You'll Ever Need"
- "Stop Building Bad Data Models (Do This Instead)"
- "Data Modeling Masterclass: Star Schema, SCD, and Kimball from Scratch"
- "I Built a Production Data Model in 2 Hours — Here's How"
