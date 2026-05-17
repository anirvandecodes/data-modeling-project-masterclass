# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 08 — Building dim_date: The Calendar Dimension
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why Does a Date Dimension Exist?
# MAGIC
# MAGIC At first, a date dimension sounds redundant. You already have `order_date` in your fact table — why create a whole separate table just for dates?
# MAGIC
# MAGIC The answer is **consistency and centralization**.
# MAGIC
# MAGIC Without a date dimension, every analyst writing a query has to compute calendar attributes from scratch:
# MAGIC
# MAGIC ```sql
# MAGIC -- Analyst A's version
# MAGIC SELECT YEAR(order_date), MONTH(order_date), SUM(total_amount)
# MAGIC FROM fact_orders
# MAGIC WHERE DAYOFWEEK(order_date) NOT IN (1, 7)  -- exclude weekends
# MAGIC
# MAGIC -- Analyst B's version (different convention for DAYOFWEEK)
# MAGIC SELECT YEAR(order_date), MONTH(order_date), SUM(total_amount)
# MAGIC FROM fact_orders
# MAGIC WHERE DAYOFWEEK(order_date) BETWEEN 2 AND 6  -- also excludes weekends
# MAGIC
# MAGIC -- Analyst C's version (wrong — DAYOFWEEK starts at 1=Sunday in Spark)
# MAGIC WHERE DAYOFWEEK(order_date) NOT IN (6, 7)  -- BUG: excludes Friday and Saturday
# MAGIC ```
# MAGIC
# MAGIC Three analysts, three different implementations, at least one with a bug. This is how "why don't my numbers match?" happens.
# MAGIC
# MAGIC A date dimension **pre-computes every calendar attribute once, correctly**. After that, every query just looks up a row:
# MAGIC
# MAGIC ```sql
# MAGIC -- Everyone's version with dim_date
# MAGIC SELECT d.year, d.month, SUM(f.total_amount)
# MAGIC FROM fact_orders f
# MAGIC JOIN dim_date d ON f.order_date_key = d.date_key
# MAGIC WHERE d.is_weekend = FALSE
# MAGIC GROUP BY d.year, d.month
# MAGIC ```
# MAGIC
# MAGIC One source of truth. `is_weekend = FALSE` means the same thing to everyone.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## The `date_key` Convention: Integer YYYYMMDD
# MAGIC
# MAGIC The date dimension uses an **integer** as its primary key, not a date type. Specifically: `YYYYMMDD` format.
# MAGIC
# MAGIC For example:
# MAGIC - March 15, 2024 → `date_key = 20240315`
# MAGIC - January 1, 2022 → `date_key = 20220101`
# MAGIC
# MAGIC Why integer instead of date?
# MAGIC
# MAGIC | Reason | Detail |
# MAGIC |---|---|
# MAGIC | **Performance** | Integer comparisons are marginally faster than date comparisons in columnar storage |
# MAGIC | **Readability** | `date_key = 20240315` is instantly human-readable in query results |
# MAGIC | **Interoperability** | Some BI tools work more reliably with integer keys |
# MAGIC | **Portability** | Works the same across all SQL dialects — no date format ambiguity |
# MAGIC
# MAGIC The convention is universal in dimensional modeling. You'll see it in every production data warehouse.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## What Columns Does dim_date Need?
# MAGIC
# MAGIC We pre-compute every calendar attribute an analyst might ever need:
# MAGIC
# MAGIC | Column | Type | Example | Use case |
# MAGIC |---|---|---|---|
# MAGIC | `date_key` | INT | 20240315 | PK — used as FK in fact tables |
# MAGIC | `full_date` | DATE | 2024-03-15 | Human-readable date |
# MAGIC | `year` | INT | 2024 | Annual aggregations |
# MAGIC | `quarter` | INT | 1 | Quarterly reporting |
# MAGIC | `month` | INT | 3 | Monthly aggregations |
# MAGIC | `month_name` | STRING | March | Labels in charts |
# MAGIC | `month_short` | STRING | Mar | Compact labels |
# MAGIC | `week_of_year` | INT | 11 | Weekly aggregations |
# MAGIC | `day_of_month` | INT | 15 | Day-of-month patterns |
# MAGIC | `day_of_week` | INT | 6 | Weekday vs weekend filtering |
# MAGIC | `day_name` | STRING | Friday | Labels in charts |
# MAGIC | `day_short` | STRING | Fri | Compact labels |
# MAGIC | `is_weekend` | BOOLEAN | false | Exclude/include weekends |
# MAGIC | `is_month_end` | BOOLEAN | false | Month-end reporting |
# MAGIC | `is_quarter_end` | BOOLEAN | false | Quarter-end reporting |
# MAGIC | `fiscal_year` | INT | 2024 | In this model, same as calendar year |
# MAGIC | `fiscal_quarter` | INT | 1 | In this model, same as calendar quarter |
# MAGIC | `year_month` | STRING | 2024-03 | Grouping by month in ORDER BY |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to Generate a Date Range Without Source Data
# MAGIC
# MAGIC The date dimension is unique: it's the only dimension table that doesn't come from source data. We **generate it programmatically** using Spark's `sequence()` function, which produces a list of consecutive dates between a start and end date.
# MAGIC
# MAGIC Then we use `explode()` to turn that list into one row per date, and apply date functions to derive all the calendar attributes.
# MAGIC
# MAGIC We'll cover 2020-01-01 through 2025-12-31 — giving us a 6-year date spine that covers our 3 years of order data (2022–2024) with room to grow.

# COMMAND ----------

from pyspark.sql.functions import (
    col, year, quarter, month, date_format,
    weekofyear, dayofmonth, dayofweek, last_day, when
)

catalog = "workspace"

# Generate one row per day from 2020-01-01 to 2025-12-31
# sequence() creates an array of dates; explode() turns the array into rows
date_range = spark.sql("""
    SELECT explode(
        sequence(to_date('2020-01-01'), to_date('2025-12-31'), interval 1 day)
    ) AS full_date
""")

print(f"Date range generated: {date_range.count()} days")
print("First few dates:")
date_range.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Building the dim_date DataFrame
# MAGIC
# MAGIC Now we apply all the date functions to derive every calendar attribute. A few notes on implementation:
# MAGIC
# MAGIC - **`date_format(col, "yyyyMMdd").cast("int")`** — converts the date to the YYYYMMDD integer key
# MAGIC - **`dayofweek()`** — returns 1=Sunday, 2=Monday, ..., 7=Saturday in Spark. So weekends are day 1 and day 7.
# MAGIC - **`is_month_end`** — we compare `full_date` to `last_day(full_date)`. If they're equal, it's the last day of the month.
# MAGIC - **`is_quarter_end`** — the last day of a quarter is the last day of March (3), June (6), September (9), or December (12). We check both conditions.
# MAGIC - **`year_month`** — formatted as `"yyyy-MM"` so that `ORDER BY year_month` sorts chronologically (unlike `ORDER BY month_name` which sorts alphabetically).

# COMMAND ----------

dim_date = date_range.select(
    # Primary key: integer YYYYMMDD format
    date_format(col("full_date"), "yyyyMMdd").cast("int").alias("date_key"),

    # The actual date
    col("full_date"),

    # Year / Quarter / Month breakdowns
    year("full_date").alias("year"),
    quarter("full_date").alias("quarter"),
    month("full_date").alias("month"),
    date_format(col("full_date"), "MMMM").alias("month_name"),   # January, February, ...
    date_format(col("full_date"), "MMM").alias("month_short"),   # Jan, Feb, ...

    # Week and day breakdowns
    weekofyear("full_date").alias("week_of_year"),
    dayofmonth("full_date").alias("day_of_month"),
    dayofweek("full_date").alias("day_of_week"),                 # 1=Sun, 7=Sat
    date_format(col("full_date"), "EEEE").alias("day_name"),     # Monday, Tuesday, ...
    date_format(col("full_date"), "EEE").alias("day_short"),     # Mon, Tue, ...

    # Boolean flags — pre-computed so analysts don't have to
    when(dayofweek("full_date").isin(1, 7), True)
        .otherwise(False).alias("is_weekend"),

    when(col("full_date") == last_day(col("full_date")), True)
        .otherwise(False).alias("is_month_end"),

    when(
        month("full_date").isin(3, 6, 9, 12) &
        (col("full_date") == last_day(col("full_date"))),
        True
    ).otherwise(False).alias("is_quarter_end"),

    # Fiscal year (same as calendar year in this business)
    year("full_date").alias("fiscal_year"),
    quarter("full_date").alias("fiscal_quarter"),

    # Sortable year-month string: "2024-03" — ORDER BY this, not month_name
    date_format(col("full_date"), "yyyy-MM").alias("year_month"),
)

print("dim_date schema:")
dim_date.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing dim_date to Gold
# MAGIC
# MAGIC We write with `mode("overwrite")` — this is the **full refresh** load strategy we defined in Notebook 06. dim_date never needs an incremental merge because calendar dates don't change. We just regenerate the entire table if we ever extend the date range.

# COMMAND ----------

# Write to Gold layer
dim_date.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.gold.dim_date")

total_rows = spark.sql(f"SELECT COUNT(*) FROM {catalog}.gold.dim_date").collect()[0][0]
print(f"workspace.gold.dim_date written successfully.")
print(f"Total rows: {total_rows:,}")
print(f"Expected: 2,192 rows (6 years × ~365.25 days/year, including 2020 and 2024 leap years)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verifying the Flags Work
# MAGIC
# MAGIC Let's query a specific week — March 28 through April 2, 2024 — which crosses a weekend AND a month-end. This lets us verify that `is_weekend` and `is_month_end` are both working correctly.
# MAGIC
# MAGIC March 31, 2024 (Sunday) should have BOTH `is_weekend = true` AND `is_month_end = true`.

# COMMAND ----------

print("Verification: a week crossing a weekend and a month-end (March 28 - April 2, 2024)")
print("March 31 should be: is_weekend=true AND is_month_end=true")
print()

spark.sql(f"""
    SELECT
        date_key,
        full_date,
        day_name,
        is_weekend,
        is_month_end,
        is_quarter_end
    FROM {catalog}.gold.dim_date
    WHERE full_date BETWEEN '2024-03-28' AND '2024-04-02'
    ORDER BY full_date
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC **What you should see:**
# MAGIC - 2024-03-28 (Thursday) — is_weekend=false, is_month_end=false
# MAGIC - 2024-03-29 (Friday) — is_weekend=false, is_month_end=false
# MAGIC - 2024-03-30 (Saturday) — is_weekend=**true**, is_month_end=false
# MAGIC - 2024-03-31 (Sunday) — is_weekend=**true**, is_month_end=**true** (last day of March)
# MAGIC - 2024-04-01 (Monday) — is_weekend=false, is_month_end=false
# MAGIC - 2024-04-02 (Tuesday) — is_weekend=false, is_month_end=false

# COMMAND ----------

# Verify the quarter-end flags (March 31, June 30, September 30, December 31)
print("Quarter-end dates in 2024 (is_quarter_end = true):")
spark.sql(f"""
    SELECT date_key, full_date, day_name, month_name, is_month_end, is_quarter_end
    FROM {catalog}.gold.dim_date
    WHERE year = 2024 AND is_quarter_end = true
    ORDER BY full_date
""").show()

# COMMAND ----------

# Verify the weekend count makes sense
print("Weekend days vs weekdays in 2024:")
spark.sql(f"""
    SELECT
        is_weekend,
        COUNT(*) AS day_count
    FROM {catalog}.gold.dim_date
    WHERE year = 2024
    GROUP BY is_weekend
    ORDER BY is_weekend
""").show()

# 2024 has 366 days (leap year). 52 full weeks = 104 weekend days. Plus 2 extra days.
# Check: does our count match expectations?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checking Coverage Against Our Orders Data
# MAGIC
# MAGIC Let's confirm that every `order_date` in our Bronze orders table has a matching `date_key` in `dim_date`. If any dates fall outside our 2020–2025 range, those orders won't be able to join to the date dimension — a data integrity problem.

# COMMAND ----------

print("Date coverage check: do all order dates fall within dim_date's range?")

spark.sql(f"""
    SELECT
        COUNT(*) AS total_orders,
        SUM(CASE WHEN d.date_key IS NULL THEN 1 ELSE 0 END) AS orders_missing_date_key,
        MIN(o.order_date) AS earliest_order_date,
        MAX(o.order_date) AS latest_order_date
    FROM {catalog}.bronze.orders o
    LEFT JOIN {catalog}.gold.dim_date d
        ON CAST(date_format(o.order_date, 'yyyyMMdd') AS INT) = d.date_key
""").show()

# COMMAND ----------

# Also show the full date range in dim_date
print("dim_date coverage range:")
spark.sql(f"""
    SELECT
        MIN(full_date) AS start_date,
        MAX(full_date) AS end_date,
        COUNT(*) AS total_days,
        COUNT(DISTINCT year) AS years_covered
    FROM {catalog}.gold.dim_date
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## A Look at the Full Table
# MAGIC
# MAGIC Let's take a final look at a broader sample of `dim_date` to appreciate the richness of attributes it provides.

# COMMAND ----------

print("dim_date — sample rows (first week of January 2024):")
spark.sql(f"""
    SELECT
        date_key,
        full_date,
        year,
        quarter,
        month,
        month_name,
        week_of_year,
        day_of_week,
        day_name,
        is_weekend,
        is_month_end,
        year_month
    FROM {catalog}.gold.dim_date
    WHERE full_date BETWEEN '2024-01-01' AND '2024-01-07'
    ORDER BY full_date
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC In this notebook we:
# MAGIC 1. Explained why date dimensions exist: centralized calendar logic, consistent `is_weekend` / `is_month_end` flags
# MAGIC 2. Explained the `date_key` integer convention (YYYYMMDD): `20240315 = March 15, 2024`
# MAGIC 3. Defined all 18 columns in `dim_date` and why each exists
# MAGIC 4. Generated 2,192 rows using Spark's `sequence()` and `explode()` functions
# MAGIC 5. Derived all calendar attributes: year, quarter, month, week, day, flags
# MAGIC 6. Wrote to `workspace.gold.dim_date` using the full refresh strategy
# MAGIC 7. Verified correctness: `is_weekend`, `is_month_end`, `is_quarter_end` flags all correct
# MAGIC 8. Confirmed all order dates fall within the date dimension's range
# MAGIC
# MAGIC **`workspace.gold.dim_date` is complete and ready for use in fact tables.**
# MAGIC
# MAGIC **Next up — Notebook 09: dim_customer.** We'll build the customer dimension and introduce one of the most important concepts in dimensional modeling: Slowly Changing Dimensions (SCDs) — what to do when a customer's address changes.
