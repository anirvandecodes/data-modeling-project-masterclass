# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — dim_date
# MAGIC Build a complete calendar dimension covering 2020–2025.
# MAGIC
# MAGIC **Why?** Never store raw dates in fact tables. A dedicated date dimension lets you
# MAGIC filter by `is_weekend`, `is_month_end`, `fiscal_quarter`, etc. without any extra logic.

# COMMAND ----------

from pyspark.sql.functions import (
    col, year, quarter, month, date_format,
    weekofyear, dayofmonth, dayofweek, last_day, when
)

catalog = "workspace"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate one row per date

# COMMAND ----------

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
    date_format(col("full_date"), "MMM").alias("month_short"),
    weekofyear("full_date").alias("week_of_year"),
    dayofmonth("full_date").alias("day_of_month"),
    dayofweek("full_date").alias("day_of_week"),
    date_format(col("full_date"), "EEEE").alias("day_name"),
    date_format(col("full_date"), "EEE").alias("day_short"),
    when(dayofweek("full_date").isin(1, 7), True).otherwise(False).alias("is_weekend"),
    when(
        col("full_date") == last_day(col("full_date")), True
    ).otherwise(False).alias("is_month_end"),
    when(
        month("full_date").isin(3, 6, 9, 12) &
        (col("full_date") == last_day(col("full_date"))),
        True
    ).otherwise(False).alias("is_quarter_end"),
    year("full_date").alias("fiscal_year"),
    quarter("full_date").alias("fiscal_quarter"),
    date_format(col("full_date"), "yyyy-MM").alias("year_month"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold

# COMMAND ----------

(dim_date.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.gold.dim_date"))

count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {catalog}.gold.dim_date").collect()[0]["cnt"]
print(f"dim_date built: {count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview

# COMMAND ----------

spark.sql(f"""
    SELECT date_key, full_date, year, quarter, month_name,
           day_name, is_weekend, is_month_end, fiscal_quarter
    FROM {catalog}.gold.dim_date
    WHERE full_date BETWEEN '2024-03-28' AND '2024-04-02'
    ORDER BY full_date
""").show()
