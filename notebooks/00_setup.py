# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup
# MAGIC Create schemas and volume under the existing `workspace` catalog.

# COMMAND ----------

catalog = "workspace"
volume_schema = "bronze"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.bronze.raw_data")

print("Schemas and volume ready:")
spark.sql(f"SHOW SCHEMAS IN {catalog}").show()
