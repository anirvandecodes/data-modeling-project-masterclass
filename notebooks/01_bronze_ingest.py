# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Ingest
# MAGIC Land raw CSVs from the Unity Catalog volume into Bronze Delta tables.
# MAGIC No transformations — just durability.

# COMMAND ----------

catalog      = "workspace"
volume_path  = f"/Volumes/{catalog}/bronze/raw_data"
tables       = ["customers", "products", "locations", "orders", "returns"]

for table in tables:
    df = (spark.read
              .option("header", True)
              .option("inferSchema", True)
              .csv(f"{volume_path}/{table}.csv"))
    (df.write
       .format("delta")
       .mode("overwrite")
       .saveAsTable(f"{catalog}.bronze.{table}"))
    print(f"  {catalog}.bronze.{table}: {df.count()} rows loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Bronze tables

# COMMAND ----------

for table in tables:
    print(f"\n--- {table} ---")
    spark.sql(f"SELECT * FROM {catalog}.bronze.{table} LIMIT 3").show(truncate=False)
