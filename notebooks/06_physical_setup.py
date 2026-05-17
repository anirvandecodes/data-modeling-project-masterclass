# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 06 — Physical Data Model: Setup on Databricks
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the Physical Model Adds
# MAGIC
# MAGIC In Notebooks 02–04, we built a conceptual model (entities and relationships) and a logical model (attributes, data types, keys). Both were platform-independent — they would work in PostgreSQL, Oracle, or any other relational system.
# MAGIC
# MAGIC The **physical model** is where we make decisions specific to our platform: Databricks and Delta Lake. These decisions determine:
# MAGIC - **Storage format**: What file format stores the data on disk?
# MAGIC - **Partitioning**: How is data split across files for faster filtering?
# MAGIC - **Indexing / Z-ordering**: How is data clustered within partitions?
# MAGIC - **Load strategy**: How do we update each table when new data arrives?
# MAGIC
# MAGIC Getting these right is the difference between a Gold table that takes 5 seconds to query and one that takes 5 minutes.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why Delta Lake?
# MAGIC
# MAGIC Databricks runs on Delta Lake — an open-source storage layer on top of Apache Parquet. You could store your Gold tables as plain Parquet files, but Delta Lake adds critical capabilities:
# MAGIC
# MAGIC | Feature | Plain Parquet | Delta Lake |
# MAGIC |---|---|---|
# MAGIC | **ACID transactions** | ❌ No | ✅ Yes — reads always see consistent state |
# MAGIC | **Time travel** | ❌ No | ✅ Yes — query any previous version: `VERSION AS OF 5` |
# MAGIC | **MERGE (upsert)** | ❌ No | ✅ Yes — update existing rows and insert new ones atomically |
# MAGIC | **Schema enforcement** | ❌ No | ✅ Yes — rejects data that doesn't match the schema |
# MAGIC | **Schema evolution** | ❌ No | ✅ Yes — add columns without rewriting files |
# MAGIC | **Data skipping** | Basic | ✅ Advanced — Delta tracks min/max per file, skips irrelevant files |
# MAGIC | **Optimize / Z-order** | ❌ No | ✅ Yes — compact small files and cluster data by column |
# MAGIC | **VACUUM** | ❌ No | ✅ Yes — clean up old versions to reclaim storage |
# MAGIC
# MAGIC For a production data model, Delta Lake is not optional — it's the foundation. We use it for every table in every layer.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Partitioning: Splitting Data for Fast Filtering
# MAGIC
# MAGIC **Partitioning** physically splits a table's data into separate folders on disk based on the values in a column. When a query filters by that column, Databricks only reads the relevant folders — it skips everything else.
# MAGIC
# MAGIC ```
# MAGIC   fact_orders (partitioned by year, month)
# MAGIC   │
# MAGIC   ├── year=2022/
# MAGIC   │   ├── month=1/  → files with Jan 2022 orders
# MAGIC   │   ├── month=2/  → files with Feb 2022 orders
# MAGIC   │   └── ...
# MAGIC   ├── year=2023/
# MAGIC   │   ├── month=1/  → files with Jan 2023 orders
# MAGIC   │   └── ...
# MAGIC   └── year=2024/
# MAGIC       └── ...
# MAGIC ```
# MAGIC
# MAGIC If a query says `WHERE order_year = 2024 AND order_month = 3`, Databricks goes directly to `year=2024/month=3/` and skips 95% of the data entirely. This is called **partition pruning**.
# MAGIC
# MAGIC ### When NOT to partition
# MAGIC
# MAGIC Partitioning is not always beneficial. Over-partitioning creates thousands of tiny files — which is worse than no partitioning at all. The rule of thumb:
# MAGIC - Only partition if the column has **low cardinality** (few distinct values): year, month, region, category
# MAGIC - Do NOT partition on high-cardinality columns: customer_id, order_id, date (too many values)
# MAGIC - Each partition should ideally be at least **1 GB** of data
# MAGIC
# MAGIC For our Gold tables: `fact_orders` gets partitioned by `year` and `month` because analysts almost always filter by time range. Dimension tables are small enough that partitioning adds no benefit.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Z-Ordering: Clustering Data Within Partitions
# MAGIC
# MAGIC **Z-ordering** (also called liquid clustering in newer Databricks versions) physically co-locates rows with similar values in the same files. When a query filters by a Z-ordered column, Delta Lake's data skipping can skip entire files — even within a partition.
# MAGIC
# MAGIC Z-ordering is most effective when:
# MAGIC - The column has **high cardinality** (many distinct values): customer_key, product_key, order_id
# MAGIC - Queries frequently filter or join on that column
# MAGIC
# MAGIC For `fact_orders`:
# MAGIC - `customer_key` — because analysts often filter "show me orders for this customer"
# MAGIC - `product_key` — because analysts often filter "show me orders for this product"
# MAGIC
# MAGIC Z-ordering is applied after data is loaded using `OPTIMIZE ... ZORDER BY (...)`. We'll do this in the fact table notebooks.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Strategies
# MAGIC
# MAGIC Different tables need different strategies for how they get updated when new data arrives:
# MAGIC
# MAGIC | Strategy | How it works | When to use |
# MAGIC |---|---|---|
# MAGIC | **Full refresh** | Truncate the table, reload all rows from scratch | Small tables where all source data is always available (dim_date, dim_product) |
# MAGIC | **Incremental MERGE** | Compare incoming rows to existing; UPDATE changed rows, INSERT new ones | Large tables or slowly changing dimensions (dim_customer) |
# MAGIC | **Truncate + Insert** | Truncate the fact table, reload all rows | Fact tables where you rebuild from scratch each run |
# MAGIC | **Append** | Add new rows without touching existing ones | Immutable event streams |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Physical Decisions Summary
# MAGIC
# MAGIC Here are our final physical design decisions for every Gold table:
# MAGIC
# MAGIC | Table | Format | Partition By | Z-Order | Load Strategy |
# MAGIC |---|---|---|---|---|
# MAGIC | `dim_date` | Delta | — | — | Full refresh |
# MAGIC | `dim_product` | Delta | — | — | Full refresh |
# MAGIC | `dim_location` | Delta | — | — | Full refresh |
# MAGIC | `dim_customer` | Delta | — | — | Incremental MERGE |
# MAGIC | `fact_orders` | Delta | year, month | customer_key, product_key | Truncate + Insert |
# MAGIC | `fact_returns` | Delta | — | — | Truncate + Insert |
# MAGIC
# MAGIC **Why no partition on dimension tables?**
# MAGIC Dimension tables are small (hundreds to thousands of rows). Partitioning adds overhead without benefit. Querying all of `dim_customer` takes milliseconds — no partition needed.
# MAGIC
# MAGIC **Why incremental MERGE for dim_customer?**
# MAGIC Customer records can change (address updates, segment changes). We want to update existing rows rather than delete and re-insert millions of records. MERGE handles this gracefully. We'll cover Slowly Changing Dimensions in detail in Notebook 09.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setting Up the Environment
# MAGIC
# MAGIC Now we'll create the three Medallion Architecture schemas (`bronze`, `silver`, `gold`) and the volume where raw CSV files will be uploaded.
# MAGIC
# MAGIC In Databricks Unity Catalog, the hierarchy is:
# MAGIC ```
# MAGIC Metastore
# MAGIC └── Catalog (workspace)
# MAGIC     ├── Schema (bronze)
# MAGIC     │   ├── Tables: customers, products, orders, locations, returns
# MAGIC     │   └── Volumes: raw_data  ← CSVs go here
# MAGIC     ├── Schema (silver)
# MAGIC     │   └── Tables: (cleaned versions, optional for this project)
# MAGIC     └── Schema (gold)
# MAGIC         └── Tables: dim_date, dim_customer, dim_product, dim_location,
# MAGIC                      fact_orders, fact_returns
# MAGIC ```

# COMMAND ----------

catalog = "workspace"

# Create the three Medallion Architecture schemas
print("Creating Medallion Architecture schemas...")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")
print(f"  {catalog}.bronze — created (or already exists)")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
print(f"  {catalog}.silver — created (or already exists)")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")
print(f"  {catalog}.gold — created (or already exists)")

# COMMAND ----------

# Create the volume for raw CSV files
# A Volume in Unity Catalog is a managed storage location for unstructured files
print("Creating volume for raw CSV files...")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.bronze.raw_data")
print(f"  {catalog}.bronze.raw_data — created (or already exists)")
print(f"\n  Upload your CSV files to: /Volumes/{catalog}/bronze/raw_data/")
print(f"  Expected files: customers.csv, products.csv, locations.csv, orders.csv, returns.csv")

# COMMAND ----------

# Verify: show all schemas in the catalog
print("Schemas in the workspace catalog:")
spark.sql(f"SHOW SCHEMAS IN {catalog}").show()

# COMMAND ----------

# Show the volume path for confirmation
print("Volume details:")
spark.sql(f"DESCRIBE VOLUME {catalog}.bronze.raw_data").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Uploading Your CSV Files
# MAGIC
# MAGIC Before running Notebook 07 (Bronze Ingest), you need to upload the 5 CSV files to the volume.
# MAGIC
# MAGIC **Option 1: Databricks UI**
# MAGIC 1. In the left sidebar, go to **Catalog**
# MAGIC 2. Navigate to `workspace` → `bronze` → `Volumes` → `raw_data`
# MAGIC 3. Click **Upload to this volume**
# MAGIC 4. Upload all 5 CSV files: `customers.csv`, `products.csv`, `locations.csv`, `orders.csv`, `returns.csv`
# MAGIC
# MAGIC **Option 2: Databricks CLI**
# MAGIC ```bash
# MAGIC databricks fs cp customers.csv dbfs:/Volumes/workspace/bronze/raw_data/customers.csv
# MAGIC databricks fs cp products.csv  dbfs:/Volumes/workspace/bronze/raw_data/products.csv
# MAGIC databricks fs cp locations.csv dbfs:/Volumes/workspace/bronze/raw_data/locations.csv
# MAGIC databricks fs cp orders.csv    dbfs:/Volumes/workspace/bronze/raw_data/orders.csv
# MAGIC databricks fs cp returns.csv   dbfs:/Volumes/workspace/bronze/raw_data/returns.csv
# MAGIC ```
# MAGIC
# MAGIC **Option 3: From the repo**
# MAGIC The CSV files are included in the `data/` folder of this repository. You can use the `databricks.yml` bundle to deploy them, or upload manually.

# COMMAND ----------

# Verify the volume is accessible and check if files have been uploaded
import os

volume_path = f"/Volumes/{catalog}/bronze/raw_data"
print(f"Checking volume: {volume_path}")
print()

try:
    files = dbutils.fs.ls(volume_path)
    if files:
        print("Files found in volume:")
        for f in files:
            print(f"  {f.name} ({f.size:,} bytes)")
    else:
        print("Volume is empty. Please upload the CSV files before running Notebook 07.")
except Exception as e:
    print(f"Volume check: {e}")
    print("Volume exists — upload CSV files to proceed with Notebook 07.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC In this notebook we:
# MAGIC 1. Defined what the physical model adds: storage format, partitioning, indexing, load strategy
# MAGIC 2. Explained why Delta Lake is superior to plain Parquet for analytics
# MAGIC 3. Explained partitioning and when to use it (low cardinality columns, large tables)
# MAGIC 4. Explained Z-ordering and when to use it (high cardinality columns, frequent filter columns)
# MAGIC 5. Documented our complete load strategy table for all 6 Gold tables
# MAGIC 6. Created the three Medallion Architecture schemas: `bronze`, `silver`, `gold`
# MAGIC 7. Created the `raw_data` volume for uploading CSV source files
# MAGIC
# MAGIC **Next up — Notebook 07: Bronze Ingest.** We'll load all 5 source CSV files from the volume into Bronze Delta tables — the raw landing zone of our Medallion Architecture.
