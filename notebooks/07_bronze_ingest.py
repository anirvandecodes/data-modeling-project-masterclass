# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 07 — Bronze Layer: Ingesting Raw Data
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is the Bronze Layer?
# MAGIC
# MAGIC The **Bronze layer** is the raw landing zone of the Medallion Architecture. Data arrives here exactly as it came from the source — no transformations, no cleaning, no renaming of columns, no fixing of nulls.
# MAGIC
# MAGIC Bronze has one job: **preserve the original data faithfully**.
# MAGIC
# MAGIC ### Why keep raw data?
# MAGIC
# MAGIC If something breaks downstream — a Silver transformation has a bug, a Gold model is corrupted, a business rule changes — you can always **replay from Bronze**. Every downstream layer is derived from Bronze and can be rebuilt.
# MAGIC
# MAGIC Without Bronze, a bug in your Silver transformation means the original data is gone. With Bronze, you just fix the transformation and replay.
# MAGIC
# MAGIC Think of Bronze as an **immutable audit log** of everything you ever received from source systems.
# MAGIC
# MAGIC ### Why Delta format even at Bronze?
# MAGIC
# MAGIC We could store Bronze as plain CSV files. But we use Delta even here because:
# MAGIC - **Time travel**: query what the raw data looked like at any point in the past
# MAGIC - **ACID transactions**: no partial writes — if the CSV load fails halfway, the table isn't corrupted
# MAGIC - **Schema enforcement**: if a source system sends a CSV with wrong column types, Delta rejects it
# MAGIC - **Data skipping**: Delta tracks file statistics for faster queries even on raw data
# MAGIC
# MAGIC The cost is minimal. The benefits are significant.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## What We're Loading
# MAGIC
# MAGIC We have 5 CSV files in the volume. Let's understand the expected schema of each before we load them:
# MAGIC
# MAGIC **customers.csv**
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | customer_id | INT | Unique identifier |
# MAGIC | first_name | STRING | |
# MAGIC | last_name | STRING | |
# MAGIC | email | STRING | |
# MAGIC | phone | STRING | |
# MAGIC | address | STRING | Street address |
# MAGIC | city | STRING | |
# MAGIC | state | STRING | |
# MAGIC | zip_code | STRING | |
# MAGIC | segment | STRING | Bronze / Silver / Gold tier |
# MAGIC | signup_date | DATE | When they registered |
# MAGIC
# MAGIC **products.csv**
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | product_id | INT | Unique identifier |
# MAGIC | sku | STRING | Stock keeping unit |
# MAGIC | product_name | STRING | |
# MAGIC | category | STRING | Top-level category |
# MAGIC | subcategory | STRING | Second-level category |
# MAGIC | brand | STRING | |
# MAGIC | unit_price | DECIMAL | Selling price |
# MAGIC | cost_price | DECIMAL | Cost to acquire |
# MAGIC | is_active | BOOLEAN | Currently available? |
# MAGIC
# MAGIC **locations.csv**
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | location_id | INT | Unique identifier |
# MAGIC | city | STRING | |
# MAGIC | state | STRING | |
# MAGIC | zip_code | STRING | |
# MAGIC | region | STRING | Northeast / Southeast / Midwest / West |
# MAGIC | country | STRING | |
# MAGIC
# MAGIC **orders.csv**
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | order_id | INT | Unique identifier |
# MAGIC | customer_id | INT | FK to customers |
# MAGIC | product_id | INT | FK to products |
# MAGIC | location_id | INT | FK to locations |
# MAGIC | order_date | DATE | When the order was placed |
# MAGIC | quantity | INT | Items ordered |
# MAGIC | unit_price | DECIMAL | Price at time of order |
# MAGIC | total_amount | DECIMAL | quantity × unit_price |
# MAGIC | channel | STRING | Online / In-Store / Mobile / Phone |
# MAGIC | status | STRING | Completed / Cancelled / Returned |
# MAGIC
# MAGIC **returns.csv**
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | return_id | INT | Unique identifier |
# MAGIC | order_id | INT | FK to orders |
# MAGIC | customer_id | INT | FK to customers |
# MAGIC | product_id | INT | FK to products |
# MAGIC | return_date | DATE | When the return was initiated |
# MAGIC | return_amount | DECIMAL | Amount refunded |
# MAGIC | reason | STRING | Defective / Wrong Item / Not as described / Other |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading All 5 Tables
# MAGIC
# MAGIC We'll use a simple loop: for each table name, read the CSV from the volume and write it as a Delta table in `workspace.bronze.*`.
# MAGIC
# MAGIC Key settings:
# MAGIC - `header=True` — first row of CSV is column names
# MAGIC - `inferSchema=True` — automatically detect data types (INT, DATE, DECIMAL, etc.)
# MAGIC - `mode="overwrite"` — replace the table if it already exists (idempotent load)
# MAGIC - `format("delta")` — store as Delta, not Parquet or CSV

# COMMAND ----------

catalog = "workspace"
volume_path = f"/Volumes/{catalog}/bronze/raw_data"

tables = ["customers", "products", "locations", "orders", "returns"]

print("Loading source CSV files into Bronze Delta tables...")
print("=" * 60)

for table in tables:
    csv_path = f"{volume_path}/{table}.csv"

    # Read the CSV file
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(csv_path)

    # Write as a Delta table — overwrite for idempotent loads
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{catalog}.bronze.{table}")

    row_count = df.count()
    col_count = len(df.columns)
    print(f"  ✓ {catalog}.bronze.{table}: {row_count:,} rows, {col_count} columns")

print()
print("Bronze ingest complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verifying the Load
# MAGIC
# MAGIC Let's verify each table loaded correctly by checking row counts and examining a sample of each.

# COMMAND ----------

# Quick row count verification for all tables
print("Row count verification:")
print("-" * 40)
for table in tables:
    count = spark.sql(f"SELECT COUNT(*) FROM {catalog}.bronze.{table}").collect()[0][0]
    print(f"  {catalog}.bronze.{table}: {count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample: Customers Table
# MAGIC
# MAGIC The `customers` table is our CUSTOMER entity. Let's check the data — we're looking for:
# MAGIC - Correct data types (customer_id as integer, signup_date as date)
# MAGIC - The three customer segments: Bronze, Silver, Gold
# MAGIC - Geographic spread across multiple states

# COMMAND ----------

print("workspace.bronze.customers — schema:")
spark.sql(f"DESCRIBE TABLE {catalog}.bronze.customers").show(truncate=False)

# COMMAND ----------

print("workspace.bronze.customers — sample rows:")
spark.sql(f"""
    SELECT customer_id, first_name, last_name, email, city, state, segment, signup_date
    FROM {catalog}.bronze.customers
    ORDER BY customer_id
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# Check the customer segment distribution
print("Customer segment distribution:")
spark.sql(f"""
    SELECT segment, COUNT(*) AS customer_count
    FROM {catalog}.bronze.customers
    GROUP BY segment
    ORDER BY customer_count DESC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample: Products Table
# MAGIC
# MAGIC The `products` table is our PRODUCT entity. We're looking for:
# MAGIC - 4 categories with multiple subcategories each
# MAGIC - Price variety across products
# MAGIC - All products having a valid SKU

# COMMAND ----------

print("workspace.bronze.products — sample rows:")
spark.sql(f"""
    SELECT product_id, sku, product_name, category, subcategory, brand, unit_price
    FROM {catalog}.bronze.products
    ORDER BY product_id
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# Check the product category/subcategory hierarchy
print("Product category and subcategory breakdown:")
spark.sql(f"""
    SELECT category, subcategory, COUNT(*) AS product_count
    FROM {catalog}.bronze.products
    GROUP BY category, subcategory
    ORDER BY category, subcategory
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample: Locations Table
# MAGIC
# MAGIC The `locations` table represents shipping destinations. We're looking for:
# MAGIC - Coverage across multiple US regions (Northeast, Southeast, Midwest, West)
# MAGIC - City/state/zip combinations

# COMMAND ----------

print("workspace.bronze.locations — sample rows:")
spark.sql(f"""
    SELECT location_id, city, state, zip_code, region, country
    FROM {catalog}.bronze.locations
    ORDER BY region, state, city
    LIMIT 15
""").show(truncate=False)

# COMMAND ----------

# Check region distribution
print("Location distribution by region:")
spark.sql(f"""
    SELECT region, COUNT(*) AS location_count, COUNT(DISTINCT state) AS state_count
    FROM {catalog}.bronze.locations
    GROUP BY region
    ORDER BY region
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample: Orders Table
# MAGIC
# MAGIC The `orders` table is the central event — the most important table in our source data. We're looking for:
# MAGIC - The date range (should span 3 years: 2022–2024)
# MAGIC - The 4 channels: Online, In-Store, Mobile, Phone
# MAGIC - The FK relationships: customer_id, product_id, location_id
# MAGIC - Numeric measures: quantity, unit_price, total_amount

# COMMAND ----------

print("workspace.bronze.orders — sample rows:")
spark.sql(f"""
    SELECT order_id, customer_id, product_id, location_id,
           order_date, quantity, unit_price, total_amount, channel, status
    FROM {catalog}.bronze.orders
    ORDER BY order_date
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# Check the date range and channel distribution
print("Orders — date range and channel distribution:")
spark.sql(f"""
    SELECT
        MIN(order_date) AS earliest_order,
        MAX(order_date) AS latest_order,
        COUNT(DISTINCT YEAR(order_date)) AS years_of_data
    FROM {catalog}.bronze.orders
""").show()

spark.sql(f"""
    SELECT channel, COUNT(*) AS order_count, ROUND(SUM(total_amount), 2) AS total_revenue
    FROM {catalog}.bronze.orders
    GROUP BY channel
    ORDER BY order_count DESC
""").show()

# COMMAND ----------

# Check the order status distribution
print("Order status distribution:")
spark.sql(f"""
    SELECT status, COUNT(*) AS count
    FROM {catalog}.bronze.orders
    GROUP BY status
    ORDER BY count DESC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample: Returns Table
# MAGIC
# MAGIC The `returns` table captures product returns. We're looking for:
# MAGIC - The FK links back to orders, customers, and products
# MAGIC - Return reason categories
# MAGIC - Return amounts (should be <= original order amount)

# COMMAND ----------

print("workspace.bronze.returns — sample rows:")
spark.sql(f"""
    SELECT return_id, order_id, customer_id, product_id,
           return_date, return_amount, reason
    FROM {catalog}.bronze.returns
    ORDER BY return_date
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# Check return reason distribution
print("Return reason distribution:")
spark.sql(f"""
    SELECT reason, COUNT(*) AS return_count, ROUND(SUM(return_amount), 2) AS total_refunded
    FROM {catalog}.bronze.returns
    GROUP BY reason
    ORDER BY return_count DESC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Check: Referential Integrity
# MAGIC
# MAGIC Before we move on to building Gold tables, let's do a quick referential integrity check. We want to make sure all the FK values in `orders` exist in the corresponding dimension tables. If they don't, our Gold fact table will have orphaned rows — rows that can't be joined to any dimension.

# COMMAND ----------

print("Referential integrity check on workspace.bronze.orders:")
print()

# Check: all customer_ids in orders exist in customers
orphaned_customers = spark.sql(f"""
    SELECT COUNT(*) AS orphaned_orders
    FROM {catalog}.bronze.orders o
    LEFT JOIN {catalog}.bronze.customers c ON o.customer_id = c.customer_id
    WHERE c.customer_id IS NULL
""").collect()[0][0]
print(f"  Orders with invalid customer_id: {orphaned_customers}")

# Check: all product_ids in orders exist in products
orphaned_products = spark.sql(f"""
    SELECT COUNT(*) AS orphaned_orders
    FROM {catalog}.bronze.orders o
    LEFT JOIN {catalog}.bronze.products p ON o.product_id = p.product_id
    WHERE p.product_id IS NULL
""").collect()[0][0]
print(f"  Orders with invalid product_id:  {orphaned_products}")

# Check: all location_ids in orders exist in locations
orphaned_locations = spark.sql(f"""
    SELECT COUNT(*) AS orphaned_orders
    FROM {catalog}.bronze.orders o
    LEFT JOIN {catalog}.bronze.locations l ON o.location_id = l.location_id
    WHERE l.location_id IS NULL
""").collect()[0][0]
print(f"  Orders with invalid location_id: {orphaned_locations}")

print()
if orphaned_customers == 0 and orphaned_products == 0 and orphaned_locations == 0:
    print("All referential integrity checks passed. Bronze data is clean.")
else:
    print("WARNING: Orphaned records found. Investigate before building Gold tables.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC In this notebook we:
# MAGIC 1. Explained Bronze layer principles: raw landing zone, append-only, never edit
# MAGIC 2. Documented the expected schema for all 5 source CSV files
# MAGIC 3. Loaded all 5 CSVs from the volume into Delta tables in `workspace.bronze.*`
# MAGIC 4. Verified the load with row counts and schema inspection
# MAGIC 5. Sampled each table to understand the data
# MAGIC 6. Ran referential integrity checks to confirm FKs are valid
# MAGIC
# MAGIC **Bronze table summary:**
# MAGIC - `workspace.bronze.customers` — customer master data
# MAGIC - `workspace.bronze.products` — product catalog
# MAGIC - `workspace.bronze.locations` — shipping destinations
# MAGIC - `workspace.bronze.orders` — order transactions (the central fact)
# MAGIC - `workspace.bronze.returns` — product returns
# MAGIC
# MAGIC **Next up — Notebook 08: dim_date.** We'll build the calendar dimension — one of the most important (and most often overlooked) tables in any data warehouse.
