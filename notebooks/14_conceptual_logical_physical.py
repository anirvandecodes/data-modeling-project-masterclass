# Databricks notebook source
# MAGIC %md
# MAGIC # 14 — Conceptual, Logical & Physical Data Modeling
# MAGIC
# MAGIC Every real data model goes through three stages before a single table is created.
# MAGIC Most people skip the first two and wonder why their physical model needs constant rework.
# MAGIC
# MAGIC | Stage | Question it answers | Audience | Tools |
# MAGIC |---|---|---|---|
# MAGIC | **Conceptual** | What entities and relationships exist? | Business stakeholders | Boxes and arrows, whiteboard |
# MAGIC | **Logical** | What columns, data types, and keys? | Data architects, engineers | ERD with attributes |
# MAGIC | **Physical** | How is it stored, partitioned, optimized? | DBAs, data engineers | DDL, Delta/Parquet, cluster keys |
# MAGIC
# MAGIC We'll walk through all three stages using our e-commerce project —
# MAGIC the same model you've been building, but this time from the very beginning.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## STAGE 1: Conceptual Data Model
# MAGIC
# MAGIC ### What it is
# MAGIC The highest-level view. No columns, no data types, no keys.
# MAGIC Just **entities** (the things) and **relationships** (how they connect).
# MAGIC Written in plain English, drawn on a whiteboard.
# MAGIC
# MAGIC ### Purpose
# MAGIC - Align with business stakeholders before touching any code
# MAGIC - Define the scope: what's IN the model, what's OUT
# MAGIC - Catch missing entities before you build anything
# MAGIC
# MAGIC ### How to build one
# MAGIC 1. List the nouns in the business process — those are your entities
# MAGIC 2. List the verbs between them — those are your relationships
# MAGIC 3. Draw it, validate it with a non-technical stakeholder
# MAGIC
# MAGIC ### Our E-Commerce Conceptual Model
# MAGIC
# MAGIC ```
# MAGIC Business process: Customer orders products, products get shipped, sometimes returned.
# MAGIC
# MAGIC Entities:
# MAGIC   CUSTOMER   — a person who buys from us
# MAGIC   PRODUCT    — an item we sell
# MAGIC   ORDER      — a purchase transaction
# MAGIC   LOCATION   — where an order is shipped to / fulfilled from
# MAGIC   RETURN     — a product sent back by a customer
# MAGIC
# MAGIC Relationships (plain English first):
# MAGIC   A CUSTOMER places one or many ORDERs
# MAGIC   An ORDER contains one or many PRODUCTs
# MAGIC   An ORDER is fulfilled from a LOCATION
# MAGIC   A CUSTOMER can make a RETURN
# MAGIC   A RETURN is for a specific PRODUCT
# MAGIC
# MAGIC Diagram (entities + relationships only, no attributes):
# MAGIC
# MAGIC   CUSTOMER ──── places ────> ORDER ──── fulfilled at ────> LOCATION
# MAGIC                                │
# MAGIC                           contains
# MAGIC                                │
# MAGIC                            PRODUCT
# MAGIC                                │
# MAGIC                         subject of
# MAGIC                                │
# MAGIC                             RETURN <──── made by ──── CUSTOMER
# MAGIC ```
# MAGIC
# MAGIC **What's intentionally OUT of scope:**
# MAGIC - Inventory levels (separate business process)
# MAGIC - Supplier relationships (separate system)
# MAGIC - Payments (out of scope for this model)
# MAGIC
# MAGIC Scope decisions made here save weeks of rework later.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## STAGE 2: Logical Data Model
# MAGIC
# MAGIC ### What it is
# MAGIC The conceptual model with detail added: **attributes, data types, primary keys,
# MAGIC foreign keys, and cardinality**. Still platform-independent — no partitioning,
# MAGIC no Delta, no Parquet. Just the structure.
# MAGIC
# MAGIC ### Purpose
# MAGIC - Precise enough for engineers to implement
# MAGIC - Platform-agnostic (works on Databricks, Postgres, Snowflake, BigQuery equally)
# MAGIC - The contract between data modeling and data engineering
# MAGIC
# MAGIC ### Our E-Commerce Logical Model (OLTP — normalized)
# MAGIC
# MAGIC ```
# MAGIC CUSTOMER
# MAGIC   customer_id     INT          PK
# MAGIC   first_name      VARCHAR(100) NOT NULL
# MAGIC   last_name       VARCHAR(100) NOT NULL
# MAGIC   email           VARCHAR(255) NOT NULL UNIQUE
# MAGIC   phone           VARCHAR(20)
# MAGIC   address         VARCHAR(300)
# MAGIC   city            VARCHAR(100)
# MAGIC   state           CHAR(2)
# MAGIC   country         CHAR(3)      DEFAULT 'US'
# MAGIC   channel         VARCHAR(20)  CHECK (channel IN ('web','mobile','in-store','phone'))
# MAGIC
# MAGIC PRODUCT
# MAGIC   product_id      INT          PK
# MAGIC   sku             VARCHAR(20)  NOT NULL UNIQUE
# MAGIC   product_name    VARCHAR(200) NOT NULL
# MAGIC   category        VARCHAR(100) NOT NULL
# MAGIC   subcategory     VARCHAR(100)
# MAGIC   brand           VARCHAR(100)
# MAGIC   unit_price      DECIMAL(10,2) NOT NULL
# MAGIC   cost_price      DECIMAL(10,2)
# MAGIC
# MAGIC LOCATION
# MAGIC   location_id     INT          PK
# MAGIC   city            VARCHAR(100)
# MAGIC   state           CHAR(2)
# MAGIC   country         CHAR(3)
# MAGIC   region          VARCHAR(50)
# MAGIC   postal_code     VARCHAR(10)
# MAGIC
# MAGIC ORDER
# MAGIC   order_id        INT          PK
# MAGIC   order_number    VARCHAR(20)  NOT NULL UNIQUE
# MAGIC   customer_id     INT          FK → CUSTOMER.customer_id
# MAGIC   location_id     INT          FK → LOCATION.location_id
# MAGIC   order_date      DATE         NOT NULL
# MAGIC   ship_date       DATE
# MAGIC
# MAGIC ORDER_LINE          ← weak entity (PK is composite)
# MAGIC   order_id          INT          PK + FK → ORDER.order_id
# MAGIC   line_item_id      INT          PK
# MAGIC   product_id        INT          FK → PRODUCT.product_id
# MAGIC   quantity          INT          NOT NULL CHECK (quantity > 0)
# MAGIC   unit_price        DECIMAL(10,2) NOT NULL
# MAGIC   discount_amount   DECIMAL(10,2) DEFAULT 0
# MAGIC
# MAGIC RETURN
# MAGIC   return_id         INT          PK
# MAGIC   order_id          INT          FK → ORDER.order_id
# MAGIC   customer_id       INT          FK → CUSTOMER.customer_id
# MAGIC   product_id        INT          FK → PRODUCT.product_id
# MAGIC   return_date       DATE         NOT NULL
# MAGIC   quantity_returned INT          NOT NULL
# MAGIC   unit_price        DECIMAL(10,2)
# MAGIC   return_reason     VARCHAR(200)
# MAGIC
# MAGIC Cardinality:
# MAGIC   CUSTOMER   1:M   ORDER
# MAGIC   ORDER      1:M   ORDER_LINE
# MAGIC   PRODUCT    1:M   ORDER_LINE
# MAGIC   LOCATION   1:M   ORDER
# MAGIC   CUSTOMER   1:M   RETURN
# MAGIC   PRODUCT    1:M   RETURN
# MAGIC   ORDER      1:M   RETURN
# MAGIC   PRODUCT    M:M   ORDER    (resolved via ORDER_LINE bridge)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Logical → Analytical: Translating to a Dimensional Model
# MAGIC
# MAGIC The logical OLTP model above is normalized. Before building Gold tables,
# MAGIC we translate it into a **dimensional (analytical) logical model**:
# MAGIC
# MAGIC ```
# MAGIC ANALYTICAL LOGICAL MODEL (dimensional)
# MAGIC
# MAGIC dim_customer
# MAGIC   customer_key        VARCHAR(32)  PK  (MD5 surrogate)
# MAGIC   source_customer_id  INT              (natural key from OLTP)
# MAGIC   first_name          VARCHAR(100)
# MAGIC   last_name           VARCHAR(100)
# MAGIC   email               VARCHAR(255)
# MAGIC   city                VARCHAR(100)
# MAGIC   state               CHAR(2)
# MAGIC   channel             VARCHAR(20)
# MAGIC   start_date          DATE             (SCD Type 2)
# MAGIC   end_date            DATE             (SCD Type 2)
# MAGIC   is_current          BOOLEAN          (SCD Type 2)
# MAGIC
# MAGIC dim_product
# MAGIC   product_key         VARCHAR(32)  PK  (MD5 surrogate)
# MAGIC   source_product_id   INT
# MAGIC   sku                 VARCHAR(20)
# MAGIC   product_name        VARCHAR(200)
# MAGIC   category            VARCHAR(100)     ← denormalized from OLTP hierarchy
# MAGIC   subcategory         VARCHAR(100)     ← denormalized
# MAGIC   brand               VARCHAR(100)
# MAGIC   unit_price          DECIMAL(10,2)
# MAGIC
# MAGIC dim_date
# MAGIC   date_key            INT          PK  (YYYYMMDD)
# MAGIC   full_date           DATE
# MAGIC   year                INT
# MAGIC   month               INT
# MAGIC   is_weekend          BOOLEAN
# MAGIC   fiscal_quarter      INT
# MAGIC   ... (all calendar attributes)
# MAGIC
# MAGIC dim_location
# MAGIC   location_key        VARCHAR(32)  PK  (MD5 surrogate)
# MAGIC   source_location_id  INT
# MAGIC   city, state, region VARCHAR
# MAGIC
# MAGIC fact_orders                          ← grain: one row per order line item
# MAGIC   order_line_key      VARCHAR(32)  PK  (MD5 of order_id + line_item_id)
# MAGIC   customer_key        VARCHAR(32)  FK → dim_customer
# MAGIC   product_key         VARCHAR(32)  FK → dim_product
# MAGIC   location_key        VARCHAR(32)  FK → dim_location
# MAGIC   order_date_key      INT          FK → dim_date  (role-playing)
# MAGIC   ship_date_key       INT          FK → dim_date  (role-playing)
# MAGIC   order_number        VARCHAR(20)      (degenerate dimension)
# MAGIC   quantity            INT
# MAGIC   unit_price          DECIMAL(10,2)
# MAGIC   discount_amount     DECIMAL(10,2)
# MAGIC   total_amount        DECIMAL(10,2)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## STAGE 3: Physical Data Model
# MAGIC
# MAGIC ### What it is
# MAGIC The logical model implemented on a **specific platform** with specific
# MAGIC storage decisions: file format, partitioning, clustering, compression,
# MAGIC indexing, table properties.
# MAGIC
# MAGIC ### Purpose
# MAGIC - Query performance at scale
# MAGIC - Storage efficiency
# MAGIC - Incremental load strategy (MERGE vs INSERT OVERWRITE)
# MAGIC - Decisions that only matter when data is large
# MAGIC
# MAGIC ### Physical decisions we make for Databricks Delta Lake

# COMMAND ----------

catalog = "workspace"

# MAGIC %md
# MAGIC #### Decision 1: File Format — Delta Lake
# MAGIC
# MAGIC We use **Delta Lake** (not raw Parquet, not CSV).
# MAGIC
# MAGIC Why Delta over plain Parquet?
# MAGIC - ACID transactions — concurrent reads and writes are safe
# MAGIC - Time travel — `SELECT * FROM table VERSION AS OF 5` for free
# MAGIC - Schema enforcement — bad data types are rejected at write time
# MAGIC - MERGE support — UPSERT is a first-class operation (critical for SCD Type 2)
# MAGIC - Z-ordering / liquid clustering — co-locate related data for fast reads
# MAGIC
# MAGIC All our Gold tables are Delta. This is non-negotiable on Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Decision 2: Partitioning
# MAGIC
# MAGIC Partitioning splits a table into folders by a column value.
# MAGIC A query with a filter on that column skips entire partitions — **partition pruning**.
# MAGIC
# MAGIC **When to partition:**
# MAGIC - Table is large (100M+ rows)
# MAGIC - You almost always filter by that column
# MAGIC - The column has reasonable cardinality (not too many unique values)
# MAGIC
# MAGIC **When NOT to partition:**
# MAGIC - Small tables — partitioning adds overhead with no benefit
# MAGIC - High-cardinality columns (user_id, order_id) — too many small files
# MAGIC
# MAGIC **Our physical decisions:**

# COMMAND ----------

# fact_orders is large and almost always filtered by date
# Partition by year+month — each partition = one month of data

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.fact_orders_partitioned
    USING DELTA
    PARTITIONED BY (order_year, order_month)
    AS
    SELECT
        f.*,
        d.year  AS order_year,
        d.month AS order_month
    FROM {catalog}.gold.fact_orders f
    JOIN {catalog}.gold.dim_date d ON f.order_date_key = d.date_key
""")

# Show how partition pruning works — only reads the partitions for 2024
spark.sql(f"""
    SELECT order_year, order_month, COUNT(*) AS rows, ROUND(SUM(total_amount), 2) AS revenue
    FROM {catalog}.gold.fact_orders_partitioned
    WHERE order_year = 2024
    GROUP BY order_year, order_month
    ORDER BY order_month
""").show()

print("With partitioning: Spark only reads 2024 folders, skips 2022 and 2023 entirely")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Decision 3: Z-Ordering / Liquid Clustering
# MAGIC
# MAGIC Z-ordering co-locates rows with similar values in the same files.
# MAGIC When you filter on a Z-ordered column, Delta skips entire files — **data skipping**.
# MAGIC
# MAGIC **Use Z-order on columns you filter by but don't partition on.**
# MAGIC
# MAGIC ```sql
# MAGIC OPTIMIZE workspace.gold.fact_orders
# MAGIC ZORDER BY (customer_key, product_key)
# MAGIC ```
# MAGIC
# MAGIC After Z-ordering:
# MAGIC - `WHERE customer_key = '...'` reads far fewer files
# MAGIC - `WHERE product_key = '...'` reads far fewer files
# MAGIC
# MAGIC **Liquid Clustering** (Databricks-specific, newer):
# MAGIC More flexible than Z-order — can change clustering columns without rewriting.
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE fact_orders
# MAGIC CLUSTER BY (customer_key, order_date_key)
# MAGIC ```

# COMMAND ----------

# Demo: run OPTIMIZE with ZORDER on fact_orders
spark.sql(f"""
    OPTIMIZE {catalog}.gold.fact_orders
    ZORDER BY (customer_key, product_key)
""")

print("Z-order applied — future queries filtering by customer_key or product_key will be faster")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Decision 4: Table Properties & Statistics
# MAGIC
# MAGIC Physical decisions that affect query planning:

# COMMAND ----------

# Enable column statistics for better query planning
spark.sql(f"""
    ANALYZE TABLE {catalog}.gold.fact_orders
    COMPUTE STATISTICS FOR ALL COLUMNS
""")

# Show the statistics collected
spark.sql(f"""
    DESCRIBE DETAIL {catalog}.gold.fact_orders
""").select("format", "numFiles", "sizeInBytes", "numRows").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Decision 5: Load Strategy — Full Refresh vs Incremental
# MAGIC
# MAGIC **Full Refresh (INSERT OVERWRITE / overwrite mode)**
# MAGIC - Truncate and reload the entire table every run
# MAGIC - Simple, always correct
# MAGIC - Works when: table is small, source is a full snapshot
# MAGIC - Our dimensions use this (they're small, always rebuilt from full source)
# MAGIC
# MAGIC **Incremental (MERGE)**
# MAGIC - Only process new/changed records
# MAGIC - Required when: table is large, source delivers change data
# MAGIC - SCD Type 2 uses MERGE — it's designed for incremental loads
# MAGIC
# MAGIC **Append-only (INSERT)**
# MAGIC - New records only, never update existing rows
# MAGIC - Works for immutable events — click logs, IoT streams, audit trails
# MAGIC - Most transactional fact tables use this
# MAGIC
# MAGIC | Table | Strategy | Why |
# MAGIC |---|---|---|
# MAGIC | `dim_date` | Full refresh | Static calendar, regenerated once |
# MAGIC | `dim_product` | Full refresh | Small, always full snapshot from source |
# MAGIC | `dim_customer` | Incremental MERGE | SCD Type 2 — must preserve history |
# MAGIC | `fact_orders` | TRUNCATE + INSERT | Full reload from Bronze each run |
# MAGIC | `fact_returns` | TRUNCATE + INSERT | Full reload from Bronze each run |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Putting It All Together: The 3-Stage Flow
# MAGIC
# MAGIC Here is the complete flow from business requirement to running table:
# MAGIC
# MAGIC ```
# MAGIC BUSINESS REQUIREMENT
# MAGIC   "We need to track customer orders and answer:
# MAGIC    total sales by category, top customers, sales by region"
# MAGIC         │
# MAGIC         ▼
# MAGIC CONCEPTUAL MODEL  (whiteboard, 30 minutes)
# MAGIC   Entities: Customer, Product, Order, Location
# MAGIC   Relationships: Customer places Order, Order contains Product, Order at Location
# MAGIC         │
# MAGIC         ▼
# MAGIC LOGICAL MODEL  (ERD with attributes, 2-4 hours)
# MAGIC   OLTP: normalized tables with PKs, FKs, data types, cardinality
# MAGIC   OLAP: denormalized star schema — fact_orders + 4 dims
# MAGIC   Decisions: grain = order line item, SCD Type 2 on customer
# MAGIC         │
# MAGIC         ▼
# MAGIC PHYSICAL MODEL  (DDL + optimization, ongoing)
# MAGIC   Platform: Databricks Delta Lake
# MAGIC   Format: Delta (ACID, time travel, schema enforcement)
# MAGIC   Partitioning: fact_orders by year+month
# MAGIC   Z-order: by customer_key, product_key
# MAGIC   Load strategy: dim = full refresh, fact = truncate+insert, customer = MERGE
# MAGIC   Statistics: ANALYZE TABLE for query planning
# MAGIC         │
# MAGIC         ▼
# MAGIC RUNNING PIPELINE
# MAGIC   Databricks Workflow DAG — Bronze → dims → facts → analytics
# MAGIC   Serverless compute, ~3 min end to end
# MAGIC ```
# MAGIC
# MAGIC **The key insight:**
# MAGIC Most data modeling mistakes happen when people skip directly to physical.
# MAGIC They build a table, it doesn't answer the right questions, they rebuild.
# MAGIC Conceptual and logical are cheap. Physical rework is expensive.
# MAGIC Do the thinking before you write the DDL.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Side-by-Side Comparison: Same Model at All Three Levels
# MAGIC
# MAGIC | | Conceptual | Logical | Physical |
# MAGIC |---|---|---|---|
# MAGIC | Shows | Entities + relationships | Attributes, types, keys | Storage, partitions, indexes |
# MAGIC | Audience | Business + tech | Data architects | Data engineers + DBAs |
# MAGIC | Platform | None | None | Databricks / Snowflake / BQ |
# MAGIC | Customer entity | Box labeled "CUSTOMER" | Table with 10 typed columns + PK | Delta table, Z-ordered, stats computed |
# MAGIC | Relationship | "places orders" (arrow) | FK: orders.customer_id → customers.customer_id | Broadcast join hint if dim is small |
# MAGIC | SCD | Not mentioned | start_date, end_date, is_current columns defined | Two-step MERGE, incremental load, OPTIMIZE after |
# MAGIC | Time to build | 30 min | 2–4 hours | Ongoing |
# MAGIC
# MAGIC **Interview tip:**
# MAGIC When asked to design a data model, always state which level you're working at.
# MAGIC "I'll start with a conceptual model to align on scope, then move to logical design."
# MAGIC This signals maturity. Most candidates jump straight to table columns.
