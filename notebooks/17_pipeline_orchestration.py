# Databricks notebook source

# MAGIC %md
# MAGIC # Pipeline Orchestration — Load Order & Databricks Workflow
# MAGIC
# MAGIC **Notebook 17 of the Data Modeling Masterclass (Final Notebook)**
# MAGIC
# MAGIC We've built the complete star schema manually, notebook by notebook. In production, these notebooks need to run automatically, in the correct order, on a schedule — without human intervention. This notebook covers how to orchestrate the full pipeline using Databricks Workflows and Asset Bundles.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Most Common Pipeline Mistake
# MAGIC
# MAGIC The single most common error when first building a data pipeline is **loading fact tables before their dimensions**.
# MAGIC
# MAGIC Here's what happens:
# MAGIC
# MAGIC 1. You run `fact_orders` first (maybe because it alphabetically sorts before `dim_`)
# MAGIC 2. `fact_orders` does `LEFT JOIN dim_customer dc ON src.customer_id = dc.source_customer_id`
# MAGIC 3. But `dim_customer` doesn't exist yet (or has no rows from this run)
# MAGIC 4. Every `LEFT JOIN` returns NULL, which falls to `'unknown'`
# MAGIC 5. Your entire fact table has `customer_key = 'unknown'` for every row
# MAGIC 6. You don't notice until a report shows "Unknown" for 100% of customers
# MAGIC
# MAGIC This is a silent failure. The pipeline "succeeds" — no errors. But the data is wrong.
# MAGIC
# MAGIC **The rule: always load dimensions before facts.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Load Sequence
# MAGIC
# MAGIC Here is the dependency graph for our pipeline:
# MAGIC
# MAGIC ```
# MAGIC Bronze Ingest (source tables in workspace.bronze.*)
# MAGIC         │
# MAGIC         ├─── dim_date        (no dependencies — static calendar)
# MAGIC         │
# MAGIC         ├─── dim_product     (depends on: bronze.products)
# MAGIC         │
# MAGIC         ├─── dim_location    (depends on: bronze.locations)
# MAGIC         │
# MAGIC         └─── dim_customer    (depends on: bronze.customers, SCD2 logic)
# MAGIC                    │
# MAGIC               [All 4 dims ready]
# MAGIC                    │
# MAGIC         ├─── fact_orders     (depends on: dim_customer, dim_product,
# MAGIC         │                                 dim_location, dim_date)
# MAGIC         │
# MAGIC         └─── fact_returns    (depends on: dim_customer, dim_product, dim_date)
# MAGIC                    │
# MAGIC               [Both facts ready]
# MAGIC                    │
# MAGIC         ├─── analytics       (depends on: fact_orders, fact_returns, all dims)
# MAGIC         │
# MAGIC         └─── metrics         (depends on: fact_orders, fact_returns, all dims)
# MAGIC ```
# MAGIC
# MAGIC Notice that `dim_date`, `dim_product`, `dim_location`, and `dim_customer` can run in **parallel** — they have no dependencies on each other. This is a key optimization: parallelizing dimension loads cuts wall-clock time significantly.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Sequence Table
# MAGIC
# MAGIC | Step | Wave | Task | Depends On | Estimated Runtime |
# MAGIC |------|------|------|-----------|-------------------|
# MAGIC | 1 | Bronze | Bronze ingest | Source systems | ~1 min |
# MAGIC | 2a | Dims | dim_date | Bronze (none this run) | ~30 sec |
# MAGIC | 2b | Dims | dim_product | bronze.products | ~30 sec |
# MAGIC | 2c | Dims | dim_location | bronze.locations | ~20 sec |
# MAGIC | 2d | Dims | dim_customer | bronze.customers | ~45 sec |
# MAGIC | 3a | Facts | fact_orders | All dims (wave 2) | ~60 sec |
# MAGIC | 3b | Facts | fact_returns | dim_customer, dim_product, dim_date | ~30 sec |
# MAGIC | 4a | Analytics | analytics queries | fact_orders, fact_returns | ~30 sec |
# MAGIC | 4b | Analytics | business metrics | fact_orders, fact_returns | ~30 sec |
# MAGIC
# MAGIC **Total with parallelism:** ~3.5 minutes end-to-end on serverless compute.
# MAGIC **Total sequential (naively):** ~6 minutes. Parallelism cuts it nearly in half.

# COMMAND ----------

catalog = "workspace"

load_sequence = [
    (1,  "Bronze",    "01_bronze_ingest",            "Source systems",                       "~60s"),
    (2,  "Dims",      "08_dim_date",                 "Bronze (no deps this run)",             "~30s"),
    (2,  "Dims",      "09_dim_product",              "bronze.products",                       "~30s"),
    (2,  "Dims",      "10_dim_location",             "bronze.locations",                      "~20s"),
    (2,  "Dims",      "12_dim_customer_scd2",        "bronze.customers",                      "~45s"),
    (3,  "Facts",     "14_fact_orders",              "dim_customer, dim_product, dim_location, dim_date", "~60s"),
    (3,  "Facts",     "15_fact_returns",             "dim_customer, dim_product, dim_date",   "~30s"),
    (4,  "Analytics", "16_analytics_and_metrics",    "fact_orders, fact_returns, all dims",   "~30s"),
]

df = spark.createDataFrame(load_sequence, ["wave", "layer", "notebook", "depends_on", "est_runtime"])
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Late-Arriving Dimensions
# MAGIC
# MAGIC What happens when a fact record arrives before its dimension record?
# MAGIC
# MAGIC **Scenario:** A new product is launched. Orders start coming in at 9:00am. The product catalog sync runs at 9:30am. There are 30 minutes of orders with a `product_id` that doesn't exist in `dim_product` yet.
# MAGIC
# MAGIC Without proper design, those 30 minutes of revenue would be lost (INNER JOIN) or show as NULL (LEFT JOIN without unknown member).
# MAGIC
# MAGIC **Our design handles this gracefully:**
# MAGIC 1. `LEFT JOIN + COALESCE('unknown')` catches unresolvable product keys
# MAGIC 2. Those fact rows get `product_key = 'unknown'` — they're not lost, they're flagged
# MAGIC 3. When `dim_product` is loaded at 9:30am, the product now exists
# MAGIC 4. A **backfill** reruns `fact_orders` for the affected time window
# MAGIC 5. The `TRUNCATE + INSERT` pattern (or a MERGE-based incremental) correctly resolves the keys
# MAGIC 6. The "Unknown" rows disappear, replaced by properly resolved keys
# MAGIC
# MAGIC **This is why every dimension has an unknown member row.** It's not just for data quality visibility — it's the mechanism that makes late-arriving dimension handling possible without data loss.

# COMMAND ----------

# Simulate the late-arriving dimension scenario conceptually
print("Late-Arriving Dimension Handling:")
print("="*60)
print()
print("T+0:00  New product P999 launched in source system")
print("T+0:05  Orders start arriving for P999")
print("T+0:30  Dim pipeline runs: dim_product DOES NOT have P999 yet")
print("        → Orders for P999 land in fact with product_key='unknown'")
print()
print("T+1:00  Product catalog sync runs: P999 now in bronze.products")
print("T+1:05  dim_product pipeline runs: P999 now has a surrogate key")
print()
print("T+2:00  fact_orders backfill runs for affected time window")
print("        → P999 rows now resolve to proper product_key")
print("        → 'Unknown' category disappears from reports")
print()
print("Result: NO DATA LOST. Temporary 'Unknown' rows cleaned up by backfill.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Asset Bundle (DAB): Define the Pipeline as Code
# MAGIC
# MAGIC A Databricks Asset Bundle (DAB) is a YAML configuration file that defines your entire workflow — all tasks, their dependencies, their compute settings, and their schedule — as version-controlled code.
# MAGIC
# MAGIC Benefits:
# MAGIC - **Version controlled:** The pipeline definition lives in Git. Rollback, branch, PR review.
# MAGIC - **Environment promotion:** Same bundle deploys to dev, staging, and prod — just with different variable substitutions.
# MAGIC - **Code review:** Infrastructure changes go through the same review process as code changes.
# MAGIC - **Reproducibility:** Every deployment is identical and auditable.

# COMMAND ----------

dab_yaml = """
# databricks.yml — Data Modeling Masterclass Pipeline
# Deploy with: databricks bundle deploy
# Run with:    databricks bundle run data_modeling_pipeline

bundle:
  name: data-modeling-masterclass

variables:
  catalog:
    default: workspace
    description: "Unity Catalog catalog name"
  env:
    default: dev
    description: "Environment: dev, staging, prod"

targets:
  dev:
    mode: development
    default: true
    variables:
      catalog: workspace
      env: dev

  prod:
    mode: production
    variables:
      catalog: workspace
      env: prod

resources:
  jobs:
    data_modeling_pipeline:
      name: "Data Modeling Masterclass — ${var.env}"
      description: "E-commerce star schema pipeline: Bronze → Dims → Facts → Analytics"

      # Serverless compute — zero cluster startup time
      job_clusters: []

      # Run daily at 6am UTC
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: "UTC"
        pause_status: UNPAUSED

      # Email on failure
      email_notifications:
        on_failure:
          - data-engineering-alerts@company.com

      tasks:

        # Wave 1: Bronze ingest
        - task_key: bronze_ingest
          notebook_task:
            notebook_path: ./notebooks/07_bronze_ingest.py
            base_parameters:
              catalog: ${var.catalog}
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 2
            enable_elastic_disk: true

        # Wave 2: Dimensions (all parallel — no depends_on each other)
        - task_key: dim_date
          depends_on:
            - task_key: bronze_ingest
          notebook_task:
            notebook_path: ./notebooks/08_dim_date.py
            base_parameters:
              catalog: ${var.catalog}
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 1

        - task_key: dim_product
          depends_on:
            - task_key: bronze_ingest
          notebook_task:
            notebook_path: ./notebooks/09_dim_product.py
            base_parameters:
              catalog: ${var.catalog}
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 1

        - task_key: dim_location
          depends_on:
            - task_key: bronze_ingest
          notebook_task:
            notebook_path: ./notebooks/10_dim_location.py
            base_parameters:
              catalog: ${var.catalog}
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 1

        - task_key: dim_customer
          depends_on:
            - task_key: bronze_ingest
          notebook_task:
            notebook_path: ./notebooks/12_dim_customer_scd2.py
            base_parameters:
              catalog: ${var.catalog}
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 2

        # Wave 3: Facts (depend on ALL dimensions)
        - task_key: fact_orders
          depends_on:
            - task_key: dim_date
            - task_key: dim_product
            - task_key: dim_location
            - task_key: dim_customer
          notebook_task:
            notebook_path: ./notebooks/14_fact_orders.py
            base_parameters:
              catalog: ${var.catalog}
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 4
            autoscale:
              min_workers: 2
              max_workers: 8

        - task_key: fact_returns
          depends_on:
            - task_key: dim_date
            - task_key: dim_product
            - task_key: dim_customer
          notebook_task:
            notebook_path: ./notebooks/15_fact_returns.py
            base_parameters:
              catalog: ${var.catalog}
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 2

        # Wave 4: Analytics (depend on BOTH facts)
        - task_key: analytics_and_metrics
          depends_on:
            - task_key: fact_orders
            - task_key: fact_returns
          notebook_task:
            notebook_path: ./notebooks/16_analytics_and_metrics.py
            base_parameters:
              catalog: ${var.catalog}
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 2
"""

print(dab_yaml)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploying and Running the Pipeline
# MAGIC
# MAGIC With the `databricks.yml` file saved to your project root, the full deployment and run is three commands:
# MAGIC
# MAGIC ```bash
# MAGIC # 1. Validate the bundle — catches YAML syntax errors, missing notebooks, bad references
# MAGIC databricks bundle validate
# MAGIC
# MAGIC # 2. Deploy to the target environment (default: dev)
# MAGIC databricks bundle deploy
# MAGIC
# MAGIC # 3. Run the pipeline immediately (outside the schedule)
# MAGIC databricks bundle run data_modeling_pipeline
# MAGIC
# MAGIC # Deploy and run to prod
# MAGIC databricks bundle deploy --target prod
# MAGIC databricks bundle run data_modeling_pipeline --target prod
# MAGIC ```
# MAGIC
# MAGIC After `bundle deploy`, the workflow is visible in the Databricks UI under **Workflows → Jobs**. You can monitor runs, see task-level logs, and trigger reruns from there.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Performance on Serverless Compute
# MAGIC
# MAGIC One of the key advantages of running this pipeline on Databricks is **serverless compute** — there's no cluster startup time. When a task is ready to run, compute is available instantly.
# MAGIC
# MAGIC Traditional cluster-based pipelines have a 3-5 minute cluster startup overhead per task. With 9 tasks in our pipeline, that's up to 45 minutes of cluster startup time alone.
# MAGIC
# MAGIC With serverless:
# MAGIC | Wave | Tasks | Wall-clock Time |
# MAGIC |------|-------|----------------|
# MAGIC | Bronze ingest | 1 | ~60s |
# MAGIC | Dimensions (parallel) | 4 | ~45s (longest dim) |
# MAGIC | Facts (parallel) | 2 | ~60s (longest fact) |
# MAGIC | Analytics | 1 | ~30s |
# MAGIC | **Total** | **8** | **~3.5 minutes** |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Recap: The Complete Star Schema

# COMMAND ----------

tables_built = [
    ("workspace.gold.dim_date",                     "Calendar dimension — 10+ years of dates with flags (holiday, weekend, fiscal quarter)"),
    ("workspace.gold.dim_product",                  "Product dimension — MD5 surrogate key, category/subcategory hierarchy, unknown member"),
    ("workspace.gold.dim_location",                 "Location dimension — city/state/country/region, unknown member"),
    ("workspace.gold.dim_customer",                 "Customer dimension — SCD Type 2, full history of city/channel changes"),
    ("workspace.gold.dim_customer_scd1",            "Customer dimension — SCD Type 1 variant for comparison (overwrites changes)"),
    ("workspace.gold.fact_orders",                  "Transactional fact — one row per order line item, all 4 dims + role-playing date"),
    ("workspace.gold.fact_returns",                 "Transactional fact — returns, uses conformed dims shared with fact_orders"),
    ("workspace.gold.fact_customer_monthly_snapshot","Periodic snapshot fact — monthly customer spend summary"),
    ("workspace.gold.fact_order_fulfillment",       "Accumulating snapshot fact — order fulfillment with milestone dates"),
    ("workspace.gold.fact_promotions",              "Factless fact — which products were on promotion on which dates"),
]

df_tables = spark.createDataFrame(tables_built, ["table", "description"])
df_tables.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify All Tables Exist and Have Data

# COMMAND ----------

for table, _ in tables_built:
    try:
        count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {table}").collect()[0]["cnt"]
        status = "✓" if count > 0 else "✗ EMPTY"
        print(f"  {status}  {table}: {count:,} rows")
    except Exception as e:
        print(f"  ✗ MISSING  {table}: {str(e)[:60]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## The Complete Checklist
# MAGIC
# MAGIC Everything covered in this 17-notebook series:
# MAGIC
# MAGIC **Foundations**
# MAGIC - [x] OLTP vs OLAP — why analytical workloads need a different data model
# MAGIC - [x] Conceptual data model — entities, relationships, before any technology
# MAGIC - [x] Relationship types — 1:1, 1:M, M:M (resolved via bridge table), recursive
# MAGIC - [x] Logical data model — cardinality, normalization, business rules
# MAGIC - [x] Star schema vs snowflake — the tradeoffs, when to choose each
# MAGIC
# MAGIC **Physical Setup**
# MAGIC - [x] Delta Lake fundamentals — ACID transactions, time travel, OPTIMIZE
# MAGIC - [x] Partitioning strategy — partition by date for time-series fact tables
# MAGIC - [x] Z-ordering — co-locate data by commonly filtered columns
# MAGIC - [x] Catalog, schema, table structure — Unity Catalog hierarchy
# MAGIC
# MAGIC **Bronze Layer**
# MAGIC - [x] Bronze ingest pattern — raw data preserved as-is in Delta
# MAGIC - [x] Idempotent loads — TRUNCATE + INSERT for full reload safety
# MAGIC
# MAGIC **Dimension Design**
# MAGIC - [x] dim_date — built from scratch, fiscal periods, holiday flags, date spine
# MAGIC - [x] Surrogate keys — MD5 hash, why not natural keys, three alternatives
# MAGIC - [x] Unknown member row — absorbs unresolvable FK references gracefully
# MAGIC - [x] dim_product — MD5 key, category hierarchy denormalized, unknown member
# MAGIC - [x] dim_location — simple static dimension, same MD5 pattern
# MAGIC - [x] SCD Type 1 — MERGE upsert, overwrites old values, no history
# MAGIC - [x] SCD Type 2 — two-step MERGE, full history, start/end date, is_current
# MAGIC - [x] dim_customer — SCD2 with mixed Type1/Type2 columns
# MAGIC - [x] Tracked vs non-tracked attributes — what triggers new rows vs overwrites
# MAGIC
# MAGIC **Fact Table Design**
# MAGIC - [x] Grain declaration — the most important design decision
# MAGIC - [x] Three measure types — additive, semi-additive, non-additive
# MAGIC - [x] Four fact table types — transactional, periodic snapshot, accumulating snapshot, factless
# MAGIC - [x] fact_orders — lookup join pattern, role-playing dim_date, degenerate dimension
# MAGIC - [x] fact_returns — conformed dimensions shared across fact tables
# MAGIC - [x] LEFT JOIN + COALESCE — no rows dropped, unknown member catches misses
# MAGIC - [x] Row count verification — Bronze count = Gold count
# MAGIC
# MAGIC **Analytics**
# MAGIC - [x] Gold-only queries — no Bronze access from analytics layer
# MAGIC - [x] Category revenue trends — fact + dim_product + dim_date
# MAGIC - [x] Top customers by LTV — fact + dim_customer
# MAGIC - [x] Geographic sales — fact + dim_location
# MAGIC - [x] AOV by channel — fact + dim_customer + dim_date
# MAGIC - [x] YoY growth — CTE self-join pattern
# MAGIC - [x] DAB (Daily Active Buyers) — distinct buyers per day
# MAGIC - [x] MAB + Stickiness — monthly buyers + DAB/MAB ratio
# MAGIC - [x] Cohort retention — acquisition cohort, offset months, retention %
# MAGIC - [x] Return rate by category — cross-fact-table query with conformed dim_product
# MAGIC
# MAGIC **Pipeline & Orchestration**
# MAGIC - [x] Load order dependency — dimensions before facts
# MAGIC - [x] Parallel dimension loading — 4 dims run simultaneously
# MAGIC - [x] Late-arriving dimension handling — unknown member + backfill pattern
# MAGIC - [x] Databricks Asset Bundle — workflow as code in databricks.yml
# MAGIC - [x] Environment promotion — dev/staging/prod via bundle targets
# MAGIC - [x] Serverless compute — zero cluster startup, ~3.5 min total runtime
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **The star schema is complete. All 10 Gold tables are built and loaded. The pipeline is defined as code and ready to deploy.**
# MAGIC
# MAGIC You now have a production-grade dimensional model for an e-commerce business, built entirely on Databricks with Delta Lake and Unity Catalog. Every concept — from conceptual modeling to pipeline orchestration — has been demonstrated with running code on real data.
