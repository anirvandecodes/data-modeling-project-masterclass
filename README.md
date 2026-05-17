# Practical Data Modeling Masterclass

A complete, hands-on data modeling project built on Databricks. Covers dimensional modeling (Kimball), the Medallion architecture, and every core concept from star schema design to slowly changing dimensions — with runnable notebooks and a fully orchestrated pipeline.

---

## What You'll Learn

- Turning business requirements into structured tables (facts + dimensions)
- OLTP vs OLAP — the key mental shift for analytics engineering
- Kimball dimensional modeling from scratch
- Star schema vs snowflake schema — when to use each
- All four fact table types: transactional, periodic snapshot, accumulating snapshot, factless
- MD5 hash surrogate keys — deterministic, distributed-friendly
- SCD Type 1 (overwrite) and SCD Type 2 (full history with start/end/is_current)
- Role-playing dimensions, degenerate dimensions, conformed dimensions
- Unknown member pattern for late-arriving data
- Load order and the lookup join pattern
- Business metrics: DAU, MAU, AOV, cohort retention, return rate
- Product-sense modeling: define metrics first, then design the model
- Common app data models: ride-hailing, food delivery, social media, library, cloud storage
- Databricks Workflow orchestration with task dependencies

---

## Domain

E-commerce / Retail — customers placing orders across products, locations, and channels, with a returns process on top.

**Business questions answered:**
1. What are total sales by product category per month?
2. Which customers are our top buyers?
3. How do sales vary by geography?
4. What is the average order value by channel?

---

## Architecture

```
Bronze (raw CSVs) → Silver (cleaned) → Gold (star schema)
```

```
dim_date ──────────────────────────────────────────────┐
  (order_date_key, ship_date_key — role-playing)        │
                                                        │
dim_customer (SCD Type 2) ──── fact_orders ──── dim_product
                                 │  order_number (degenerate)
dim_location ────────────────────┘

dim_customer (conformed) ──── fact_returns ──── dim_product (conformed)
dim_date     (conformed) ─────┘
```

---

## Folder Structure

```
data-modeling-project-masterclass/
├── data/
│   ├── raw/                        ← sample CSVs (200 customers, 40 products, 5K+ orders)
│   └── generate_data.py            ← script to regenerate sample data
├── notebooks/
│   ├── 00_setup.py                 ← create schemas and volume
│   ├── 01_bronze_ingest.py         ← land raw CSVs into Bronze Delta tables
│   ├── 02_dim_date.py              ← calendar dimension with flags
│   ├── 03_dim_product.py           ← product dimension, MD5 surrogate key, hierarchy
│   ├── 04_dim_location.py          ← location dimension, MD5 surrogate key
│   ├── 05a_dim_customer_scd1.py    ← SCD Type 1 — MERGE overwrite
│   ├── 05b_dim_customer_scd2.py    ← SCD Type 2 — full history, is_current
│   ├── 06_fact_orders.py           ← fact_orders, lookup join, role-playing, degenerate dim
│   ├── 07_fact_returns.py          ← fact_returns, conformed dimensions
│   ├── 08_analytics_queries.py     ← answer all 4 business questions + YoY growth
│   ├── 09_data_modeling_fundamentals.py  ← theory + 5 real-world system designs
│   ├── 10_fact_table_types.py      ← transactional, periodic snapshot, accumulating, factless
│   ├── 11_star_vs_snowflake.py     ← side-by-side comparison with runnable SQL
│   └── 12_metrics_and_product_sense.py  ← DAU, MAU, AOV, cohort retention, return rate
├── databricks.yml                  ← Databricks Asset Bundle (DAB) definition
└── MASTERCLASS_PLAN.md             ← full video plan with code for every segment
```

---

## Quickstart

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Databricks CLI configured (`databricks auth login`)

### Deploy and Run

```bash
# 1. Clone the repo
git clone <repo-url>
cd data-modeling-project-masterclass

# 2. Generate sample data
python3 data/generate_data.py

# 3. Upload CSVs to your Databricks volume
#    (create the volume first via 00_setup or CLI)
for f in customers products locations orders returns; do
  databricks fs cp data/raw/${f}.csv dbfs:/Volumes/workspace/bronze/raw_data/${f}.csv --overwrite
done

# 4. Deploy the bundle
databricks bundle deploy

# 5. Run the full pipeline
databricks bundle run data_modeling_pipeline
```

---

## Pipeline DAG

The Databricks Workflow enforces the correct load order — dimensions always before facts.

```
setup
  └── bronze_ingest
        ├── dim_date ──────────────────────────────────┐
        ├── dim_product ───────────────────────────────┤
        ├── dim_location ──────────────────────────────┤
        └── dim_customer_scd2 ────────────────────────►├── fact_orders ──► analytics
                                                       │                    └── metrics
                                                       └── fact_returns ─►
                                                                          advanced_facts
                                                       fact_orders ──────► snowflake_schema
```

---

## Catalog / Schema Layout

| Layer | Schema | Tables |
|---|---|---|
| Bronze | `workspace.bronze` | `customers`, `products`, `locations`, `orders`, `returns` |
| Gold | `workspace.gold` | `dim_date`, `dim_product`, `dim_location`, `dim_customer`, `dim_category`, `dim_subcategory` |
| Gold | `workspace.gold` | `fact_orders`, `fact_returns`, `fact_customer_monthly_snapshot`, `fact_order_fulfillment`, `fact_promotions` |

---

## Key Concepts by Notebook

| Notebook | Concepts |
|---|---|
| `02_dim_date` | Calendar dimension, date flags, `is_weekend`, `is_month_end`, fiscal periods |
| `03_dim_product` | MD5 surrogate key, natural hierarchy, unknown member row |
| `05a_dim_customer_scd1` | SCD Type 1, MERGE overwrite, when history doesn't matter |
| `05b_dim_customer_scd2` | SCD Type 2, two-step MERGE, `start_date`/`end_date`/`is_current` |
| `06_fact_orders` | Grain declaration, lookup join pattern, role-playing dimensions, degenerate dimension |
| `07_fact_returns` | Conformed dimensions, cross-fact-table analytics |
| `09_data_modeling_fundamentals` | OLTP vs OLAP, problem-solving approach, ride-hailing, food delivery, social media, library, cloud storage schemas |
| `10_fact_table_types` | Transactional, periodic snapshot (semi-additive measures), accumulating snapshot (milestone pipeline), factless facts |
| `11_star_vs_snowflake` | Side-by-side build and query comparison, when to normalize |
| `12_metrics_and_product_sense` | DAU, MAU, stickiness, AOV, cohort retention, return rate — define metrics first, design model second |

---

## Platform

Built on Databricks with serverless compute. All SQL is standard — the concepts apply directly to Snowflake, BigQuery, and Redshift.
