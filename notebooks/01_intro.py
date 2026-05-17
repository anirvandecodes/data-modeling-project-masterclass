# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 01 — What is Data Modeling & What We're Building
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Starting Point
# MAGIC
# MAGIC Imagine you work at an e-commerce company.
# MAGIC
# MAGIC One day your manager comes to you and says:
# MAGIC
# MAGIC > **"We have been collecting data for years — orders, customers, products, returns. But we cannot really use any of it. Can you build us a proper data system?"**
# MAGIC
# MAGIC That is it. No detailed requirements. Just a pile of raw data and a business that needs answers.
# MAGIC
# MAGIC This is the project we are going to build together — from a blank slate to a fully working data system that can answer real business questions in seconds.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is Data Modeling?
# MAGIC
# MAGIC Data modeling is the process of translating **business requirements** into **structured tables** that are accurate, consistent, and fast to query.
# MAGIC
# MAGIC It happens in three stages:
# MAGIC
# MAGIC | Stage | What it defines | Audience |
# MAGIC |---|---|---|
# MAGIC | **Conceptual** | What entities exist and how they relate — no columns, no types | Business stakeholders, analysts |
# MAGIC | **Logical** | What attributes each entity has, with data types, primary keys, and foreign keys | Data engineers, architects |
# MAGIC | **Physical** | How tables are stored — file format, partitioning, indexing, load strategy | Data engineers, platform teams |
# MAGIC
# MAGIC We will work through all three stages in this masterclass. By the end, you will have designed and built a production-quality star schema on Databricks from scratch.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## OLTP vs OLAP — The Key Mental Shift
# MAGIC
# MAGIC Most data starts in an **OLTP** system (Online Transaction Processing) — a database designed to record individual business events quickly and reliably. Your order management system, your CRM, your point-of-sale system — all OLTP.
# MAGIC
# MAGIC But analysts don't need to record events. They need to **aggregate thousands of events** to answer questions like "What was revenue in Q3?" or "Which product category is declining?". That requires a completely different design philosophy: **OLAP** (Online Analytical Processing).
# MAGIC
# MAGIC | Property | OLTP | OLAP |
# MAGIC |---|---|---|
# MAGIC | **Purpose** | Record individual transactions fast | Analyze large volumes of historical data |
# MAGIC | **Design goal** | Eliminate redundancy (normalized) | Eliminate joins (denormalized) |
# MAGIC | **Typical query** | `SELECT * FROM orders WHERE order_id = 12345` | `SELECT category, SUM(revenue) FROM ... GROUP BY category` |
# MAGIC | **Row volume per query** | 1–100 rows | Millions of rows |
# MAGIC | **Schema style** | Many narrow tables, lots of joins | Few wide tables, few joins |
# MAGIC | **Updates** | Constant inserts, updates, deletes | Mostly bulk inserts, rarely updated |
# MAGIC | **Users** | Applications, services | Analysts, BI tools, data scientists |
# MAGIC | **Example systems** | MySQL, PostgreSQL, Oracle | Databricks, Snowflake, BigQuery, Redshift |
# MAGIC
# MAGIC The mistake most teams make is trying to run analytics directly on their OLTP database. It works when the company is small. It falls apart at scale — both in performance and in the "why don't my numbers match?" problem above.
# MAGIC
# MAGIC **This masterclass is about designing the OLAP layer.** We take raw OLTP-style data and reshape it into an analytical model that is fast, consistent, and easy to query.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## The 4 Business Questions We Will Answer
# MAGIC
# MAGIC By the end of this series, our data model will be able to answer these four questions with simple, fast SQL queries:
# MAGIC
# MAGIC ### Question 1 — Total Sales by Product Category per Month
# MAGIC > "How much revenue did each product category generate each month over the past 3 years?"
# MAGIC
# MAGIC This requires: a fact table with revenue, a date dimension with month/year, a product dimension with category.
# MAGIC
# MAGIC ### Question 2 — Top Customers by Lifetime Value
# MAGIC > "Who are our top 20 customers by total spend, and what is their average order value?"
# MAGIC
# MAGIC This requires: a fact table with order amounts, a customer dimension with customer details.
# MAGIC
# MAGIC ### Question 3 — Sales by Geography
# MAGIC > "Which cities and states are generating the most revenue?"
# MAGIC
# MAGIC This requires: a fact table with revenue, a location dimension with city, state, and region.
# MAGIC
# MAGIC ### Question 4 — Average Order Value by Channel
# MAGIC > "Do customers who buy online spend more or less per order than in-store customers?"
# MAGIC
# MAGIC This requires: a fact table with order amount and channel, aggregated and compared.
# MAGIC
# MAGIC All four questions will be answered in the final notebook of this series using clean, readable SQL — because we built the model right.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Medallion Architecture: Bronze → Silver → Gold
# MAGIC
# MAGIC Before we design our analytical tables, we need to understand where data comes from and how it moves through the system. Databricks recommends the **Medallion Architecture** — a layered approach where data gets progressively cleaner and more structured as it moves from raw ingestion to analytics-ready.
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                    MEDALLION ARCHITECTURE                       │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC
# MAGIC   ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
# MAGIC   │    BRONZE    │ ───► │    SILVER    │ ───► │     GOLD     │
# MAGIC   │              │      │              │      │              │
# MAGIC   │  Raw landing │      │   Cleaned &  │      │   Modeled &  │
# MAGIC   │  zone. Data  │      │  deduplicated│      │  aggregated. │
# MAGIC   │  exactly as  │      │  data. Types │      │  Star schema │
# MAGIC   │  it arrived. │      │  validated.  │      │  for BI and  │
# MAGIC   │  Never edit. │      │  Nulls       │      │  analytics.  │
# MAGIC   │              │      │  handled.    │      │              │
# MAGIC   │ Raw CSVs →   │      │ 1:1 with     │      │  Dims +      │
# MAGIC   │ Delta tables │      │ Bronze but   │      │  Facts       │
# MAGIC   │              │      │ trusted      │      │              │
# MAGIC   └──────────────┘      └──────────────┘      └──────────────┘
# MAGIC
# MAGIC   workspace.bronze.*    workspace.silver.*    workspace.gold.*
# MAGIC ```
# MAGIC
# MAGIC ### Why three layers?
# MAGIC
# MAGIC - **Bronze** is your safety net. If anything goes wrong downstream, you replay from Bronze. It is append-only, never edited. Think of it as an audit log of every piece of data you ever received.
# MAGIC - **Silver** is your trusted single source of truth. Duplicates removed, types validated, null handling applied. Business logic lives here.
# MAGIC - **Gold** is optimized for analytics. Tables are denormalized, aggregated, and structured specifically for the questions analysts ask most often.
# MAGIC
# MAGIC In this masterclass, we focus on Bronze ingest and Gold modeling. We'll keep Silver light — our source data is clean enough to go nearly direct from Bronze to Gold.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Schema We Are Building
# MAGIC
# MAGIC A **star schema** puts one fact table at the center, surrounded by dimension tables. Here is the shape of what we are building — two fact tables, four shared dimension tables. Don't worry about the column details yet. By Notebook 04, you will have designed every table yourself from first principles, and this diagram will make complete sense.
# MAGIC
# MAGIC ```
# MAGIC                        workspace.gold.*
# MAGIC
# MAGIC                       ┌──────────────┐
# MAGIC                       │   dim_date   │
# MAGIC                       └──────┬───────┘
# MAGIC                              │
# MAGIC  ┌───────────────┐           │           ┌───────────────┐
# MAGIC  │  dim_customer │           │           │  dim_product  │
# MAGIC  └───────┬───────┘           │           └───────┬───────┘
# MAGIC          │                   │                   │
# MAGIC          └──────────┬────────┴───────────────────┘
# MAGIC                     │
# MAGIC             ┌───────┴────────┐        ┌────────────────┐
# MAGIC             │  fact_orders   │        │  dim_location  │
# MAGIC             └───────┬────────┘        └───────┬────────┘
# MAGIC                     │                         │
# MAGIC             ┌───────┴────────┐                │
# MAGIC             │  fact_returns  │◄───────────────┘
# MAGIC             └────────────────┘
# MAGIC ```
# MAGIC
# MAGIC The **full schema diagram** — every table with all columns, data types, primary keys, foreign keys, and relationship annotations — is revealed in **Notebook 04** after you have built the conceptual and logical models. That is the right moment to see it, because by then you will understand every decision it encodes.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## The E-Commerce Scenario
# MAGIC
# MAGIC Throughout this masterclass, we work with a realistic e-commerce dataset representing 3 years of operations (2022–2024):
# MAGIC
# MAGIC | Entity | Volume | Details |
# MAGIC |---|---|---|
# MAGIC | **Customers** | ~200 | Segmented into Bronze, Silver, Gold tiers |
# MAGIC | **Products** | 40 | Across 4 categories: Electronics, Clothing, Home & Garden, Sports |
# MAGIC | **Locations** | ~50 | US cities across 4 regions: Northeast, Southeast, Midwest, West |
# MAGIC | **Orders** | 5,000+ | Spanning Jan 2022 – Dec 2024 |
# MAGIC | **Channels** | 4 | Online, In-Store, Mobile, Phone |
# MAGIC | **Returns** | ~500 | With reason codes |
# MAGIC
# MAGIC The data lives in `workspace.bronze.*` — five tables: `customers`, `products`, `locations`, `orders`, `returns`.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Masterclass Roadmap
# MAGIC
# MAGIC Here is what each notebook covers and what you will learn and build:
# MAGIC
# MAGIC | Notebook | Title | What You Learn | What You Build |
# MAGIC |---|---|---|---|
# MAGIC | **01** | Intro & What We're Building | Data modeling overview, OLTP vs OLAP, Medallion architecture | Nothing yet — set the stage |
# MAGIC | **02** | Conceptual Modeling | Entities, relationships, ERD notation | ASCII ERD of our e-commerce domain |
# MAGIC | **03** | Relationship Types | 1:1, 1:M, M:M, recursive relationships | Live SQL queries proving each relationship type |
# MAGIC | **04** | Logical Modeling | Attributes, data types, PKs, FKs, OLTP→OLAP translation | Full logical model with all columns defined |
# MAGIC | **05** | Star vs Snowflake Schema | Star schema anatomy, snowflake comparison, when to use each | Snowflake demo tables + comparison queries |
# MAGIC | **06** | Physical Setup | Delta Lake, partitioning, Z-ordering, load strategies | Schemas and volumes in Databricks |
# MAGIC | **07** | Bronze Ingest | Medallion architecture in practice, loading raw data | All 5 Bronze Delta tables |
# MAGIC | **08** | dim_date | Why date dimensions exist, pre-computing calendar attributes | `workspace.gold.dim_date` with 2,192 rows |
# MAGIC | **09** | dim_customer | SCDs (Type 1 vs Type 2), surrogate keys | `workspace.gold.dim_customer` |
# MAGIC | **10** | dim_product | Hierarchies in dimensions, flattening | `workspace.gold.dim_product` |
# MAGIC | **11** | dim_location | Geographic hierarchies | `workspace.gold.dim_location` |
# MAGIC | **12** | fact_orders | Grain definition, additive vs semi-additive measures, FK resolution | `workspace.gold.fact_orders` |
# MAGIC | **13** | fact_returns | Conformed dimensions, relating two facts | `workspace.gold.fact_returns` |
# MAGIC | **14** | Analytical Queries | Answering real business questions, validating the model | 4 business question queries + validation |
# MAGIC
# MAGIC Let's begin.

# COMMAND ----------
