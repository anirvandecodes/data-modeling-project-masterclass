# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 02 — Conceptual Data Modeling: What Entities Exist?
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is a Conceptual Model?
# MAGIC
# MAGIC A **conceptual data model** is the highest-level view of your data. It contains no column names, no data types, no SQL — just the **entities** that exist in your business domain and the **relationships** between them.
# MAGIC
# MAGIC Think of it as a conversation tool. Before you write a single line of code, you sit down with business stakeholders — the product manager, the head of sales, the finance director — and agree on what things exist and how they connect.
# MAGIC
# MAGIC A conceptual model answers two questions:
# MAGIC 1. **What are we tracking?** (entities)
# MAGIC 2. **How do they relate?** (relationships)
# MAGIC
# MAGIC That's it. No more, no less.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why Start Here?
# MAGIC
# MAGIC Skipping the conceptual model is the most common mistake in data projects. Engineers jump straight to writing SQL, discover halfway through that they forgot to model the "returns" entity, and have to redesign tables that already have downstream dependencies.
# MAGIC
# MAGIC Starting at the conceptual level gives you three benefits:
# MAGIC
# MAGIC **1. Catch missing entities before writing code.**
# MAGIC It's much cheaper to add a box to a diagram than to add a table to a schema that already has downstream consumers.
# MAGIC
# MAGIC **2. Align with stakeholders using a language they understand.**
# MAGIC A box labeled "CUSTOMER" connected to a box labeled "ORDER" requires no technical knowledge to validate. Your CEO can review this and tell you if something is missing.
# MAGIC
# MAGIC **3. Set the scope of your model.**
# MAGIC What is in scope? What is explicitly out of scope? A conceptual model forces you to decide.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is an Entity?
# MAGIC
# MAGIC An **entity** is a real-world thing — a person, place, object, or event — that your business needs to store data about. Each entity will eventually become a table.
# MAGIC
# MAGIC A few tests to identify an entity:
# MAGIC - Can you point to an individual instance of it? (e.g., "Customer #1042 - Jane Doe")
# MAGIC - Does the business care about tracking multiple attributes of it?
# MAGIC - Is it distinct from other entities — not just a property of something else?
# MAGIC
# MAGIC Examples:
# MAGIC | Is it an entity? | Why |
# MAGIC |---|---|
# MAGIC | CUSTOMER — yes | We track name, email, address, signup date... each customer is an instance |
# MAGIC | ORDER — yes | We track date, channel, amount... each order is a discrete event |
# MAGIC | CUSTOMER EMAIL — no | That's just an *attribute* of CUSTOMER, not a separate entity |
# MAGIC | ORDER STATUS HISTORY — maybe | Depends if we need to track the history. In our scope: no. |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## ERD Notation — Crow's Foot
# MAGIC
# MAGIC Entity Relationship Diagrams (ERDs) use **Crow's Foot notation** to show how entities connect. The symbols go at the end of each relationship line, describing the minimum and maximum number of instances on each side.
# MAGIC
# MAGIC Here are the symbols and what they mean:
# MAGIC
# MAGIC ```
# MAGIC   ──────┤         One (and only one)
# MAGIC                   "Exactly one"
# MAGIC
# MAGIC   ──────<         Many
# MAGIC                   "One or more" — the crow's foot (looks like a bird's toes)
# MAGIC
# MAGIC   ──────○         Zero or one
# MAGIC                   Optional — the circle means "zero is possible"
# MAGIC
# MAGIC   ──────|<        One or many (mandatory many)
# MAGIC                   "At least one"
# MAGIC
# MAGIC   ──────○<        Zero or many (optional many)
# MAGIC                   "None, or as many as needed"
# MAGIC ```
# MAGIC
# MAGIC ### Reading a relationship
# MAGIC
# MAGIC You read both ends of a relationship line. For example:
# MAGIC
# MAGIC ```
# MAGIC   CUSTOMER  ┤──────○<  ORDER
# MAGIC ```
# MAGIC
# MAGIC Read left-to-right: "One customer can have zero or many orders."
# MAGIC Read right-to-left: "Each order belongs to one and only one customer."
# MAGIC
# MAGIC Always read both directions. The business meaning lives in both readings.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Our E-Commerce Entities
# MAGIC
# MAGIC For our masterclass e-commerce scenario, we have identified five core entities:
# MAGIC
# MAGIC | Entity | What it represents |
# MAGIC |---|---|
# MAGIC | **CUSTOMER** | A person who has registered and can place orders |
# MAGIC | **PRODUCT** | An item available for purchase |
# MAGIC | **ORDER** | A purchase transaction placed by a customer |
# MAGIC | **LOCATION** | A geographic location associated with an order (shipping destination) |
# MAGIC | **RETURN** | A request to return a previously purchased item |
# MAGIC
# MAGIC ### Relationships in plain English
# MAGIC
# MAGIC Before drawing the ERD, always write out the relationships in plain sentences. One sentence per direction, per relationship:
# MAGIC
# MAGIC - A **CUSTOMER** can place **zero or many ORDERS**. Each **ORDER** belongs to exactly **one CUSTOMER**.
# MAGIC - An **ORDER** can contain **one or many PRODUCTS**. A **PRODUCT** can appear in **zero or many ORDERS**. *(This is a many-to-many — resolved through the ORDER itself acting as a bridge in our model.)*
# MAGIC - Each **ORDER** is associated with exactly **one LOCATION**. A **LOCATION** can be associated with **zero or many ORDERS**.
# MAGIC - A **RETURN** references exactly **one ORDER** (specifically one line item). An **ORDER** can have **zero or many RETURNS**.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conceptual ERD — Our E-Commerce Domain
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────┐
# MAGIC │              E-COMMERCE CONCEPTUAL DATA MODEL                       │
# MAGIC └─────────────────────────────────────────────────────────────────────┘
# MAGIC
# MAGIC
# MAGIC   ┌──────────────┐                        ┌──────────────┐
# MAGIC   │   CUSTOMER   │                        │   LOCATION   │
# MAGIC   └──────┬───────┘                        └──────┬───────┘
# MAGIC          │                                       │
# MAGIC          │  (one customer                        │  (one location
# MAGIC          │   many orders)                        │   many orders)
# MAGIC          │                                       │
# MAGIC          ┤○<                                  ┤○<
# MAGIC          │                                       │
# MAGIC          └────────────┐           ┌──────────────┘
# MAGIC                       │           │
# MAGIC                       ▼           ▼
# MAGIC                    ┌──────────────────┐
# MAGIC                    │      ORDER       │
# MAGIC                    └────────┬─────────┘
# MAGIC                             │
# MAGIC                    ┌────────┴─────────┐
# MAGIC                    │                  │
# MAGIC                ┤○<                ┤○<
# MAGIC                    │                  │
# MAGIC                    ▼                  ▼
# MAGIC            ┌──────────────┐   ┌──────────────┐
# MAGIC            │   PRODUCT    │   │    RETURN    │
# MAGIC            └──────────────┘   └──────────────┘
# MAGIC
# MAGIC
# MAGIC   Relationships:
# MAGIC
# MAGIC   CUSTOMER ──┤────────○<── ORDER
# MAGIC   "One customer places zero or many orders.
# MAGIC    Each order belongs to exactly one customer."
# MAGIC
# MAGIC   LOCATION ──┤────────○<── ORDER
# MAGIC   "One location is linked to zero or many orders.
# MAGIC    Each order ships to exactly one location."
# MAGIC
# MAGIC   ORDER ──┤────────○<── PRODUCT   (many-to-many via ORDER as bridge)
# MAGIC   "One order includes one or many products.
# MAGIC    One product appears in zero or many orders."
# MAGIC
# MAGIC   ORDER ──┤────────○<── RETURN
# MAGIC   "One order can generate zero or many returns.
# MAGIC    Each return references exactly one order."
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's In Scope vs Out of Scope
# MAGIC
# MAGIC Part of conceptual modeling is **explicitly deciding what NOT to model**. This is as important as what you include. Every entity you add costs engineering time, storage, and maintenance.
# MAGIC
# MAGIC | Entity | Decision | Reason |
# MAGIC |---|---|---|
# MAGIC | CUSTOMER | ✅ In scope | Core to our business questions |
# MAGIC | PRODUCT | ✅ In scope | Core to our business questions |
# MAGIC | ORDER | ✅ In scope | The central fact of our business |
# MAGIC | LOCATION | ✅ In scope | Geography is one of our 4 business questions |
# MAGIC | RETURN | ✅ In scope | Returns impact net revenue |
# MAGIC | INVENTORY | ❌ Out of scope | No inventory data in our source system |
# MAGIC | PAYMENT / PAYMENT METHOD | ❌ Out of scope | Not in source data; finance has separate system |
# MAGIC | SUPPLIER / VENDOR | ❌ Out of scope | Procurement is a separate domain |
# MAGIC | EMPLOYEE / SALES REP | ❌ Out of scope | Not tracked in this dataset |
# MAGIC | PRODUCT REVIEW | ❌ Out of scope | Would be valuable, but not in source data |
# MAGIC | ORDER STATUS HISTORY | ❌ Out of scope | We only track final order state |
# MAGIC
# MAGIC Scope decisions should be documented and agreed upon with stakeholders before moving to logical modeling. Scope creep at the logical or physical stage is expensive.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seeing the Raw Data
# MAGIC
# MAGIC Let's look at what our entities actually look like in the Bronze layer. This is the raw data we'll be working with throughout this masterclass. Run the cells below to inspect each entity.
# MAGIC
# MAGIC **Before running:** Make sure you have completed Notebook 07 (Bronze Ingest) so these tables exist. If you haven't, come back after running that notebook.

# COMMAND ----------

# Let's look at the CUSTOMER entity — who are our customers?
print("=" * 60)
print("ENTITY: CUSTOMER")
print("=" * 60)
df_customers = spark.sql("SELECT * FROM workspace.bronze.customers LIMIT 5")
df_customers.show(truncate=False)
print(f"Total customers: {spark.sql('SELECT COUNT(*) FROM workspace.bronze.customers').collect()[0][0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC Notice the customer table has: an ID, name, email, phone, address fields, a customer segment (Bronze/Silver/Gold), and a signup date. All of these are **attributes** of the CUSTOMER entity. We'll define which ones to bring into our dimension table in Notebook 04.

# COMMAND ----------

# The PRODUCT entity — what are we selling?
print("=" * 60)
print("ENTITY: PRODUCT")
print("=" * 60)
df_products = spark.sql("SELECT * FROM workspace.bronze.products LIMIT 5")
df_products.show(truncate=False)
print(f"Total products: {spark.sql('SELECT COUNT(*) FROM workspace.bronze.products').collect()[0][0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC Products have a category and subcategory hierarchy. This is a **hierarchy** within a single entity — something we'll discuss in detail in Notebook 04 when we talk about logical modeling and how hierarchies get flattened in dimensional models.

# COMMAND ----------

# The ORDER entity — the central event in our business
print("=" * 60)
print("ENTITY: ORDER")
print("=" * 60)
df_orders = spark.sql("SELECT * FROM workspace.bronze.orders LIMIT 5")
df_orders.show(truncate=False)
print(f"Total orders: {spark.sql('SELECT COUNT(*) FROM workspace.bronze.orders').collect()[0][0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC Orders reference other entities via IDs: `customer_id`, `product_id`, `location_id`. These are the **foreign key relationships** we identified in our conceptual model. One order → one customer, one product, one location.
# MAGIC
# MAGIC You'll also notice `order_date`, `quantity`, `unit_price`, `total_amount`, and `channel`. These are the **measures** and **context attributes** that will live in our fact table.

# COMMAND ----------

# The LOCATION entity — where are orders being shipped?
print("=" * 60)
print("ENTITY: LOCATION")
print("=" * 60)
df_locations = spark.sql("SELECT * FROM workspace.bronze.locations LIMIT 5")
df_locations.show(truncate=False)
print(f"Total locations: {spark.sql('SELECT COUNT(*) FROM workspace.bronze.locations').collect()[0][0]}")

# COMMAND ----------

# The RETURN entity — what comes back?
print("=" * 60)
print("ENTITY: RETURN")
print("=" * 60)
df_returns = spark.sql("SELECT * FROM workspace.bronze.returns LIMIT 5")
df_returns.show(truncate=False)
print(f"Total returns: {spark.sql('SELECT COUNT(*) FROM workspace.bronze.returns').collect()[0][0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: What We've Done in This Notebook
# MAGIC
# MAGIC We have:
# MAGIC 1. Defined what a conceptual model is (entities + relationships, no technical details)
# MAGIC 2. Learned Crow's Foot ERD notation
# MAGIC 3. Identified our 5 entities: CUSTOMER, PRODUCT, ORDER, LOCATION, RETURN
# MAGIC 4. Written out every relationship in plain English before diagramming it
# MAGIC 5. Drew the full conceptual ERD
# MAGIC 6. Decided what's in scope and explicitly out of scope
# MAGIC 7. Looked at the actual raw Bronze data to ground our model in reality
# MAGIC
# MAGIC **Next up — Notebook 03: Relationship Types.** We'll explore each type of relationship (1:1, 1:M, M:M, recursive) in depth, and demonstrate each one with live queries against our Bronze data.
