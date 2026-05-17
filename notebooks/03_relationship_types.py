# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 03 — Relationship Types: How Entities Connect
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC In Notebook 02 we drew a conceptual ERD showing that entities like CUSTOMER and ORDER are connected. But we were loose about *how* they connect. In this notebook we get precise.
# MAGIC
# MAGIC There are four fundamental relationship types in data modeling:
# MAGIC
# MAGIC | Type | Notation | Classic example |
# MAGIC |---|---|---|
# MAGIC | One-to-One | 1:1 | Person → Passport |
# MAGIC | One-to-Many | 1:M | Customer → Orders |
# MAGIC | Many-to-Many | M:M | Students ↔ Courses |
# MAGIC | Recursive (self-referencing) | — | Employee → Manager |
# MAGIC
# MAGIC Understanding these is not just theoretical. It directly determines **which table gets the foreign key** — and getting that wrong creates broken joins and incorrect query results.
# MAGIC
# MAGIC We'll explore each type with a definition and a live SQL query against our Bronze data (or a quick demo table) to prove the concept.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. One-to-One (1:1)
# MAGIC
# MAGIC ### Definition
# MAGIC
# MAGIC A **one-to-one** relationship means that each instance of entity A is associated with **at most one** instance of entity B, and vice versa.
# MAGIC
# MAGIC ```
# MAGIC   PERSON  ──┤────────┤──  PASSPORT
# MAGIC
# MAGIC   "One person has one passport.
# MAGIC    One passport belongs to one person."
# MAGIC ```
# MAGIC
# MAGIC ### When do you use it?
# MAGIC
# MAGIC 1:1 relationships are common in two scenarios:
# MAGIC - **Splitting a wide table**: A CUSTOMER table might have 50 columns. You can split optional/rarely-used fields into a separate CUSTOMER_PROFILE table with a 1:1 relationship. This improves query performance on the core table.
# MAGIC - **Security/access control**: Sensitive data (e.g., SSN, payment info) is kept in a separate table with stricter access, linked 1:1 to the main entity.
# MAGIC
# MAGIC ### Important: the FK can go on either side
# MAGIC
# MAGIC In a 1:1 relationship, the foreign key can technically go on either table. Convention is to put it on the table that is the "dependent" or "optional" side — the one that wouldn't exist without the other.
# MAGIC
# MAGIC ### Demo: One email per customer
# MAGIC
# MAGIC In our `customers` table, each customer has exactly one email address. Let's verify this 1:1 property — no customer should have more than one email, and no email should belong to more than one customer.

# COMMAND ----------

# Demonstrate 1:1: each customer has exactly one email address.
# If any customer_id appears more than once, we have a data quality issue.

print("Checking: is customer_id → email a 1:1 relationship?")
print("(If the result is empty, the relationship is clean — every customer has exactly one email)\n")

spark.sql("""
    SELECT
        customer_id,
        COUNT(DISTINCT email) AS distinct_emails
    FROM workspace.bronze.customers
    GROUP BY customer_id
    HAVING COUNT(DISTINCT email) > 1
""").show()

# COMMAND ----------

# Also check the reverse: is email unique across customers?
# (No two customers share the same email)

print("Checking: does any email belong to more than one customer?")
print("(If empty: every email maps to exactly one customer)\n")

spark.sql("""
    SELECT
        email,
        COUNT(DISTINCT customer_id) AS customer_count
    FROM workspace.bronze.customers
    GROUP BY email
    HAVING COUNT(DISTINCT customer_id) > 1
""").show()

# COMMAND ----------

# Show a clean sample of the 1:1 relationship
print("Sample of customers — each row: one customer, one email (1:1)")
spark.sql("""
    SELECT customer_id, first_name, last_name, email
    FROM workspace.bronze.customers
    ORDER BY customer_id
    LIMIT 8
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC The first two queries should return empty results — confirming that `customer_id → email` is a proper 1:1 relationship: every customer has exactly one email, and every email belongs to exactly one customer.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. One-to-Many (1:M)
# MAGIC
# MAGIC ### Definition
# MAGIC
# MAGIC A **one-to-many** relationship means that each instance of entity A can be associated with **many** instances of entity B, but each instance of B is associated with **only one** instance of A.
# MAGIC
# MAGIC ```
# MAGIC   CUSTOMER  ──┤────────○<──  ORDER
# MAGIC
# MAGIC   "One customer places zero or many orders.
# MAGIC    Each order belongs to exactly one customer."
# MAGIC ```
# MAGIC
# MAGIC This is by far the most common relationship type in data modeling. The parent-child pattern: one parent, many children.
# MAGIC
# MAGIC ### The Foreign Key Rule — Critical
# MAGIC
# MAGIC **The foreign key always lives on the "many" side.**
# MAGIC
# MAGIC In a CUSTOMER → ORDER relationship:
# MAGIC - ❌ Wrong: Put `order_id` in the CUSTOMER table. You'd need an array for customers with multiple orders — messy.
# MAGIC - ✅ Correct: Put `customer_id` in the ORDER table. One column, one value per row.
# MAGIC
# MAGIC This is not optional. It's the foundational rule of relational design.
# MAGIC
# MAGIC ### Demo: One customer → many orders

# COMMAND ----------

# Demonstrate 1:M: how many orders does each customer have?
# This query shows the "many" side — each customer has multiple orders.

print("One-to-Many: Customer → Orders")
print("Each row = one customer. order_count shows how many orders they have.\n")

spark.sql("""
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        COUNT(o.order_id) AS order_count,
        ROUND(SUM(o.total_amount), 2) AS total_spent
    FROM workspace.bronze.customers c
    LEFT JOIN workspace.bronze.orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.first_name, c.last_name
    ORDER BY order_count DESC
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# Prove the FK is on the "many" side: orders table has customer_id column
print("The ORDER table (many side) holds the FK: customer_id")
print("The CUSTOMER table (one side) does NOT have an order_id column\n")

spark.sql("""
    SELECT order_id, customer_id, order_date, total_amount
    FROM workspace.bronze.orders
    LIMIT 8
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Notice that `customer_id` lives in the **orders** table, not a list of `order_id` values in the customers table. This is the 1:M pattern: the FK (`customer_id`) is on the **many** side (the orders table).
# MAGIC
# MAGIC Every order knows its customer. But the customer record doesn't need to list every order — you query them by joining on `customer_id`.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Many-to-Many (M:M)
# MAGIC
# MAGIC ### Definition
# MAGIC
# MAGIC A **many-to-many** relationship means that each instance of entity A can be associated with **many** instances of entity B, *and* each instance of B can be associated with **many** instances of A.
# MAGIC
# MAGIC ```
# MAGIC   CUSTOMER  ──○<────────○<──  PRODUCT
# MAGIC
# MAGIC   "One customer buys many products.
# MAGIC    One product is bought by many customers."
# MAGIC ```
# MAGIC
# MAGIC ### Why M:M Requires a Bridge Table
# MAGIC
# MAGIC You cannot store an M:M relationship directly in either entity table. You can't put a `product_id` column in CUSTOMER (a customer has many products), and you can't put a `customer_id` column in PRODUCT (a product has many customers). You'd need arrays — and relational databases don't handle that cleanly.
# MAGIC
# MAGIC The solution is a **bridge table** (also called a junction table or associative entity) that sits between the two entities:
# MAGIC
# MAGIC ```
# MAGIC   CUSTOMER ──┤────────○<── ORDER_LINE ──○<────────┤── PRODUCT
# MAGIC
# MAGIC   ORDER_LINE is the bridge. It has:
# MAGIC     - customer_id (FK to CUSTOMER)
# MAGIC     - product_id  (FK to PRODUCT)
# MAGIC     - ...and its own attributes: quantity, price, etc.
# MAGIC ```
# MAGIC
# MAGIC ### In analytics: the fact table IS the bridge
# MAGIC
# MAGIC In a dimensional model, the **fact table** naturally resolves every M:M relationship. `fact_orders` has a row per customer+product combination (per order line). It holds FKs to both CUSTOMER and PRODUCT — it IS the bridge table.
# MAGIC
# MAGIC This is one reason fact tables are designed the way they are.
# MAGIC
# MAGIC ### Demo: Customers and Products in M:M

# COMMAND ----------

# Show the M:M: each customer has bought many products
print("Many-to-Many: how many distinct products has each customer ordered?")
print("(One customer → many products)\n")

spark.sql("""
    SELECT
        c.customer_id,
        c.first_name || ' ' || c.last_name AS customer_name,
        COUNT(DISTINCT o.product_id) AS distinct_products_ordered
    FROM workspace.bronze.customers c
    JOIN workspace.bronze.orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.first_name, c.last_name
    ORDER BY distinct_products_ordered DESC
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# Show the other direction: each product has been ordered by many customers
print("Many-to-Many: how many distinct customers ordered each product?")
print("(One product → many customers)\n")

spark.sql("""
    SELECT
        p.product_id,
        p.product_name,
        p.category,
        COUNT(DISTINCT o.customer_id) AS distinct_customers
    FROM workspace.bronze.products p
    JOIN workspace.bronze.orders o ON p.product_id = o.product_id
    GROUP BY p.product_id, p.product_name, p.category
    ORDER BY distinct_customers DESC
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# The ORDERS table is the bridge that resolves the M:M
# Each row in orders: one customer × one product × one date
print("The ORDERS table acts as the bridge (resolves the M:M):")
print("Each row ties ONE customer to ONE product on ONE date.\n")

spark.sql("""
    SELECT order_id, customer_id, product_id, order_date, quantity, total_amount
    FROM workspace.bronze.orders
    ORDER BY customer_id, order_date
    LIMIT 10
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Both directions confirm the M:M: customers have many distinct products, and products have many distinct customers. The `orders` table is what connects them — it is the bridge entity that resolves the M:M by storing one FK to each side.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Recursive (Self-Referencing) Relationships
# MAGIC
# MAGIC ### Definition
# MAGIC
# MAGIC A **recursive relationship** is when an entity has a foreign key that points back to the **same entity**. The most common example is an organizational hierarchy: an EMPLOYEE has a `manager_id` that references another EMPLOYEE's `employee_id`.
# MAGIC
# MAGIC ```
# MAGIC   EMPLOYEE ──┤────────○<──┐
# MAGIC              │            │
# MAGIC              └────────────┘
# MAGIC              manager_id → employee_id
# MAGIC
# MAGIC   "One employee (the manager) manages zero or many employees.
# MAGIC    Each employee reports to at most one manager."
# MAGIC ```
# MAGIC
# MAGIC ### Other examples of recursive relationships:
# MAGIC - CATEGORY → SUBCATEGORY → SUB-SUBCATEGORY (all in one CATEGORY table)
# MAGIC - LOCATION → PARENT LOCATION (country → state → city in one table)
# MAGIC - PRODUCT COMPONENT → PARENT PRODUCT (bill of materials)
# MAGIC
# MAGIC ### Demo: Employee Hierarchy with a Recursive CTE
# MAGIC
# MAGIC Our e-commerce dataset doesn't have an employee table, so we'll build a small one inline to demonstrate the concept. We'll then use a **recursive CTE** (Common Table Expression) to walk the hierarchy from top to bottom.

# COMMAND ----------

# Build a small demo employee hierarchy table
spark.sql("""
    CREATE OR REPLACE TEMP VIEW demo_employees AS
    SELECT * FROM (VALUES
        (1,  'Alice Chen',     NULL,  'CEO'),
        (2,  'Bob Torres',     1,     'VP Sales'),
        (3,  'Carol West',     1,     'VP Marketing'),
        (4,  'David Kim',      2,     'Sales Manager'),
        (5,  'Emma Patel',     2,     'Sales Manager'),
        (6,  'Frank Liu',      3,     'Marketing Manager'),
        (7,  'Grace Johnson',  4,     'Sales Rep'),
        (8,  'Henry Brown',    4,     'Sales Rep'),
        (9,  'Iris Davis',     5,     'Sales Rep'),
        (10, 'Jack Wilson',    6,     'Marketing Analyst')
    ) AS t(employee_id, employee_name, manager_id, title)
""")

print("Demo employee table (self-referencing: manager_id → employee_id):")
spark.sql("SELECT * FROM demo_employees ORDER BY employee_id").show(truncate=False)

# COMMAND ----------

# Use a recursive CTE to walk the org hierarchy from the top down
# The recursive CTE has two parts:
#   1. ANCHOR: the root node (no manager = CEO)
#   2. RECURSIVE: join each level back to find the next level down

print("Org hierarchy using a recursive CTE:")
print("(depth = how many levels below the CEO)\n")

spark.sql("""
    WITH RECURSIVE org_hierarchy AS (
        -- Anchor: start with the top-level employee (no manager)
        SELECT
            employee_id,
            employee_name,
            manager_id,
            title,
            0 AS depth,
            employee_name AS hierarchy_path
        FROM demo_employees
        WHERE manager_id IS NULL

        UNION ALL

        -- Recursive: join each employee to their manager found so far
        SELECT
            e.employee_id,
            e.employee_name,
            e.manager_id,
            e.title,
            h.depth + 1 AS depth,
            h.hierarchy_path || ' > ' || e.employee_name AS hierarchy_path
        FROM demo_employees e
        JOIN org_hierarchy h ON e.manager_id = h.employee_id
    )
    SELECT
        depth,
        REPEAT('  ', depth) || employee_name AS indented_name,
        title,
        hierarchy_path
    FROM org_hierarchy
    ORDER BY hierarchy_path
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC The recursive CTE walks the tree from the CEO down through every level of management. Each iteration of the `UNION ALL` goes one level deeper, stopping when there are no more children to find.
# MAGIC
# MAGIC **Recursive relationships in dimensional models:**
# MAGIC In analytics, recursive hierarchies are usually **flattened**. Instead of keeping `parent_category_id` in the PRODUCT table, we store `category`, `subcategory`, and `sub_subcategory` as separate columns in `dim_product`. This eliminates the need for recursive CTEs in every analytical query — a major performance win.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Golden Rule of Foreign Keys
# MAGIC
# MAGIC Before we close this notebook, here is the rule to tattoo on your brain:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC > **The foreign key always lives on the "many" side of a relationship.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Let's see this rule in action across every relationship type:
# MAGIC
# MAGIC | Relationship | "One" side | "Many" side | FK lives on... |
# MAGIC |---|---|---|---|
# MAGIC | Customer → Orders | CUSTOMER | ORDERS | `customer_id` in ORDERS |
# MAGIC | Location → Orders | LOCATION | ORDERS | `location_id` in ORDERS |
# MAGIC | Product → Orders | PRODUCT | ORDERS | `product_id` in ORDERS |
# MAGIC | Order → Returns | ORDER | RETURNS | `order_id` in RETURNS |
# MAGIC | Employee → Reports | EMPLOYEE (manager) | EMPLOYEE (report) | `manager_id` in EMPLOYEE |
# MAGIC
# MAGIC ### Why this matters for star schema design
# MAGIC
# MAGIC In a star schema, the **fact table is always on the "many" side** of every relationship with every dimension. One customer can have many orders. One product can appear in many orders. One date can have many orders.
# MAGIC
# MAGIC Therefore, **the fact table holds all the foreign keys** — one FK column per dimension table. The dimension tables hold only their own primary key.
# MAGIC
# MAGIC This is why fact tables look the way they do: lots of FK columns (linking to dimensions) plus the actual measures (revenue, quantity). That design is not arbitrary — it is the direct consequence of the FK rule applied consistently.

# COMMAND ----------

# Final demo: show that fact_orders (when we build it) will hold ALL the FKs
# Preview of what fact_orders will look like by showing the raw orders structure
print("The orders table (future fact_orders) holds ALL the foreign keys:")
print("customer_id, product_id, location_id — one per dimension\n")

spark.sql("""
    SELECT
        order_id,
        customer_id,     -- FK → dim_customer (many orders per customer)
        product_id,      -- FK → dim_product  (many orders per product)
        location_id,     -- FK → dim_location (many orders per location)
        order_date,      -- FK → dim_date     (many orders per day)
        quantity,        -- MEASURE
        total_amount     -- MEASURE
    FROM workspace.bronze.orders
    LIMIT 8
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC In this notebook we covered all four relationship types:
# MAGIC
# MAGIC | Type | Key rule | Our example |
# MAGIC |---|---|---|
# MAGIC | **1:1** | FK can go on either side | customer → email |
# MAGIC | **1:M** | FK goes on the **many** side | customer → orders |
# MAGIC | **M:M** | Requires a bridge table; in analytics the fact IS the bridge | customers ↔ products (via orders) |
# MAGIC | **Recursive** | FK points back to the same table; flatten in analytics | employee → manager |
# MAGIC
# MAGIC **Next up — Notebook 04: Logical Data Modeling.** We'll add attributes, data types, primary keys, and foreign keys to our model — and make the critical translation from OLTP logical model to analytical (dimensional) logical model.
