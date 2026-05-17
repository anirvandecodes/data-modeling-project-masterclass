# Databricks notebook source
# MAGIC %md
# MAGIC # 13 — Relationship Modeling & Entity-Relationship Diagrams (ERD)
# MAGIC
# MAGIC ## What is an ERD?
# MAGIC
# MAGIC An **Entity-Relationship Diagram (ERD)** is a visual blueprint of a data model.
# MAGIC It shows:
# MAGIC - **Entities** — the things you store data about (Customer, Order, Product)
# MAGIC - **Attributes** — the properties of each entity (customer_id, name, email)
# MAGIC - **Relationships** — how entities are connected (a Customer *places* an Order)
# MAGIC - **Cardinality** — the numeric nature of those connections (one customer, many orders)
# MAGIC
# MAGIC ERDs are the universal language of data modeling.
# MAGIC Every data modeling interview expects you to read and draw them.
# MAGIC Every system design starts with one.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ERD Notation — Crow's Foot (Industry Standard)
# MAGIC
# MAGIC ```
# MAGIC  ──────┤     One (and only one)
# MAGIC  ──────<     Many
# MAGIC  ──────○     Zero or one (optional)
# MAGIC  ──────○<    Zero or many
# MAGIC  ──────|<    One or many
# MAGIC ```
# MAGIC
# MAGIC Reading a relationship between two entities:
# MAGIC ```
# MAGIC CUSTOMER ──────|<──── ORDER
# MAGIC               reads as:
# MAGIC        "one customer places one or many orders"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## The Three Relationship Types

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. One-to-One (1:1)
# MAGIC
# MAGIC One record in Table A relates to exactly one record in Table B, and vice versa.
# MAGIC
# MAGIC ```
# MAGIC EMPLOYEE ──────┤──────┤ EMPLOYEE_DETAILS
# MAGIC   employee_id         employee_id (FK)
# MAGIC   name                salary
# MAGIC   department          tax_id
# MAGIC                       emergency_contact
# MAGIC ```
# MAGIC
# MAGIC **Why split into two tables?**
# MAGIC - Separate sensitive/PII data from general data (access control)
# MAGIC - Keep frequently queried columns in a narrow table (performance)
# MAGIC - Store optional attributes that not every entity will have
# MAGIC
# MAGIC **In practice:** 1:1 relationships are rare in analytics models.
# MAGIC You usually flatten them into one wide dimension table.
# MAGIC In OLTP, they're used for security or performance reasons.
# MAGIC
# MAGIC **Real examples:**
# MAGIC - User ↔ UserProfile (separate for PII isolation)
# MAGIC - Country ↔ CountryDetails (store optional extended attributes)
# MAGIC - Employee ↔ EmployeeContract (each employee has exactly one active contract)

# COMMAND ----------

catalog = "workspace"

# Demo: 1:1 — customer base table + customer profile (sensitive attributes separate)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.demo_customer_base (
        customer_id   INT,
        first_name    STRING,
        last_name     STRING,
        channel       STRING
    ) USING DELTA
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.demo_customer_profile (
        customer_id   INT,
        email         STRING,
        phone         STRING,
        address       STRING,
        city          STRING,
        state         STRING
    ) USING DELTA
""")

spark.sql(f"TRUNCATE TABLE {catalog}.gold.demo_customer_base")
spark.sql(f"TRUNCATE TABLE {catalog}.gold.demo_customer_profile")

spark.sql(f"""
    INSERT INTO {catalog}.gold.demo_customer_base
    SELECT customer_id, first_name, last_name, channel
    FROM {catalog}.bronze.customers LIMIT 5
""")

spark.sql(f"""
    INSERT INTO {catalog}.gold.demo_customer_profile
    SELECT customer_id, email, phone, address, city, state
    FROM {catalog}.bronze.customers LIMIT 5
""")

# The 1:1 JOIN — every row in base has exactly one matching row in profile
spark.sql(f"""
    SELECT b.customer_id, b.first_name, b.channel,
           p.email, p.city
    FROM {catalog}.gold.demo_customer_base    b
    JOIN {catalog}.gold.demo_customer_profile p
        ON b.customer_id = p.customer_id
""").show()

print("Notice: every customer_id in base has exactly one match in profile — that's 1:1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. One-to-Many (1:M)
# MAGIC
# MAGIC One record in Table A relates to many records in Table B.
# MAGIC Table B holds the foreign key.
# MAGIC
# MAGIC **This is the most common relationship in data modeling.**
# MAGIC It is the foundation of the star schema:
# MAGIC one customer → many orders, one product → many order lines.
# MAGIC
# MAGIC ```
# MAGIC CUSTOMER ──────|<──── ORDER
# MAGIC   customer_id           order_id
# MAGIC   name                  customer_id (FK)  ← FK lives on the "many" side
# MAGIC   email                 order_date
# MAGIC                         total_amount
# MAGIC
# MAGIC ORDER ──────|<──── ORDER_LINE
# MAGIC   order_id              line_id
# MAGIC   customer_id           order_id (FK)     ← FK lives on the "many" side
# MAGIC   order_date            product_id (FK)
# MAGIC                         quantity
# MAGIC                         unit_price
# MAGIC ```
# MAGIC
# MAGIC **Real examples:**
# MAGIC - One Customer → Many Orders
# MAGIC - One Order → Many Order Lines
# MAGIC - One Product Category → Many Products
# MAGIC - One Driver → Many Rides
# MAGIC - One Restaurant → Many Menu Items
# MAGIC - One Department → Many Employees
# MAGIC
# MAGIC **Key rule:** The foreign key always lives on the **"many" side** of the relationship.

# COMMAND ----------

# Demo: 1:M — one customer, many orders

spark.sql(f"""
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        COUNT(DISTINCT o.order_id)       AS total_orders,
        SUM(o.quantity * o.unit_price)   AS total_spent
    FROM {catalog}.bronze.customers c
    LEFT JOIN {catalog}.bronze.orders o
        ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.first_name, c.last_name
    ORDER BY total_orders DESC
    LIMIT 10
""").show()

# Show the "many" side — one customer, multiple order rows
spark.sql(f"""
    SELECT customer_id, order_id, order_number, order_date, quantity, unit_price
    FROM {catalog}.bronze.orders
    WHERE customer_id = 1
    ORDER BY order_date
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Many-to-Many (M:M)
# MAGIC
# MAGIC Many records in Table A relate to many records in Table B.
# MAGIC
# MAGIC **You cannot represent M:M directly between two tables.**
# MAGIC You always need a **bridge table** (also called a junction table or associative entity)
# MAGIC that sits between them and holds the foreign keys from both sides.
# MAGIC
# MAGIC ```
# MAGIC STUDENT ──────|<──── ENROLLMENT ──────>|──── COURSE
# MAGIC   student_id          student_id (FK)         course_id
# MAGIC   name                course_id  (FK)         title
# MAGIC   email               enrollment_date         credits
# MAGIC                       grade                   instructor
# MAGIC ```
# MAGIC
# MAGIC The bridge table `ENROLLMENT` resolves the M:M.
# MAGIC It often carries its own attributes (grade, enrollment_date).
# MAGIC
# MAGIC **Common M:M examples:**
# MAGIC - Students ↔ Courses (a student takes many courses, a course has many students)
# MAGIC - Products ↔ Orders (a product appears in many orders, an order has many products)
# MAGIC - Actors ↔ Movies
# MAGIC - Users ↔ Roles (a user has many roles, a role applies to many users)
# MAGIC - Tags ↔ Articles
# MAGIC - Doctors ↔ Patients
# MAGIC
# MAGIC **In dimensional modeling:** The fact table IS the bridge table.
# MAGIC `fact_orders` resolves the M:M between customers and products.

# COMMAND ----------

# Demo: M:M — products and orders resolved via order_lines (the bridge / fact table)

spark.sql(f"""
    SELECT
        p.product_name,
        p.category,
        COUNT(DISTINCT o.order_id)   AS times_ordered,
        COUNT(DISTINCT o.customer_id) AS unique_customers,
        SUM(o.quantity)              AS total_units_sold
    FROM {catalog}.bronze.products p
    JOIN {catalog}.bronze.orders   o ON p.product_id = o.product_id
    GROUP BY p.product_name, p.category
    ORDER BY times_ordered DESC
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Recursive / Self-Referencing Relationship
# MAGIC
# MAGIC An entity that has a relationship with itself.
# MAGIC The foreign key points back to the same table's primary key.
# MAGIC
# MAGIC ```
# MAGIC EMPLOYEE
# MAGIC   employee_id  PK
# MAGIC   name
# MAGIC   manager_id   FK → employee_id   ← self-reference!
# MAGIC ```
# MAGIC
# MAGIC **"One employee is managed by another employee."**
# MAGIC The manager is also an employee in the same table.
# MAGIC
# MAGIC **Real examples:**
# MAGIC - Employee → Manager (org hierarchy)
# MAGIC - Category → Parent Category (category tree)
# MAGIC - Comment → Parent Comment (threaded comments)
# MAGIC - Product → Related Product (recommendations)
# MAGIC - Location → Parent Location (city → state → country)
# MAGIC
# MAGIC **Querying with a recursive CTE:**

# COMMAND ----------

# Demo: Recursive — product category hierarchy (category → subcategory → product)
# Simulated org chart using employee data

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.demo_employees (
        employee_id  INT,
        name         STRING,
        role         STRING,
        manager_id   INT
    ) USING DELTA
""")

spark.sql(f"TRUNCATE TABLE {catalog}.gold.demo_employees")

spark.sql(f"""
    INSERT INTO {catalog}.gold.demo_employees VALUES
    (1,  'Sarah Chen',    'VP of Engineering',        NULL),
    (2,  'James Liu',     'Engineering Manager',       1),
    (3,  'Priya Patel',   'Engineering Manager',       1),
    (4,  'Alex Kim',      'Senior Data Engineer',      2),
    (5,  'Maria Torres',  'Data Engineer',             2),
    (6,  'Tom Nguyen',    'Senior Data Engineer',      3),
    (7,  'Anna Brown',    'Data Analyst',              3),
    (8,  'Chris Davis',   'Data Analyst',              4)
""")

# Recursive CTE to traverse the org hierarchy
spark.sql(f"""
    WITH RECURSIVE org_hierarchy AS (
        SELECT
            employee_id,
            name,
            role,
            manager_id,
            0                AS depth,
            name             AS hierarchy_path
        FROM {catalog}.gold.demo_employees
        WHERE manager_id IS NULL

        UNION ALL

        SELECT
            e.employee_id,
            e.name,
            e.role,
            e.manager_id,
            h.depth + 1,
            concat(h.hierarchy_path, ' → ', e.name)
        FROM {catalog}.gold.demo_employees e
        JOIN org_hierarchy h ON e.manager_id = h.employee_id
    )
    SELECT
        depth,
        repeat('  ', depth) || name  AS org_chart,
        role,
        hierarchy_path
    FROM org_hierarchy
    ORDER BY hierarchy_path
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Supertype / Subtype Relationship (Inheritance)
# MAGIC
# MAGIC A general entity (supertype) with specialized variants (subtypes).
# MAGIC All subtypes share common attributes; each has its own specific attributes.
# MAGIC
# MAGIC ```
# MAGIC PAYMENT (supertype)
# MAGIC   payment_id
# MAGIC   order_id
# MAGIC   amount
# MAGIC   payment_date
# MAGIC   payment_type    ← discriminator column
# MAGIC        │
# MAGIC        ├── CREDIT_CARD_PAYMENT (subtype)
# MAGIC        │     payment_id (FK)
# MAGIC        │     card_last_four
# MAGIC        │     card_brand
# MAGIC        │     billing_zip
# MAGIC        │
# MAGIC        ├── BANK_TRANSFER (subtype)
# MAGIC        │     payment_id (FK)
# MAGIC        │     bank_name
# MAGIC        │     routing_number
# MAGIC        │
# MAGIC        └── WALLET_PAYMENT (subtype)
# MAGIC               payment_id (FK)
# MAGIC               wallet_provider
# MAGIC               wallet_id
# MAGIC ```
# MAGIC
# MAGIC **Three implementation strategies:**
# MAGIC
# MAGIC | Strategy | Tables | Approach |
# MAGIC |---|---|---|
# MAGIC | Table per hierarchy | 1 | All subtypes in one table, NULLs for inapplicable columns |
# MAGIC | Table per subtype | N+1 | Supertype table + one table per subtype |
# MAGIC | Table per concrete type | N | One table per subtype, no shared supertype table |
# MAGIC
# MAGIC **For analytics:** Use **table per hierarchy** — one wide table with a `payment_type`
# MAGIC discriminator column and NULLs for non-applicable fields.
# MAGIC Analysts can filter on `payment_type` without complex JOINs.

# COMMAND ----------

# Demo: Supertype/subtype — payment types in one wide table (table per hierarchy)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.gold.demo_payments (
        payment_id        INT,
        order_id          INT,
        amount            DOUBLE,
        payment_date      DATE,
        payment_type      STRING,
        card_last_four    STRING,
        card_brand        STRING,
        bank_name         STRING,
        wallet_provider   STRING
    ) USING DELTA
""")

spark.sql(f"TRUNCATE TABLE {catalog}.gold.demo_payments")

import random
from datetime import date, timedelta

random.seed(99)
payments = []
for i in range(1, 31):
    ptype = random.choice(["credit_card", "credit_card", "bank_transfer", "wallet"])
    payments.append({
        "payment_id": i,
        "order_id": i,
        "amount": round(random.uniform(20, 500), 2),
        "payment_date": str(date(2024, 1, 1) + timedelta(days=i)),
        "payment_type": ptype,
        "card_last_four": str(random.randint(1000, 9999)) if ptype == "credit_card" else None,
        "card_brand": random.choice(["Visa", "Mastercard"]) if ptype == "credit_card" else None,
        "bank_name": random.choice(["Chase", "BofA", "Wells Fargo"]) if ptype == "bank_transfer" else None,
        "wallet_provider": random.choice(["PayPal", "Apple Pay"]) if ptype == "wallet" else None,
    })

spark.createDataFrame(payments).write.format("delta").mode("overwrite") \
    .saveAsTable(f"{catalog}.gold.demo_payments")

spark.sql(f"""
    SELECT
        payment_type,
        COUNT(*)              AS transactions,
        ROUND(SUM(amount), 2) AS total_amount,
        ROUND(AVG(amount), 2) AS avg_amount
    FROM {catalog}.gold.demo_payments
    GROUP BY payment_type
    ORDER BY total_amount DESC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Weak Entity
# MAGIC
# MAGIC An entity that **cannot exist without another entity**.
# MAGIC Its primary key includes the foreign key of the parent entity.
# MAGIC
# MAGIC ```
# MAGIC ORDER ──────|<──── ORDER_LINE
# MAGIC   order_id PK           order_id   PK + FK  ← partial key
# MAGIC   order_date            line_number PK       ← partial key together = composite PK
# MAGIC   customer_id           product_id
# MAGIC                         quantity
# MAGIC ```
# MAGIC
# MAGIC `ORDER_LINE` is a weak entity — a line item only makes sense as part of an order.
# MAGIC Its PK is composite: `(order_id, line_number)`.
# MAGIC
# MAGIC **Other examples:**
# MAGIC - Address is weak to Person (an address only exists because a person has it)
# MAGIC - Room is weak to Building
# MAGIC - Question is weak to Survey
# MAGIC
# MAGIC **In our project:** `fact_orders` has a composite surrogate key
# MAGIC (`md5(order_id || line_item_id)`) because a line item only exists within an order.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Full ERD — Our E-Commerce System
# MAGIC
# MAGIC Putting it all together. This is the complete ERD for the masterclass project.
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────┐        ┌──────────────────────┐        ┌────────────────┐
# MAGIC │   CUSTOMER      │        │     ORDER            │        │   PRODUCT      │
# MAGIC │─────────────────│        │──────────────────────│        │────────────────│
# MAGIC │ customer_id  PK │──|<────│ order_id       PK    │────>|──│ product_id  PK │
# MAGIC │ first_name      │        │ customer_id    FK    │        │ sku            │
# MAGIC │ last_name       │        │ order_date           │        │ product_name   │
# MAGIC │ email           │        │ channel              │        │ category       │
# MAGIC │ address         │        └──────────────────────┘        │ subcategory    │
# MAGIC │ city            │                  │                     │ unit_price     │
# MAGIC │ channel         │                 |<                     └────────────────┘
# MAGIC └─────────────────┘                  │
# MAGIC                           ┌──────────────────────┐
# MAGIC                           │   ORDER_LINE (weak)  │
# MAGIC                           │──────────────────────│
# MAGIC                           │ order_id      PK+FK  │
# MAGIC                           │ line_item_id  PK     │
# MAGIC                           │ product_id    FK     │
# MAGIC                           │ quantity             │
# MAGIC                           │ unit_price           │
# MAGIC                           └──────────────────────┘
# MAGIC
# MAGIC                           ┌──────────────────────┐
# MAGIC                           │   RETURN             │
# MAGIC                           │──────────────────────│
# MAGIC                           │ return_id      PK    │
# MAGIC                           │ order_id       FK    │
# MAGIC                           │ customer_id    FK    │
# MAGIC                           │ product_id     FK    │
# MAGIC                           │ return_date          │
# MAGIC                           │ quantity_returned    │
# MAGIC                           │ return_reason        │
# MAGIC                           └──────────────────────┘
# MAGIC
# MAGIC Relationships:
# MAGIC   CUSTOMER   1 ──── M   ORDER          (one customer places many orders)
# MAGIC   ORDER      1 ──── M   ORDER_LINE     (one order has many line items — weak entity)
# MAGIC   PRODUCT    1 ──── M   ORDER_LINE     (one product appears on many order lines)
# MAGIC   ORDER      1 ──── M   RETURN         (one order can generate many returns)
# MAGIC   CUSTOMER   1 ──── M   RETURN         (one customer can make many returns)
# MAGIC   PRODUCT    1 ──── M   RETURN         (one product can be returned many times)
# MAGIC   PRODUCT    M ──── M   ORDER          (M:M → resolved by ORDER_LINE bridge table)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. OLTP ERD → Dimensional Model Translation
# MAGIC
# MAGIC The ERD above is an OLTP model — normalized, write-optimized.
# MAGIC When we move to Gold (analytical layer), we denormalize it into a star schema.
# MAGIC
# MAGIC **Transformation rules:**
# MAGIC
# MAGIC | OLTP ERD | → | Dimensional Model |
# MAGIC |---|---|---|
# MAGIC | Transaction entity (ORDER_LINE) | → | Fact table (fact_orders) |
# MAGIC | Reference entity (CUSTOMER) | → | Dimension table (dim_customer) |
# MAGIC | Reference entity (PRODUCT) | → | Dimension table (dim_product) |
# MAGIC | Date attribute | → | Date dimension (dim_date) |
# MAGIC | Natural key (customer_id) | → | Surrogate key (customer_key MD5) |
# MAGIC | M:M bridge table | → | Fact table (it IS the bridge) |
# MAGIC | 1:M hierarchy in dim | → | Flatten into wide dim table |
# MAGIC | Weak entity PK | → | Composite surrogate key (MD5 of both parts) |
# MAGIC
# MAGIC **The star schema IS the denormalized, analytics-optimized version of the ERD.**
# MAGIC The ERD tells you what data exists.
# MAGIC The star schema tells you how to query it efficiently.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Interview Cheat Sheet — Relationship Modeling
# MAGIC
# MAGIC **When you see this in an interview question → you know this:**
# MAGIC
# MAGIC | Scenario | Relationship | Implementation |
# MAGIC |---|---|---|
# MAGIC | "A customer can place many orders" | 1:M | FK on orders table |
# MAGIC | "A student can take many courses, a course has many students" | M:M | Bridge table (enrollment) |
# MAGIC | "An employee is managed by another employee" | Recursive 1:M | Self-referencing FK + recursive CTE |
# MAGIC | "A payment can be by card, bank, or wallet" | Supertype/subtype | Single table with discriminator column |
# MAGIC | "An order line only exists as part of an order" | Weak entity | Composite PK with parent FK |
# MAGIC | "Each user has exactly one profile" | 1:1 | Usually flatten into one table for analytics |
# MAGIC | "A product belongs to a category, subcategory, SKU" | 1:M hierarchy | Flatten into wide dim table (star) OR normalize (snowflake) |
# MAGIC
# MAGIC **The two questions that reveal bad modeling:**
# MAGIC 1. "Can you answer X without a JOIN?" — if yes, you might have redundancy
# MAGIC 2. "How many JOINs does it take to answer Y?" — if > 3, consider denormalizing

