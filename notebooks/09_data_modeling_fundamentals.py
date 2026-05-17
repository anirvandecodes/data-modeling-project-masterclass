# Databricks notebook source
# MAGIC %md
# MAGIC # 09 — Data Modeling Fundamentals
# MAGIC
# MAGIC ## What is Data Modeling?
# MAGIC
# MAGIC **Data modeling** is the process of turning business requirements into structured tables
# MAGIC and relationships that a database can store and query efficiently.
# MAGIC
# MAGIC A good data model answers:
# MAGIC - What happened? (facts / events)
# MAGIC - Who was involved? (dimensions — customers, products)
# MAGIC - When did it happen? (date dimension)
# MAGIC - Where did it happen? (location dimension)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## OLTP vs OLAP — The Key Mental Shift
# MAGIC
# MAGIC | | OLTP (Transactional) | OLAP (Analytical) |
# MAGIC |---|---|---|
# MAGIC | Goal | Write fast, read one row | Read millions of rows fast |
# MAGIC | Design | Normalized (3NF) | Denormalized (star/snowflake) |
# MAGIC | Users | Applications, APIs | Analysts, BI tools |
# MAGIC | Query pattern | Lookup by ID | Aggregate by dimension |
# MAGIC | Example | `SELECT * FROM orders WHERE id = 5` | `SUM(sales) GROUP BY category` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## The Structured Problem-Solving Approach
# MAGIC
# MAGIC Before touching a single table, always ask:
# MAGIC
# MAGIC 1. **What is the business process?** (orders, rides, deliveries, streams)
# MAGIC 2. **What questions must the model answer?** (start with business requirements)
# MAGIC 3. **What is the grain?** (one row = one what?)
# MAGIC 4. **What are the dimensions?** (who, what, when, where)
# MAGIC 5. **What are the measures?** (numeric facts to aggregate)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Clarifying Questions to Ask in an Interview
# MAGIC
# MAGIC - What is the primary business process? (e.g., ride booking vs ride completion)
# MAGIC - Who are the users of this model? (analysts, BI tools, ML pipelines)
# MAGIC - What are the top 3 questions the model must answer?
# MAGIC - How much historical data exists? (affects SCD strategy)
# MAGIC - How often is data refreshed? (batch daily, streaming real-time)
# MAGIC - Are there any regulatory constraints? (PII, GDPR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practice: Data Models for Common Systems
# MAGIC
# MAGIC Below are reference schemas for 5 real-world systems.
# MAGIC Each follows the same pattern: identify the business process, declare the grain,
# MAGIC design fact and dimension tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Ride-Hailing (Uber / Lyft)
# MAGIC
# MAGIC **Business process:** A rider requests a ride; a driver completes it.
# MAGIC **Grain:** One row per completed ride.
# MAGIC
# MAGIC ```
# MAGIC fact_rides
# MAGIC ├── ride_key          (surrogate, MD5)
# MAGIC ├── rider_key         → dim_user (role: rider)
# MAGIC ├── driver_key        → dim_user (role: driver)
# MAGIC ├── pickup_location_key  → dim_location
# MAGIC ├── dropoff_location_key → dim_location (role-playing)
# MAGIC ├── request_date_key  → dim_date
# MAGIC ├── complete_date_key → dim_date (role-playing)
# MAGIC ├── vehicle_key       → dim_vehicle
# MAGIC ├── ride_id           (degenerate — the app's ride ID)
# MAGIC ├── distance_miles    (measure)
# MAGIC ├── duration_minutes  (measure)
# MAGIC ├── base_fare         (measure)
# MAGIC ├── surge_multiplier  (measure — semi-additive)
# MAGIC ├── total_fare        (measure)
# MAGIC └── driver_payout     (measure)
# MAGIC
# MAGIC dim_user      — user_id, name, signup_date, tier, rating (SCD Type 2 on tier/rating)
# MAGIC dim_vehicle   — vehicle_id, make, model, year, category (standard, XL, luxury)
# MAGIC dim_location  — location_id, city, state, region, lat, lon
# MAGIC dim_date      — (shared calendar dimension)
# MAGIC ```
# MAGIC
# MAGIC **Key metrics to define first:**
# MAGIC - DAU (Daily Active Users) = distinct rider_key per day in fact_rides
# MAGIC - Driver utilization = rides_completed / hours_online per driver per day
# MAGIC - Average ride value = SUM(total_fare) / COUNT(ride_key)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Food Delivery (DoorDash / Uber Eats)
# MAGIC
# MAGIC **Business process:** A customer orders food; a dasher delivers it.
# MAGIC **Grain:** One row per order line item (one dish on one order).
# MAGIC
# MAGIC ```
# MAGIC fact_deliveries
# MAGIC ├── delivery_line_key
# MAGIC ├── customer_key      → dim_customer
# MAGIC ├── restaurant_key    → dim_restaurant
# MAGIC ├── dasher_key        → dim_dasher
# MAGIC ├── menu_item_key     → dim_menu_item
# MAGIC ├── order_date_key    → dim_date
# MAGIC ├── delivery_date_key → dim_date (role-playing)
# MAGIC ├── location_key      → dim_location (delivery address)
# MAGIC ├── order_id          (degenerate)
# MAGIC ├── quantity          (measure)
# MAGIC ├── item_price        (measure)
# MAGIC ├── delivery_fee      (measure)
# MAGIC ├── tip_amount        (measure)
# MAGIC └── total_amount      (measure)
# MAGIC
# MAGIC dim_restaurant  — restaurant_id, name, cuisine, rating, city (SCD Type 2 on rating)
# MAGIC dim_menu_item   — item_id, name, category, price, is_available
# MAGIC dim_dasher      — dasher_id, name, vehicle_type, rating
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Social Media (Twitter / Instagram style)
# MAGIC
# MAGIC **Business process:** Users create content; other users engage with it.
# MAGIC **Grain:** One row per engagement event (like, comment, share, view).
# MAGIC
# MAGIC ```
# MAGIC fact_engagements
# MAGIC ├── engagement_key
# MAGIC ├── actor_user_key    → dim_user (who engaged)
# MAGIC ├── content_key       → dim_content
# MAGIC ├── author_user_key   → dim_user (role-playing: who created the content)
# MAGIC ├── engagement_date_key → dim_date
# MAGIC ├── engagement_type   (like / comment / share / view — could be dim_engagement_type)
# MAGIC ├── session_id        (degenerate)
# MAGIC └── time_spent_seconds (measure)
# MAGIC
# MAGIC dim_user     — user_id, username, account_type, follower_count (SCD Type 2)
# MAGIC dim_content  — content_id, content_type (post/reel/story), topic, hashtags
# MAGIC
# MAGIC Key metrics:
# MAGIC - DAU = distinct actor_user_key per day
# MAGIC - MAU = distinct actor_user_key per 30-day window
# MAGIC - Engagement rate = engagements / impressions per content item
# MAGIC - Time spent = SUM(time_spent_seconds) per user per day
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Library System
# MAGIC
# MAGIC **Business process:** A member borrows a book; returns it.
# MAGIC **Grain:** One row per borrowing transaction.
# MAGIC
# MAGIC ```
# MAGIC fact_borrowings
# MAGIC ├── borrowing_key
# MAGIC ├── member_key        → dim_member
# MAGIC ├── book_key          → dim_book
# MAGIC ├── branch_key        → dim_branch
# MAGIC ├── checkout_date_key → dim_date
# MAGIC ├── due_date_key      → dim_date (role-playing)
# MAGIC ├── return_date_key   → dim_date (role-playing, NULL until returned)
# MAGIC ├── days_overdue      (measure — 0 if on time)
# MAGIC ├── fine_amount       (measure)
# MAGIC └── renewals_count    (measure)
# MAGIC
# MAGIC dim_book    — book_id, isbn, title, author, genre, publication_year
# MAGIC dim_member  — member_id, name, membership_type, join_date (SCD Type 2 on membership_type)
# MAGIC dim_branch  — branch_id, name, city, region
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Cloud Storage (S3 / GCS / Azure Blob)
# MAGIC
# MAGIC **Business process:** A customer uses storage; billed monthly.
# MAGIC **Grain:** One row per customer per bucket per day (periodic snapshot fact).
# MAGIC
# MAGIC ```
# MAGIC fact_storage_usage   ← PERIODIC SNAPSHOT (not transactional)
# MAGIC ├── snapshot_key
# MAGIC ├── customer_key      → dim_customer
# MAGIC ├── bucket_key        → dim_bucket
# MAGIC ├── region_key        → dim_region
# MAGIC ├── snapshot_date_key → dim_date
# MAGIC ├── storage_gb        (measure — semi-additive: sum across buckets, NOT across time)
# MAGIC ├── requests_count    (measure — fully additive)
# MAGIC ├── egress_gb         (measure — fully additive)
# MAGIC └── daily_cost_usd    (measure)
# MAGIC
# MAGIC Note: storage_gb is SEMI-ADDITIVE.
# MAGIC   ✅ SUM across customers on a single day = total platform storage on that day
# MAGIC   ❌ SUM across days for one customer ≠ meaningful (double-counts)
# MAGIC   ✅ AVG across days for one customer = average daily storage (meaningful)
# MAGIC ```
