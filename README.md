# BigQuery-E-Commerce-Advanced-Data-Modeling-Integration
# 🏆 BigQuery E-Commerce — Advanced Data Modeling & Integration

> **Project Owner:** Mert Ovet  
> [LinkedIn: linkedin.com/in/mertovet](https://linkedin.com/in/mertovet)

---

## 📄 Project Overview

This project demonstrates an end-to-end real-world scenario of advanced e-commerce data modeling and analytics in Google BigQuery, starting from complex nested JSON data to a fully clean, queryable, and analytics-ready schema.

- **Data Source:**  
  Synthetic Olist E-commerce data, exported as nested NDJSON (newline-delimited JSON), generated and pre-processed in Python.
- **Stack:**  
  Python (for data structuring & export), Google Cloud Storage (staging), BigQuery (data warehouse & analytics).

---

## 1. Data Flow: Step-by-Step Process

### 1.1. Python — Data Preparation & Nested Structuring

#### a. Library Imports & Notebook Setup

I started by importing all essential Python libraries for data analysis:
- `pandas`, `json`, `matplotlib`, `seaborn`, `numpy`

To keep the workflow clean, I configured the notebook for better visuals (e.g., turned off warnings, set matplotlib style).

#### b. Reading and Inspecting Raw Data

I read the raw JSON data file using Python’s `json` library.  
Loaded the data into a Pandas DataFrame for fast, tabular exploration and displayed the first rows for a quick check.

#### c. Analyzing the Nested Data Structure

I explored the hierarchical JSON schema to understand key fields:
- Main keys: `order_id`, `customer`, `items`, `payments`, `shipments`, `reviews`, etc.
- Identified that columns like `items`, `payments`, `shipments`, and `reviews` are **nested arrays/objects** (each order can have multiple products, payment types, shipments, and reviews).

#### d. Flattening Nested Fields

For each nested field (`items`, `payments`, `shipments`, `reviews`), I:
- Expanded (“flattened”) these arrays into separate DataFrames.
- Used `order_id` as the relational key to join them back to the master table.
- Ensured the resulting DataFrames could be merged and referenced for further analysis.

#### e. Calculating Basic Statistics

I computed summary statistics such as:
- Number of orders
- Total products sold
- Payment method distribution
- Shipment statuses
- Review ratings

All DataFrames were merged to create a unified and analysis-friendly flat table.


#### f. Final Structure

By the end of this process, I had transformed complex, deeply nested JSON data into flat, ready-to-analyze DataFrames.  
This workflow highlights how to tackle real-world nested data scenarios using Python before cloud data warehousing.

[Python notebook for detailed data inspection](https://github.com/movet306/BigQuery-E-Commerce-Advanced-Data-Modeling-Integration/blob/main/olist_bigquery_nested_orders%20(1).ipynb)
---

## 2. Data Loading & Cloud Integration

### 2.1. Export to NDJSON

I exported the final structured DataFrame as newline-delimited JSON (NDJSON) — a format required for loading nested data into BigQuery.

### 2.2. Google Cloud Storage

- Uploaded the NDJSON file to a Google Cloud Storage bucket, which serves as the staging point for all bulk loads into BigQuery.

### 2.3. BigQuery Table Creation

- Created a new dataset and table (`olist_nested.olistrable`) in BigQuery.
- Enabled schema auto-detection, then reviewed and finalized the nested/array/STRUCT field mapping.
- Verified that the schema reflected the same nested structure as in the raw JSON.

---

## 3. Data Profiling & Cleaning

Before starting analytics, I performed a comprehensive data cleaning and profiling process to ensure high data quality for all subsequent analyses.

---

### 3.1. Schema Review & Profiling

- Used `INFORMATION_SCHEMA.COLUMNS` and the BigQuery UI to review the table structure and nested fields.
- Exported schema summaries and key field statistics to CSV for documentation.
- Checked all nested/array/JSON fields (e.g., `order_items`, `campaign_details`, `customer`) to understand where missing or inconsistent data could impact reporting.

---

### 3.2. Data Cleaning Approach & Techniques

#### Null/Empty Value Checks

To guarantee that every row represents a real and complete order, I removed records with missing `order_id`, missing `customer_id`, or with empty `order_items` arrays.

```sql
CREATE OR REPLACE TABLE olist-bigquery.olist_nested.olistable_cleaned AS
SELECT *
FROM olistbigquery.olistable
WHERE order_id IS NOT NULL
  AND customer.customer_id IS NOT NULL
  AND ARRAY_LENGTH(order_items) > 0
```
Standardization of Text & Numerics
I normalized text fields for consistency (trimmed, lowercased, filled missing as "unknown"), and ensured all numeric fields were correctly cast for analysis.

```sql
SELECT
  LOWER(TRIM(customer.city)) AS city_cleaned,
  IFNULL(LOWER(TRIM(customer.state)), "unknown") AS state_cleaned,
  SAFE_CAST(order_items[OFFSET(0)].price AS FLOAT64) AS first_item_price_cleaned
FROM `olist-bigquery.olist_nested.olistable_cleaned`
```
Nested/Array Data Handling (Flattening)
I used UNNEST() to flatten the order_items array, enabling product-level analysis for each order.
Standardized empty arrays and nulls to avoid ambiguity in further queries.

```sql
SELECT
  o.order_id,
  o.customer.customer_id,
  o.order_status,
  item.product_id,
  item.price
FROM
  `olist-bigquery.olist_nested.olistable_cleaned` AS o,
  UNNEST(o.order_items) AS item
```
![image](https://github.com/user-attachments/assets/32e60130-e482-4749-95af-1cfa70deac77)

Null Campaign Data Standardization
To make segmentation and filtering easier, I labeled missing or null campaign/coupon fields as 'no_campaign'.
This approach is especially useful when analyzing marketing impact or filtering campaign-specific data.

![image](https://github.com/user-attachments/assets/9300c6ec-951e-4f08-8a9b-9619e48b0920)

![image](https://github.com/user-attachments/assets/0ac55155-a91c-4e0f-84c2-c9b49bb2d5f2)

```sql
SELECT
  IFNULL(campaign_details.coupon_code, 'no_campaign') AS coupon_code_status,
  IFNULL(campaign_details.channel, 'no_campaign') AS campaign_channel_status,
  *
FROM `olist-bigquery.olist_nested.olistable_cleaned`
```
![image](https://github.com/user-attachments/assets/7ea4d0f2-eda7-45b2-adc6-721aa7f76a16)

I also used LEFT JOIN UNNEST() to standardize campaign data inside order_items, ensuring all nested coupon fields are consistently filled.

## 4. Creating a Flattened, Analysis-Ready Table

After cleaning and standardizing the nested data, I created a **flat, denormalized table** to simplify downstream analytics, BI reporting, and integration with dashboard tools like Power BI or Tableau.

### 4.1. Business Explanation

- **Why Flatten?**  
  Real-world e-commerce data is often deeply nested (multiple products per order, campaign info inside arrays, etc).  
  Most analytics, KPIs, and dashboarding tasks require a flat table where each row represents a single product in an order, with all relevant customer, order, and campaign info “joined” in the same row.
- **Benefits:**  
    - Enables row-level sales, product, and campaign analysis  
    - Simplifies integration with BI tools  
    - Eliminates ambiguity from NULLs in nested fields

### 4.2. SQL: Flattening & Standardizing the Data

```sql
-- Create a flat table: one row per product per order, all campaign fields standardized
CREATE OR REPLACE TABLE `olist-bigquery.analytics_case.olistable_flattened` AS
SELECT
  o.order_id,
  o.customer.customer_id AS customer_id,
  o.customer.city AS customer_city,
  o.customer.state AS customer_state,
  o.order_status,
  o.order_timestamp,
  -- Flatten the order_items array
  item.product_id,
  item.price,
  item.shipping_limit_date,
  item.seller_id,
  -- Standardize campaign details (replace NULLs for analysis)
  IFNULL(o.campaign_details.channel, 'no_campaign') AS campaign_channel,
  IFNULL(o.campaign_details.coupon_code, 'no_campaign') AS campaign_coupon,
  IFNULL(CAST(o.campaign_details.discount AS FLOAT64), 0) AS campaign_discount,
  IFNULL(item.campaign_details.channel, 'no_campaign') AS item_campaign_channel,
  IFNULL(item.campaign_details.coupon_code, 'no_campaign') AS item_campaign_coupon,
  IFNULL(CAST(item.campaign_details.discount AS FLOAT64), 0) AS item_campaign_discount
FROM
  `olist-bigquery.olist_nested.olistable_cleaned` AS o
LEFT JOIN UNNEST(o.order_items) AS item
```
![image](https://github.com/user-attachments/assets/82840837-76f1-419d-b354-027d7a09730a)

UNNEST(o.order_items) AS item transforms each product in the order into a separate row.

IFNULL(...) and CAST(... AS FLOAT64) ensure all campaign fields are consistent and analysis-ready.

### 4.3. Post-Creation: Table & Schema Verification
After creating the flattened table, I immediately checked the schema and row counts to ensure correctness.

```sql
SELECT
  column_name, data_type
FROM
  `olist-bigquery.analytics_case.INFORMATION_SCHEMA.COLUMNS`
WHERE
  table_name = 'olistable_flattened';

-- Row count for sanity check
SELECT COUNT(*) FROM `olist-bigquery.analytics_case.olistable_flattened`;
```
![image](https://github.com/user-attachments/assets/2f8c893e-d2e2-4991-8a53-23fce1ba643d)

![image](https://github.com/user-attachments/assets/eac61697-8dca-4070-a8d2-04d53b9f5b1a)

## 5. Data Definition Language & Table Management

During this project, my goal was not only to deliver clean, analytics-ready data, but also to **demonstrate my practical skills in advanced SQL, BigQuery-specific DDL (Data Definition Language), DML (Data Manipulation Language), and modern data modeling techniques**.

By re-engineering the data between flat and nested structures, I aimed to:

- **Showcase mastery over complex BigQuery data types** (STRUCT, ARRAY, JSON) and table design.
- Practice both DDL (creating custom schemas, altering tables) and DML (inserting, transforming, populating data) in real business scenarios.
- Simulate real-world requirements, such as building reusable data contracts, exposing API-friendly data models, or powering downstream analytics pipelines.
- Deliver an end-to-end, production-level workflow — from raw nested data to analytics-friendly flat tables, and back to robust, contract-driven nested schemas.

This approach demonstrates my hands-on proficiency with:

- **BigQuery-native data modeling** (custom nested schemas, denormalization/normalization)
- **Schema management** (CREATE, ALTER, DROP TABLE)
- **Efficient ETL & data transformation with advanced SQL**
- **Business-ready documentation and transparency for future users or teams**

#### 5.1. Creating a Custom Nested Table

I designed a custom schema for a new table, featuring:

- **order_id**: Unique identifier for each order.
- **customer**: STRUCT holding customer ID, city, and state.
- **order_items**: ARRAY of STRUCTs, each containing product, pricing, shipping, seller, and campaign info.
- **campaign_details**: STRUCT for order-level campaign metadata.
- **Timestamps, status, and all other business-critical fields**.
  
![image](https://github.com/user-attachments/assets/a04a1e18-c39c-4f65-a96a-d899435ecf83)

#### 5.2. Data Mapping & Population (Why and How)

**Purpose:**
To simulate a real-life ETL scenario, I mapped and inserted cleansed flat analytics data back into the nested schema.
This proves mastery over both flattening and re-nesting complex data, crucial for API development, data warehousing, and analytics engineering.

How I mapped each field:

order_id, order_status, order_timestamp: Directly mapped from the flat table.

customer: Mapped as a STRUCT of customer_id, city, state.

order_items: For demo purposes, used a single-item array per order (in real data, this would aggregate all items per order).

campaign_details: Both order-level and item-level campaign details mapped and typecast for integrity.

NULL and missing values: All fields mapped with IFNULL/COALESCE or CAST as needed, ensuring no structural gaps in the nested table.

```sql
CREATE OR REPLACE TABLE `olist-bigquery.analytics_case.orders_custom_schema`
(
  order_id STRING,
  customer STRUCT<
    customer_id STRING,
    city STRING,
    state STRING
  >,
  order_status STRING,
  order_timestamp TIMESTAMP,
  order_items ARRAY<
    STRUCT<
      product_id STRING,
      price FLOAT64,
      shipping_limit_date TIMESTAMP,
      seller_id STRING,
      campaign_details STRUCT<
        discount FLOAT64,
        channel STRING,
        coupon_code STRING
      >
    >
  >,
  campaign_details STRUCT<
    discount FLOAT64,
    channel STRING,
    coupon_code STRING
  >
)
```
### 5.3. Altering Table Schema (Adding, Updating, Dropping Columns)

After constructing the nested schema table, I demonstrated further mastery over **DDL and DML operations** — crucial for maintaining and evolving production data models.

#### A) Adding Columns

To simulate an evolving business requirement, I added a new column for delivery type:

```sql
ALTER TABLE `olist-bigquery.analytics_case.orders_custom_schema`
ADD COLUMN delivery_type STRING;
```
![image](https://github.com/user-attachments/assets/21eeaf10-a3eb-43fe-bfdd-74e03182fe47)

B) Populating Columns with Business Logic
Populated delivery_type based on order_status:
-
```sql
UPDATE `olist-bigquery.analytics_case.orders_custom_schema`
SET delivery_type =
  CASE
    WHEN order_status = 'delivered' THEN 'standard'
    WHEN order_status = 'shipped' THEN 'express'
    ELSE 'unknown'
  END
WHERE TRUE
```
**Why?**
Automating business logic directly in the database ensures consistent, rule-based data enrichment without relying solely on external ETL processes.

![image](https://github.com/user-attachments/assets/0f98ef69-2b8a-4730-a0fa-b037cf0f0659)

![image](https://github.com/user-attachments/assets/d0eccf5b-0dda-4239-842e-fc97ee2ca239)


**C) Dropping Columns**
Removed the column if the requirement changes or for cleanup:

```sql
ALTER TABLE `olist-bigquery.analytics_case.orders_custom_schema`
DROP COLUMN delivery_type;
```
Why?
Data models should remain lean and maintainable. Dropping unused or deprecated columns avoids confusion and unnecessary storage.

**D) Changing Column Types**
For evolving business requirements or data integrity, I changed a column type:

```sql
ALTER TABLE `olist-bigquery.analytics_case.orders_custom_schema`
ALTER COLUMN order_status SET DATA TYPE STRING;
```
![image](https://github.com/user-attachments/assets/e4e5e52b-1e87-4710-aa72-5afea67a9baf)

### 5.4. Table Management: Backup, Drop, Rename
**A) Table Backup**
Created a backup of the table before major changes:

```sql
CREATE TABLE `olist-bigquery.analytics_case.orders_custom_schema_backup` AS
SELECT * FROM `olist-bigquery.analytics_case.orders_custom_schema`;
```
**Why?**
Routine backups are best practice before large data migrations or refactoring, ensuring data recoverability.

**B) Table Deletion**
Dropped a table when it was no longer needed:
```sql
DROP TABLE `olist-bigquery.analytics_case.orders_custom_schema`;
```
![image](https://github.com/user-attachments/assets/ddc2116a-2047-4bce-bfe3-0edfbaa941a7)

**Why?**
To keep the environment tidy and avoid unnecessary storage costs.

**C) Renaming Tables**
Renamed the table that we backup to reflect a new version or after structural changes:

```sql
ALTER TABLE `olist-bigquery.analytics_case.orders_custom_schema`
RENAME TO `orders_custom_schema_v2`;
```
**Why?**
Clear versioning in table names makes production operations and handovers easier.

### 5.5. Final Table Review
Inspected the final table structure and content before **Data Manipulation Language (DML)** applications

```sql
SELECT *
FROM `olist-bigquery.analytics_case.orders_custom_schema_v2`
LIMIT 10;
```
![image](https://github.com/user-attachments/assets/6f6d9e51-cb4d-4077-8c0b-3902fc929bd0)

To review only specific columns or nested structures:
```sql
SELECT
  order_id,
  order_status,
  order_items,
  campaign_details
FROM `olist-bigquery.analytics_case.orders_custom_schema_v2`
LIMIT 5;
```
![image](https://github.com/user-attachments/assets/2f478a85-f924-4b29-b133-694641ece786)

## 6. Data Modeling & Advanced DML: Working with Nested/Struct Data

After DDL applications and preparing and flattening the data, I focused on demonstrating advanced DML operations in BigQuery, specifically tailored for nested and complex e-commerce structures.

---

### 6.1. Inserting Records with Nested Arrays & Structs

**Scenario:**  
Insert a new order record that contains a nested array of products (order_items) and structured campaign details.

```sql
INSERT INTO `olist-bigquery.analytics_case.orders_custom_schema_v2` (
  order_id,
  customer,
  order_status,
  order_timestamp,
  order_items,
  campaign_details
)
VALUES (
  'ORD_DML_001',
  STRUCT('CUST_DML_001', 'ankara', 'TR'),
  'delivered',
  CURRENT_TIMESTAMP(),
  [STRUCT(
    'PROD_DML_001', 399.0, TIMESTAMP('2025-07-08 20:00:00 UTC'), 'SELLER_DML_01',
    STRUCT(25.0, 'web', 'FIRSTORDER25')
  ),
   STRUCT(
    'PROD_DML_002', 89.0, TIMESTAMP('2025-07-08 21:00:00 UTC'), 'SELLER_DML_02',
    STRUCT(0.0, 'mobile', 'no_coupon')
  )],
  STRUCT(25.0, 'web', 'FIRSTORDER25')
)
```
![image](https://github.com/user-attachments/assets/d165d229-edbc-4d90-b668-c00c3e901998)

**Why this matters:**

Shows real-world modeling of orders: each order can have multiple products (nested array) and complex campaign info (struct).

Practice for hands-on DML skills with nested BigQuery tables, which is a critical skill in modern analytics teams.

### 6.2. Updating Nested/Struct Fields
You may need to update fields at either the top-level or inside a nested struct.

**Update a top-level field:**
```sql
UPDATE `olist-bigquery.analytics_case.orders_custom_schema_v2`
SET order_status = 'cancelled'
WHERE order_id = 'ORD_DML_001'
```

![image](https://github.com/user-attachments/assets/519e284c-a217-4cc3-be82-c126ac4b1b07)

**Update a struct field:**
```sql
UPDATE `olist-bigquery.analytics_case.orders_custom_schema_v2`
SET campaign_details = STRUCT(0.0, 'web', 'NO_CAMPAIGN')
WHERE order_id = 'ORD_DML_001'
```

![image](https://github.com/user-attachments/assets/3f1fa697-2845-444a-982a-281c74a69294)

**Business context:**
These operations reflect real post-order workflows, e.g., cancellations or campaign changes after an order is created.

### 6.3. MERGE (Upsert): Update If Exists, Insert If Not
**Scenario:**
Automate the upsert process—update an existing record if found, or insert a new one if not. This mirrors real-world ETL pipelines for data warehousing.

```sql
MERGE `olist-bigquery.analytics_case.orders_custom_schema_v2` AS T
USING (
  SELECT
    '50ba38c4dc467baab1ea2c8c7747934d' AS order_id, 
    STRUCT('CUST_MERGE', 'istanbul', 'TR') AS customer,
    'shipped' AS order_status,
    CURRENT_TIMESTAMP() AS order_timestamp,
    [STRUCT(
      'PROD_MERGE_01', 475.0, TIMESTAMP('2025-07-10 17:00:00 UTC'), 'SELLER_MERGE_01',
      STRUCT(30.0, 'web', 'SHIP30')
    )] AS order_items,
    STRUCT(30.0, 'web', 'SHIP30') AS campaign_details
) AS S
ON T.order_id = S.order_id
WHEN MATCHED THEN
  UPDATE SET
    customer = S.customer,
    order_status = S.order_status,
    order_timestamp = S.order_timestamp,
    order_items = S.order_items,
    campaign_details = S.campaign_details
WHEN NOT MATCHED THEN
  INSERT (order_id, customer, order_status, order_timestamp, order_items, campaign_details)
  VALUES (S.order_id, S.customer, S.order_status, S.order_timestamp, S.order_items, S.campaign_details)
```
![image](https://github.com/user-attachments/assets/e3b85fa9-8d54-40e9-bf3a-ea5b7020d578)

```sql
SELECT *
FROM `olist-bigquery.analytics_case.orders_custom_schema_v2`
WHERE order_id = '50ba38c4dc467baab1ea2c8c7747934d'
```
![image](https://github.com/user-attachments/assets/f88a5323-93c6-4e12-9bc5-c29efa971286)

**How this works:**

If the order_id exists, all fields are updated.

If not, a new record is inserted.

Dummy values like PROD_MERGE_01, SELLER_MERGE_01 are used to easily track/test upserts in the case study context (in real-world, these are real product/seller IDs).

**Business Value:**

Simulates nightly ETL loads and real upsert scenarios in production.

Clear separation between system-generated data and manually-injected test records for traceability.

### 6.4. Why Use UNNEST with Nested Arrays?
BigQuery supports true nested/array columns, but classical SQL analytics and BI tools require flat tables for most reporting.

What is a Nested Array?
Example: In an e-commerce order, the order_items column is an array of structs, each struct representing a product in the order.

Why UNNEST is Essential
For row-level (product-level) analytics, reporting, and aggregation, every product must appear as its own row.

UNNEST explodes arrays so you can analyze by product, calculate total revenue, top-selling items, campaign impact, etc.

**Example Query:**

```sql
SELECT
  order_id,
  item.product_id,
  item.price
FROM
  `olist-bigquery.analytics_case.orders_custom_schema_v2`,
  UNNEST(order_items) AS item
```
---
**Business Use:**

Required for granular KPIs, churn analysis, campaign ROI, and BI tool exports (Power BI/Tableau).

### 6.5. Table Schema Profiling & Analyst Interpretation

To ensure robust analytics and model development, I began by profiling the table schema using BigQuery’s `INFORMATION_SCHEMA` metadata:

```sql
SELECT
  column_name, data_type
FROM
  `olist-bigquery.analytics_case.INFORMATION_SCHEMA.COLUMNS`
WHERE
  table_name = 'orders_custom_schema_v2'
```

![image](https://github.com/user-attachments/assets/ba17a99c-cf77-4ab0-b0cb-17ab2b4d1086)

![image](https://github.com/user-attachments/assets/245fb234-8292-463d-acd5-05df61ddbf28)

**Insights according to the outcome:**

**order_items:** ARRAY<STRUCT> — Each order can contain multiple products. This field requires UNNEST for product-level analysis.

**customer, campaign_details:** STRUCT — Nested, but you can directly access fields (e.g., customer.customer_id) without unnesting.

**Other fields:** STRING/TIMESTAMP — Flat fields, directly accessible for querying and reporting.

**Key Outcomes:**
Only order_items needs to be unnested for granular (row-per-product) analytics.

No raw JSON fields exist in this schema. All nested data is modeled as STRUCT/ARRAY<STRUCT> for direct, type-safe access (e.g., customer.city, item.campaign_details.discount).

No need for JSON_EXTRACT/JSON_VALUE functions — all subfields are accessible via dot notation.

### 6.6. Flattening Nested Arrays for Analytics
To enable downstream analytics and BI reporting (e.g., in Power BI/Tableau), I created a fully flattened table with one row per order-item, carrying along both root- and nested-level campaign data:

![image](https://github.com/user-attachments/assets/1d150465-0648-4a72-8d7e-f3badd4aacbd)

```sql
CREATE OR REPLACE TABLE `olist-bigquery.analytics_case.orders_flat` AS
SELECT
  o.order_id,
  o.customer.customer_id,
  o.customer.city AS customer_city,
  o.customer.state AS customer_state,
  o.order_status,
  o.order_timestamp,
  item.product_id,
  item.price,
  item.seller_id,
  item.shipping_limit_date,
  item.campaign_details.discount AS item_campaign_discount,
  item.campaign_details.channel AS item_campaign_channel,
  item.campaign_details.coupon_code AS item_campaign_coupon,
  o.campaign_details.discount AS order_campaign_discount,
  o.campaign_details.channel AS order_campaign_channel,
  o.campaign_details.coupon_code AS order_campaign_coupon
FROM
  `olist-bigquery.analytics_case.orders_custom_schema_v2` AS o,
  UNNEST(o.order_items) AS item
```
![image](https://github.com/user-attachments/assets/bc8d7130-703f-4d77-9ff7-41e318e90b48)

---

## 7. Nested Data Analytics & Aggregate KPIs

### Overview

With the fully flattened `orders_flat` table in place, I can now perform detailed business analytics and KPI reporting at any desired granularity: order, product, customer, seller, or campaign level. This approach enables best-practice e-commerce analysis directly on BigQuery, without data export or manual joins.

**This section demonstrates how to:**
- Leverage the flat table for nested data analysis (order → items → campaign).
- Build key performance indicators (KPIs) for business stakeholders.
- Combine root-level (order) and nested-level (item/campaign) insights.
- Write scalable, reusable SQL for modern BI needs.

---
### 7.1. Example: Product-Level Revenue and Order Analysis

**Business Question:**  
*Which products drive the highest revenue and how frequently are they ordered?*

**SQL Example:**

```sql
SELECT
  product_id,
  COUNT(DISTINCT order_id) AS num_orders,
  SUM(price) AS total_revenue,
  AVG(price) AS avg_price
FROM `olist-bigquery.analytics_case.orders_flat`
GROUP BY product_id
ORDER BY total_revenue DESC
LIMIT 10;
```
![image](https://github.com/user-attachments/assets/6d79dc1f-264d-42ca-8c07-d7838a8fb37f)

**Key Findings:**

**Most sold product:** aca2eb7d00ea1a7b8ebd4e683... (527 units, ₺37,609 total revenue)

**Top-grossing product:** 99a4788cb24856965c36a24e3... (488 units, ₺43,025 total revenue)

The top 5 products range from 388 to 527 units sold, and ₺21,000 to ₺43,000 in revenue.

**Business Impact:**

**Demand Signal:** Top-selling products directly indicate customer demand and category popularity.

**Revenue Leaders:** Products with high price-per-unit may dominate total revenue, even if their volume is lower.

**Inventory Priority:** These SKUs should be prioritized in purchasing and stock planning.

**Campaign Tactics:** Bundle or cross-sell strategies can be designed around bestsellers or high-revenue items.

### 7.2. Seller Performance: Volume, Revenue, and Avg. Sales Price
**Business Question:**
Which sellers drive the most sales? What is the revenue and average sales price per seller?


```sql
SELECT
  seller_id,
  COUNT(*) AS total_sales_count,
  SUM(price) AS total_revenue,
  ROUND(AVG(price), 2) AS avg_sales_price
FROM
  `olist-bigquery.analytics_case.orders_flat`
GROUP BY
  seller_id
ORDER BY
  total_revenue DESC
LIMIT 10;
```
![image](https://github.com/user-attachments/assets/28179540-8f4e-4cf4-8463-8df54c40eb36)

**Top Performers & Insights:**

**Power sellers:** The top 3 sellers lead both in volume and revenue.

**Product Mix Effect:** Sellers with higher-priced items stand out in revenue, even if their volume is lower.

**Long Tail:** Many sellers have moderate or low volume—potential for growth via training, incentives, or new campaigns.

**Actionable Steps:** Design exclusive campaigns for the top 10 sellers, develop KPIs (sales, revenue, avg. price), and consider programs for underperformers.


### 7.3. Average Order Value (AOV) Analysis

**Business Question:**
What is the average order value (AOV)? How does it reflect purchasing behavior?

**Calculation:**
AOV = Total Revenue / Total Unique Orders

```sql
SELECT
  COUNT(DISTINCT order_id) AS total_orders,
  SUM(price) AS total_revenue,
  ROUND(SUM(price) / COUNT(DISTINCT order_id), 2) AS avg_order_value
FROM
  `olist-bigquery.analytics_case.orders_flat`
```

![image](https://github.com/user-attachments/assets/dcd89bc3-6eff-4f86-aa70-0a4d68c5b1ef)

**Results:**

**Total Orders:** 98,667

**Total Revenue:** ₺13,592,407

**AOV:** ₺137.76

---

### 7.4. Campaign Performance Analysis

#### Business Question 1:  
Which campaign or coupon code was used most? What are the total sales and revenue for campaign vs. non-campaign orders?

```sql
SELECT
  CASE
    WHEN order_campaign_coupon IN ('NO_CAMPAIGN', 'no_campaign') THEN 'No Campaign'
    ELSE 'Campaign Used'
  END AS campaign_flag,
  COUNT(*) AS total_sales_count,
  SUM(price) AS total_revenue
FROM
  `olist-bigquery.analytics_case.orders_flat`
GROUP BY
  campaign_flag
ORDER BY
  total_sales_count DESC;
```
![image](https://github.com/user-attachments/assets/cee35f03-43e8-4d69-aa02-406dfc690d6c)

**Key Findings:**

Sales and revenue are almost evenly split between campaign and non-campaign orders (e.g., "no_campaign", "SUMMER20", "WELCOME10" have nearly equal totals).

There is no major difference in aggregate revenue or sales count, indicating that campaign use does not drastically increase total revenue based on this metric.

Small/unique codes (like "SHIP30") are rare—likely demo/test records.

**Business Question 2:**
What is the share of campaign vs. non-campaign sales?

```sql
SELECT
  CASE
    WHEN order_campaign_coupon IN ('NO_CAMPAIGN', 'no_campaign') THEN 'No Campaign'
    ELSE 'Campaign Used'
  END AS campaign_flag,
  COUNT(*) AS total_sales_count,
  SUM(price) AS total_revenue
FROM
  `olist-bigquery.analytics_case.orders_flat`
GROUP BY
  campaign_flag
ORDER BY
  total_sales_count DESC;
```

![image](https://github.com/user-attachments/assets/ed1ac02a-eb7c-4bd0-9863-a315e60f5246)

**Insights:**

~66% of sales (and total revenue) come from orders with campaigns, while 34% are without.

Campaign-driven orders dominate in both volume and revenue.

This highlights the strong role of campaigns in customer activation and revenue generation.

However, true campaign impact requires further analysis of discount effect and average order value (AOV).

**Business Question 3:**
How does average order value (AOV) differ between campaign and non-campaign sales?

```sql
SELECT
  CASE
    WHEN order_campaign_coupon IN ('NO_CAMPAIGN', 'no_campaign') THEN 'No Campaign'
    ELSE 'Campaign Used'
  END AS campaign_flag,
  COUNT(*) AS total_orders,
  SUM(price) AS total_revenue,
  ROUND(SUM(price)/COUNT(*),2) AS avg_order_value
FROM
  `olist-bigquery.analytics_case.orders_flat`
GROUP BY
  campaign_flag
ORDER BY
  avg_order_value DESC;
```
![image](https://github.com/user-attachments/assets/96781e6a-22d1-4194-ba83-c2c2096cb56f)

**Findings:**

Orders without campaigns have a higher average order value, even though campaign orders outnumber them.

Total revenue is still higher in the campaign segment due to volume, not AOV.

**Strategic Takeaway:**
Campaigns increase order count and total revenue, but lower the AOV due to discounts. This is typical of price sensitivity and promo-driven customer behavior. It’s vital to balance volume vs. profitability when designing marketing actions.

**Business Question 4:**
Which coupons are most/least profitable?

```sql
SELECT
  order_campaign_coupon,
  COUNT(*) AS total_orders,
  SUM(price) AS total_revenue,
  ROUND(SUM(price)/COUNT(*),2) AS avg_order_value
FROM
  `olist-bigquery.analytics_case.orders_flat`
GROUP BY
  order_campaign_coupon
ORDER BY
  avg_order_value DESC;
```
![image](https://github.com/user-attachments/assets/a0cb3d33-6df3-4c88-9267-8194d867dfea)

**Insights:**

-Major volume comes from big, public coupons like "SUMMER20" and "WELCOME10".

-SHIP30 coupon is rarely used (possibly tests or niche promos).

### 7.5. Customer Segmentation & Regional Performance

#### Business Question 1:  
Which cities and states generate the most orders and revenue?  
What is the geographical distribution of sales?

```sql
SELECT
  customer_city,
  customer_state,
  COUNT(*) AS total_orders,
  SUM(price) AS total_revenue
FROM
  `olist-bigquery.analytics_case.orders_flat`
GROUP BY
  customer_city, customer_state
ORDER BY
  total_orders DESC
LIMIT 20;
```
![image](https://github.com/user-attachments/assets/aefd10c3-570e-435c-be4e-a78348e6ca5b)

**Key Insights:**

São Paulo (SP) leads both in order volume (17,808 orders) and revenue (₺1,914,924), followed by Rio de Janeiro (RJ) and Belo Horizonte (MG).

The top 5 cities account for a dominant share of overall e-commerce activity.

This geographic concentration suggests that marketing and campaign investments should focus on these key urban centers.

Major metropolitan areas (SP, RJ, MG) are the engines of e-commerce growth.

**Business Question 2:**
What is the ratio of successful deliveries vs. cancellations by city/state?

```sql
SELECT
  customer_city,
  customer_state,
  COUNTIF(order_status = 'delivered') AS delivered_orders,
  COUNTIF(order_status = 'cancelled') AS cancelled_orders,
  COUNT(*) AS total_orders,
  ROUND(100 * COUNTIF(order_status = 'delivered') / COUNT(*), 2) AS delivery_rate,
  ROUND(100 * COUNTIF(order_status = 'cancelled') / COUNT(*), 2) AS cancellation_rate
FROM
  `olist-bigquery.analytics_case.orders_flat`
GROUP BY
  customer_city, customer_state
ORDER BY
  total_orders DESC
LIMIT 20;
```

![image](https://github.com/user-attachments/assets/87e27603-ad29-4513-be52-fcddb3462576)

**Findings:**

Major cities have very high delivery rates (96–98%):

São Paulo: 97.71%

Belo Horizonte: 98.19%

Curitiba: 98.63%

Cancellations are extremely rare (0 in top cities)—likely due to data quality or filtering, as some cancellations are expected in real business.

High delivery success in large cities points to strong logistics operations and high customer satisfaction.

**Business Commentary:**

The largest cities dominate both orders and successful deliveries, reflecting their strategic importance as core and reference markets.

Exceptionally high delivery rates are positive for brand trust and customer loyalty.

Even small differences in delivery/cancellation rates can highlight opportunities for operational excellence.










## 🔗 Project Owner
**Mert Ovet**  
[LinkedIn: linkedin.com/in/mertovet](https://linkedin.com/in/mertovet)

