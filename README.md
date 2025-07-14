# BigQuery-E-Commerce-Advanced-Data-Modeling-Integration

> **Project Owner:** Mert Ovet  
> [LinkedIn: linkedin.com/in/mertovet](https://linkedin.com/in/mertovet)

---

## 📄 Project Overview

This project demonstrates an end-to-end real-world scenario of advanced e-commerce data modeling and analytics in Google BigQuery, starting from complex nested JSON data to a fully clean, queryable, and analytics-ready schema.

- **Data Source:**  
  Synthetic Olist E-commerce data, exported as nested NDJSON (newline-delimited JSON), generated and pre-processed in Python.
- **Stack:**  
  Python (for data structuring & export), Google Cloud Storage (staging), BigQuery (data warehouse & analytics).

  ## Data Source

The data used in this case study comes from the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) on Kaggle. 

## Project Authorship & Methodology

All data analysis, feature engineering, KPIs, customer/seller segmentation, and business strategies presented in this project were **entirely designed, implemented, and documented by myself after thoroughly exploring the raw data**.  

No third-party analyses, code, or strategic frameworks were used. Every business question, analytical approach, and SQL/Python implementation carried out by myself.

---

## 1. Data Flow: Step-by-Step Process

### 1.1. Python — Data Preparation & Nested Structuring

#### a. Library Imports & Notebook Setup

I started by importing all essential Python libraries for data analysis:
- `pandas`, `json`, `numpy`

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

All DataFrames were merged to create a unified and analysis-friendly flat table.

#### e. Final Structure

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

**Most sold product:** aca2eb7d00ea1a7b8ebd4e683... (527 units, $37,609 total revenue)

**Top-grossing product:** 99a4788cb24856965c36a24e3... (488 units, $43,025 total revenue)

The top 5 products range from 388 to 527 units sold, and $21,000 to $43,000 in revenue.

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

**Total Revenue:** $13,592,407

**AOV:** $137.76

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

São Paulo (SP) leads both in order volume (17,808 orders) and revenue ($1,914,924), followed by Rio de Janeiro (RJ) and Belo Horizonte (MG).

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


---

### 7.6. Time-Based Sales Volume Analysis

#### Business Question 1:  
How do monthly order counts and revenue change? Which months are the busiest?

```sql
SELECT
  EXTRACT(YEAR FROM order_timestamp) AS order_year,
  EXTRACT(MONTH FROM order_timestamp) AS order_month,
  COUNT(*) AS total_orders,
  SUM(price) AS total_revenue
FROM
  `olist-bigquery.analytics_case.orders_flat`
GROUP BY
  order_year, order_month
ORDER BY
  order_year, order_month;
```

![image](https://github.com/user-attachments/assets/a3ba1bbb-4c58-450a-bcb7-aa7b6d4bf0cd)

![image](https://github.com/user-attachments/assets/ae2ad23f-02fb-46bd-887a-47b9e0b97d0a)

**Key Findings:**

**Early Period:** In 2016, order volume is very low (startup or partial data).

**2017:** Rapid growth trend—especially in the second half. November 2017 peaks (8,665 orders, nearly $1 million revenue).

**2018:** Monthly orders and revenue stabilize at high levels (8,000+ orders, ~$950,000–1,000,000 revenue).

**2025 Data:** Only a few orders—likely test or erroneous records. Should be excluded from strategic analysis.

**Seasonality & Trend Observations:**

**Year-end Spike:** November/December (Black Friday, year-end campaigns) show major sales peaks.

**Stabilization:** 2018 shows consistent high demand—indicating market maturity.

**Growth Curve:** Initial period spent finding product-market fit, then rapid scaling.

**Business Question 2:**
How does order volume and revenue distribute by day of the week?

```sql
SELECT
  FORMAT_DATE('%A', DATE(order_timestamp)) AS order_day,
  COUNT(*) AS total_orders,
  SUM(price) AS total_revenue
FROM
  `olist-bigquery.analytics_case.orders_flat`
GROUP BY
  order_day
ORDER BY
  total_orders DESC;
```

![image](https://github.com/user-attachments/assets/6a3d3e29-5ba6-4e73-9bc1-0f68131277ec)

**Key Findings:**

**Busiest Days:** Monday, Tuesday, and Wednesday see the most orders and revenue.

**Slowest Days:** Saturday and Sunday have noticeably lower volumes.

**Explanation:** Weekdays (especially start of week) are preferred for replenishment, campaign deadlines, and routine purchases. Weekends see a drop, likely due to physical shopping, leisure, or delayed deliveries.

**Business Recommendations:**

Schedule campaigns and promotions for Mondays/Tuesdays and business hours for maximum impact.

Stock and logistics planning should anticipate peak periods (week start, workday mornings/afternoons).

Customer support can be reinforced during busiest time slots.

**Business Question 3:**
What are the peak order hours during the day?

```sql
SELECT
  EXTRACT(HOUR FROM order_timestamp) AS order_hour,
  COUNT(*) AS total_orders,
  SUM(price) AS total_revenue
FROM
  `olist-bigquery.analytics_case.orders_flat`
GROUP BY
  order_hour
ORDER BY
  order_hour;
```

![image](https://github.com/user-attachments/assets/4bb331a1-8b0e-4727-ab42-459489c533f7)

![image](https://github.com/user-attachments/assets/fe0f1bf5-355b-4033-9c60-7f942b9f4bd1)

**Hourly Trends:**

**Night/Early Morning (00:00–06:00):** Low order activity.

**Morning (07:00–12:00):** Orders rise quickly—peaking at 09:00–12:00 (5,000–7,400 orders/hour).

**Afternoon/Evening (13:00–19:00):** Highest activity—especially 14:00–17:00 (7,000–7,600 orders/hour).

**Late Evening (20:00–23:00):** Gradual decline, but still significant volume (4,600–6,500 orders/hour).

**Business Recommendations:**

**Campaigns:** Launch in the morning or late afternoon for higher engagement.

**Digital Marketing:** Schedule emails/SMS between 09:00–11:00 or 15:00–17:00.

**Operations:** Prepare for spikes at workday transitions; ensure inventory and fulfillment teams are ready.

Time-based segmentation of e-commerce sales allows for optimal campaign timing, improved logistics, and better customer experience—directly impacting revenue and customer satisfaction.

---

### 7.7. Advanced Segmentation & Window Function Analysis

#### Business Question 1:  
Which customers have placed the most orders and spent the most?

```sql
SELECT
  customer_id,
  COUNT(order_id) AS total_orders,
  SUM(price) AS total_spent,
  RANK() OVER (ORDER BY SUM(price) DESC) AS revenue_rank
FROM
  `olist-bigquery.analytics_case.orders_flat`
GROUP BY
  customer_id
ORDER BY
  total_spent DESC
LIMIT 10;
---
```
### 7.7. Advanced Segmentation & Window Function Analysis

#### Business Question 1:  
Which customers have placed the most orders and spent the most?

```sql
SELECT
  customer_id,
  COUNT(order_id) AS total_orders,
  SUM(price) AS total_spent,
  RANK() OVER (ORDER BY SUM(price) DESC) AS revenue_rank
FROM
  `olist-bigquery.analytics_case.orders_flat`
GROUP BY
  customer_id
ORDER BY
  total_spent DESC
LIMIT 10;
---
```
### 7.7. Advanced Segmentation & Window Function Analysis

#### Business Question 1:  
Which customers have placed the most orders and spent the most?

```sql
SELECT
  customer_id,
  COUNT(order_id) AS total_orders,
  SUM(price) AS total_spent,
  RANK() OVER (ORDER BY SUM(price) DESC) AS revenue_rank
FROM
  `olist-bigquery.analytics_case.orders_flat`
GROUP BY
  customer_id
ORDER BY
  total_spent DESC
LIMIT 10;
```
![image](https://github.com/user-attachments/assets/12d08ea8-2599-411e-bb4c-c0f0da53af35)

**Key Insights:**

**Top customer (1617b1357756262bfa56ab541...):** 8 orders, $13,440 total spent.

Some in top 10 made only one order but spent >$4,500 (single high-value transaction).

Others (e.g., 6th place): Frequent orders + high total spend—loyal, high-value repeat customer.

**Business Segmentation:**

**Multi-order, high spend:** Ideal for retention programs (loyalty, VIP campaigns).

**Single big-ticket buyer:** Likely premium/corporate or campaign-driven—target for reactivation.

Segmenting by behavior enables more effective marketing and customer success strategies.

**Business Question 2:**
Which sellers generate the most revenue and what is the sales distribution?

```sql
SELECT
  seller_id,
  COUNT(order_id) AS total_orders,
  SUM(price) AS total_revenue,
  RANK() OVER (ORDER BY SUM(price) DESC) AS seller_rank
FROM
  `olist-bigquery.analytics_case.orders_flat`
GROUP BY
  seller_id
ORDER BY
  total_revenue DESC
LIMIT 10;
```
![image](https://github.com/user-attachments/assets/3b203e12-20b3-4b34-89f7-86f6cbd61a1b)

**Key Insights:**

**Top seller (4869f7a5dfa277a7dca6462dcf...):** 1,156 orders, $229,472 revenue.

**Pareto effect:** Top 3 sellers generate almost $650,000—market is heavily concentrated.

**Low-volume, high-revenue sellers:** Niche/expensive items, B2B or special deals.

**High-volume, high-revenue sellers:** Core market drivers.

**Strategic Actions:**

Tailor partnerships and exclusive campaigns for “power sellers.”

Identify “niche” sellers for premium product strategies.

**Business Question 3:**
What is each customer’s order lifecycle (first/last order, total spent)?

```sql
SELECT
  customer_id,
  MIN(order_timestamp) AS first_order,
  MAX(order_timestamp) AS last_order,
  COUNT(order_id) AS total_orders,
  SUM(price) AS total_spent
FROM
  `olist-bigquery.analytics_case.orders_flat`
GROUP BY
  customer_id
ORDER BY
  total_spent DESC
LIMIT 10;
```
![image](https://github.com/user-attachments/assets/cd87469d-e6fa-4117-b93e-06a50e4863c0)

**Key Insights:**

Some customers spend thousands in a single order; others are repeat buyers.

Lifecycle pattern: Many customers are one-time, high-ticket buyers—typical for special campaigns or business buyers.

Customers with spaced-out first/last orders and high frequency = true loyalists.

**Business Use Cases:**

**Reactivation:** Target those with a long time since last order (“We miss you!” campaigns).

**Lifecycle Marketing:** Personalized journeys based on purchase cadence.

**Business Question 4:**
Which customer-seller pairs have the strongest commercial ties?

![image](https://github.com/user-attachments/assets/45167e6e-e1fc-4e73-965d-309c85e3333f)

```sql
SELECT
  customer_id,
  seller_id,
  COUNT(order_id) AS total_orders,
  SUM(price) AS total_spent
FROM
  `olist-bigquery.analytics_case.orders_flat`
GROUP BY
  customer_id, seller_id
ORDER BY
  total_spent DESC
LIMIT 10;
```

**Key Insights:**

Some customers make all their purchases from a single seller, indicating strong brand or business relationships (B2B, bulk buyers, VIPs).

Others interact with multiple sellers—opportunity for cross-sell/upsell strategies.

**Actionable Takeaways:**

Build VIP/loyalty lists per seller.

Personalized CRM and offer management for customers with strong seller ties.

**Business Question 5:**
Who are the “loyal customers”? (At least 2 orders & above average total spend)

**Step 1: Calculate Average Total Customer Spend**

```sql
SELECT AVG(total_spent) AS avg_spent
FROM (
  SELECT customer_id, SUM(price) AS total_spent
  FROM `olist-bigquery.analytics_case.orders_flat`
  GROUP BY customer_id
)
```
![image](https://github.com/user-attachments/assets/c6a75106-a194-4cb0-bab9-4d73b59d4802)


**Step 2: Segment Loyal Customers**

```sql
WITH customer_summary AS (
  SELECT
    customer_id,
    COUNT(*) AS total_orders,
    SUM(price) AS total_spent
  FROM
    `olist-bigquery.analytics_case.orders_flat`
  GROUP BY customer_id
),
avg_stats AS (
  SELECT AVG(total_spent) AS avg_spent FROM customer_summary
)
SELECT
  COUNT(*) AS loyal_customer_count,
  SUM(total_spent) AS loyal_customer_revenue
FROM
  customer_summary, avg_stats
WHERE
  total_orders >= 2
  AND total_spent > avg_stats.avg_spent
```

![image](https://github.com/user-attachments/assets/43b90118-8ac9-4e09-bb8e-317350624a78)


**Result:**

Only 4.9% of customers (4,862 of 98,667) are “loyal,” yet they generate ~12% of total revenue ($1,616,704 out of $13,592,407).

The loyal segment has much higher than average order value.

**Business Impact:**

**Small but mighty:** A small group of repeat, high-spend customers account for a significant share of revenue.

**Growth lever:** Loyalty programs, personalized offers, and retention campaigns targeting this segment can drive outsized revenue gains.

**Opportunity:** Boosting loyal customer count even modestly = large impact on topline.

**Step 3: Total Customer Base & Revenue**

To put our loyal customer segment into perspective, here are the total figures for the entire customer base:

```sql
SELECT
  COUNT(DISTINCT customer_id) AS total_customers,
  SUM(price) AS total_revenue
FROM
  `olist-bigquery.analytics_case.orders_flat`
```

![image](https://github.com/user-attachments/assets/dcac120b-3b87-4a24-a3d6-83fc7272d373)

**Result:**

**Total customers:** 98,667

**Total revenue:** $13,592,407

**Interpretation:**

Out of nearly 99,000 customers, only ~4.9% (4,862) are classified as “loyal”—yet this segment contributes over $1.6M (about 12%) to total revenue.

The data clearly shows: loyal, repeat customers are disproportionately valuable.

Any incremental gain in this segment would have a significant impact on overall business results.

## 8. Campaign Usage Segmentation & Advanced Customer Analytics

### 8.1. **Campaign Flag Normalization & DDL Enhancement**

#### Why Create a Permanent `campaign_flag` Field?

In large-scale e-commerce analytics, accurate segmentation by **campaign usage** is crucial for understanding customer behavior, measuring campaign ROI, and building loyalty strategies. However, campaign identifiers (like coupon codes) can be stored inconsistently in raw data (e.g., `"no_campaign"`, `"NO_CAMPAIGN"`, `NULL`, etc.), making robust analysis difficult.

**To enable reliable, scalable analysis:**
- I used SQL DDL (Data Definition Language) to add a new column, `campaign_flag`, directly to the master flat table (`orders_flat`).
- I then **standardized all campaign code variations** (e.g., `"no_campaign"`, `"NO_CAMPAIGN"`, etc.) to a single, lowercased value (`"no_campaign"`).
- This ensures that all further analytics—across segments, time, or campaigns—are clean, consistent, and repeatable.

#### Implementation Steps

1. **Add a Permanent Campaign Flag Column:**
    ```sql
    ALTER TABLE `olist-bigquery.analytics_case.orders_flat`
    ADD COLUMN campaign_flag STRING;
    ```
    ![image](https://github.com/user-attachments/assets/a02afa32-3d2f-4826-b167-3e6d6ea91113)


2. **Standardize Campaign Code Values:**
    All variations of "no_campaign" are updated to a single value.
    ```sql
    UPDATE `olist-bigquery.analytics_case.orders_flat`
    SET order_campaign_coupon = 'no_campaign'
    WHERE LOWER(order_campaign_coupon) = 'no_campaign';
    ```
    

3. **Populate the Campaign Flag Column:**
    Customers with `"no_campaign"` coupon are labeled as "Not Using Campaigns", others as "Campaign Used":
    ```sql
    UPDATE `olist-bigquery.analytics_case.orders_flat`
    SET campaign_flag =
      CASE
        WHEN LOWER(order_campaign_coupon) = 'no_campaign' THEN 'Not Using Campaigns'
        ELSE 'Campaign Used'
      END
    WHERE campaign_flag IS NULL;
    ```
![image](https://github.com/user-attachments/assets/9fde89d5-a8bc-46b5-af92-65b7f8e0986e)

> **Why This Matters:**  
> Without this normalization step, all downstream analyses (segmenting customers, measuring campaign impact, CLV by campaign, etc.) would be error-prone and potentially misleading. By handling this with DDL and UPDATEs, I ensured that the data model is **analytics-ready, repeatable, and robust**—key for professional BI/analytics environments.

---

### 8.2. **Campaign User vs. Non-User Analysis**

#### Business Question  
*Do customers who use campaigns differ in their purchase frequency and spend compared to those who don’t?*

To answer this, I segmented all customers into two groups:
- **"Using Campaigns"**: Customers who used a campaign/coupon code at least once.
- **"Not Using Campaigns"**: Customers who never used any campaign/coupon.

For each group, I calculated:
- Number of unique customers
- Total orders
- Total revenue
- Average orders per customer
- Average revenue per customer

```sql
SELECT
  customer_campaign_type,
  COUNT(DISTINCT customer_id) AS customer_count,
  SUM(total_orders) AS total_orders,
  SUM(total_spent) AS total_revenue,
  ROUND(SUM(total_orders)/COUNT(DISTINCT customer_id),2) AS avg_orders_per_customer,
  ROUND(SUM(total_spent)/COUNT(DISTINCT customer_id),2) AS avg_revenue_per_customer
FROM (
  SELECT
    customer_id,
    CASE
      WHEN SUM(CASE WHEN campaign_flag = 'Campaign Used' THEN 1 ELSE 0 END) > 0 THEN 'Using Campaigns'
      ELSE 'Not Using Campaigns'
    END AS customer_campaign_type,
    COUNT(*) AS total_orders,
    SUM(price) AS total_spent
  FROM `olist-bigquery.analytics_case.orders_flat`
  GROUP BY customer_id
)
GROUP BY customer_campaign_type
```

![image](https://github.com/user-attachments/assets/457a4b3f-0b0d-4254-a514-661bf38ce507)

**Key Insights & Commentary**
Campaign usage is widespread: About two-thirds of all customers have used a campaign at least once.

Average orders/revenue per customer is similar for both segments, suggesting campaigns drive customer base growth, not just higher individual spend.

Total revenue is higher for the "campaign user" segment, but per-customer metrics are nearly identical—implying campaigns expand the customer base, rather than drastically increasing basket size.

**Strategic note:** In this business, campaigns are effective at growing reach, but don’t necessarily change the depth of customer engagement. This insight is crucial for campaign ROI calculations and loyalty planning.

#### 8.3. Customer Value (CLV) Segmentation
**Purpose**
To identify which customers create the most value, I built a CLV segmentation by dividing all customers into three equal-sized groups (terciles) by total spend.

```sql
WITH clv_base AS (
  SELECT
    customer_id,
    SUM(price) AS total_spent
  FROM
    `olist-bigquery.analytics_case.orders_flat`
  GROUP BY
    customer_id
),
clv_segmented AS (
  SELECT
    *,
    NTILE(3) OVER (ORDER BY total_spent) AS clv_segment
  FROM clv_base
)
SELECT
  CASE
    WHEN clv_segment = 1 THEN 'Low CLV'
    WHEN clv_segment = 2 THEN 'Mid CLV'
    WHEN clv_segment = 3 THEN 'High CLV'
  END AS clv_segment_label,
  COUNT(*) AS customer_count,
  SUM(total_spent) AS total_clv,
  ROUND(AVG(total_spent),2) AS avg_clv_per_customer
FROM clv_segmented
GROUP BY clv_segment_label
ORDER BY clv_segment_label
```
![image](https://github.com/user-attachments/assets/d3dd6958-be0e-47d4-bbb6-e775cd2bdc7c)

**Strategic Takeaways**
**High CLV segment:** Represents 1/3 of customers, but accounts for ~68% of total revenue (classic Pareto effect).

**Low CLV segment:** Same size as others, but delivers only ~8% of revenue.

**Actions:**

Focus retention and loyalty programs on high CLV customers.

Use targeted cross-sell/up-sell strategies for mid-CLV group.

Analyze low-CLV group for activation or cost-reduction opportunities.

### 8.4. Churn & Reactivation Analysis
**Business Objective**
Identify customer lifecycle stages and potential for reactivation.

**Segment definitions:**

**Active:** Purchased in last 90 days

**At Risk:** Last purchase 91-180 days ago

**Churned:** >180 days since last purchase

```sql
WITH churn_segments AS (
  SELECT
    customer_id,
    CASE
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(MAX(order_timestamp)), DAY) <= 90 THEN 'Active'
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(MAX(order_timestamp)), DAY) <= 180 THEN 'At Risk'
      ELSE 'Churned'
    END AS churn_segment
  FROM
    `olist-bigquery.analytics_case.orders_flat`
  GROUP BY
    customer_id
)

SELECT
  churn_segment,
  COUNT(DISTINCT customer_id) AS customer_count
FROM
  churn_segments
GROUP BY
  churn_segment
ORDER BY
  customer_count DESC;
```

**Key Observations**
Majority of customers in the data are "churned" due to data freshness; only a handful remain active (dataset caveat).

In a real-world scenario, high churn rates should trigger urgent reactivation and retention efforts.

Segmenting customers by recency enables targeted marketing, e.g. "We Miss You" campaigns.

### 8.5. RFM Segmentation
**Why RFM?**
RFM (Recency, Frequency, Monetary) is a proven framework to quantify and segment customers for retention, upsell, and personalized campaigns.

**Process:**

Calculate recency (days since last order), frequency (total orders), and monetary value (total spend).

Score each metric from 1 to 5, then assign segment labels (e.g. Champions, Loyal, At Risk, Lost, etc.)

#### **Step 1: Calculate Recency, Frequency, Monetary for Each Customer**

```sql
WITH last_order AS (
  SELECT
    customer_id,
    MAX(CAST(order_timestamp AS DATE)) AS last_order_date,    -- Most recent purchase date
    COUNT(order_id) AS frequency,                             -- Total number of orders
    SUM(price) AS monetary                                    -- Total spend
  FROM
    `olist-bigquery.analytics_case.orders_flat`
  GROUP BY customer_id
)
SELECT
  customer_id,
  last_order_date,
  frequency,
  monetary
FROM last_order
ORDER BY monetary DESC
LIMIT 10;
```
**Output:**
Each row shows for every customer: last purchase date, order count, and total spend.

**Step 2: Score Each Customer (R, F, M) — Quantile Binning**
Assign scores (1–5) for each R, F, M using NTILE().

R Score: Lower value = more recent = higher score

F & M Score: Higher value = higher score

```sql
WITH last_order AS (
  SELECT
    customer_id,
    MAX(CAST(order_timestamp AS DATE)) AS last_order_date,
    COUNT(order_id) AS frequency,
    SUM(price) AS monetary
  FROM
    `olist-bigquery.analytics_case.orders_flat`
  GROUP BY customer_id
),
rfm_scores AS (
  SELECT
    customer_id,
    DATE_DIFF(DATE('2025-07-09'), last_order_date, DAY) AS recency,
    frequency,
    monetary,
    NTILE(5) OVER (ORDER BY DATE_DIFF(DATE('2025-07-09'), last_order_date, DAY) ASC) AS r_score,   -- Most recent = 5
    NTILE(5) OVER (ORDER BY frequency DESC) AS f_score,                                             -- Most frequent = 5
    NTILE(5) OVER (ORDER BY monetary DESC) AS m_score                                               -- Highest spend = 5
  FROM last_order
)
SELECT
  customer_id,
  recency,
  frequency,
  monetary,
  r_score,
  f_score,
  m_score
FROM rfm_scores
ORDER BY m_score DESC, f_score DESC, r_score DESC
LIMIT 10;
```
**Output:**
For each customer, you now have scores from 1 (lowest) to 5 (highest) for recency, frequency, and monetary value.

**Step 3: Label Customers by RFM Segment**
Assign business-meaningful segments based on their RFM score combinations.
**For example:**

```sql
SELECT
  customer_id,
  recency,
  frequency,
  monetary,
  r_score,
  f_score,
  m_score,
  CASE
    WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
    WHEN r_score >= 3 AND f_score >= 4 THEN 'Loyal Customers'
    WHEN r_score >= 4 AND f_score BETWEEN 2 AND 3 THEN 'Potential Loyalists'
    WHEN r_score = 5 AND f_score <= 2 THEN 'Recent Customers'
    WHEN r_score BETWEEN 3 AND 4 AND f_score <= 2 THEN 'Promising'
    WHEN r_score BETWEEN 2 AND 3 AND f_score BETWEEN 2 AND 3 THEN 'Customers Needing Attention'
    WHEN r_score <= 2 AND f_score >= 4 THEN 'At Risk'
    WHEN r_score = 1 AND f_score >= 4 THEN 'Can''t Lose Them'
    WHEN r_score = 2 AND f_score BETWEEN 2 AND 3 THEN 'About To Sleep'
    WHEN r_score = 1 AND f_score <= 2 THEN 'Lost'
    ELSE 'Others'
  END AS rfm_segment
FROM rfm_scores
ORDER BY rfm_segment, monetary DESC
LIMIT 20;
```

**Champions:** Very recent, frequent, and high-spending

**Loyal Customers:** Frequent, regular shoppers

**At Risk / Lost:** Haven’t purchased in a long time, could churn

**Step 4: Final All-in-One RFM Segmentation Query**
Here is the fully integrated version for direct, actionable customer segmentation:

```sql
WITH last_order AS (
  SELECT
    customer_id,
    MAX(CAST(order_timestamp AS DATE)) AS last_order_date,
    COUNT(order_id) AS frequency,
    SUM(price) AS monetary
  FROM
    `olist-bigquery.analytics_case.orders_flat`
  GROUP BY customer_id
),
rfm_scores AS (
  SELECT
    customer_id,
    DATE_DIFF(DATE('2025-07-09'), last_order_date, DAY) AS recency,
    frequency,
    monetary,
    NTILE(5) OVER (ORDER BY DATE_DIFF(DATE('2025-07-09'), last_order_date, DAY) ASC) AS r_score,
    NTILE(5) OVER (ORDER BY frequency DESC) AS f_score,
    NTILE(5) OVER (ORDER BY monetary DESC) AS m_score
  FROM last_order
)
SELECT
  customer_id,
  recency,
  frequency,
  monetary,
  r_score,
  f_score,
  m_score,
  CASE
    WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
    WHEN r_score >= 3 AND f_score >= 4 THEN 'Loyal Customers'
    WHEN r_score >= 4 AND f_score BETWEEN 2 AND 3 THEN 'Potential Loyalists'
    WHEN r_score = 5 AND f_score <= 2 THEN 'Recent Customers'
    WHEN r_score BETWEEN 3 AND 4 AND f_score <= 2 THEN 'Promising'
    WHEN r_score BETWEEN 2 AND 3 AND f_score BETWEEN 2 AND 3 THEN 'Customers Needing Attention'
    WHEN r_score <= 2 AND f_score >= 4 THEN 'At Risk'
    WHEN r_score = 1 AND f_score >= 4 THEN 'Can''t Lose Them'
    WHEN r_score = 2 AND f_score BETWEEN 2 AND 3 THEN 'About To Sleep'
    WHEN r_score = 1 AND f_score <= 2 THEN 'Lost'
    ELSE 'Others'
  END AS rfm_segment
FROM rfm_scores
ORDER BY rfm_segment, monetary DESC;
```
![image](https://github.com/user-attachments/assets/98741ac1-a038-4a12-8a4c-dd66b6d609e4)

### RFM Segmentation — Executive Summary & Key Insights

Using the RFM segmentation model, I grouped customers based on their **Recency** (how recently they purchased), **Frequency** (how often they purchase), and **Monetary** (how much they spend). This allows us to identify distinct behavioral segments, such as *Champions*, *Loyal Customers*, *At Risk*, and *Lost*.

**Key Insights:**
- **Champions and Loyal Customers** (high RFM scores) make up a small but extremely valuable portion of the customer base, contributing disproportionately to total revenue.
- **Recent Customers** represent new or reactivated buyers with recent activity but lower frequency/monetary value—an opportunity for onboarding and upsell campaigns.
- **At Risk** and **Lost** segments signal churn risk: these customers have not purchased recently and may require targeted win-back or retention offers.
- **Promising** and **Potential Loyalists** are emerging segments that can be nurtured with loyalty programs and targeted engagement.

**Business Impact:**
- RFM segmentation enables **personalized marketing, loyalty strategy, and churn prevention**. By focusing efforts on the highest-value and highest-risk segments, businesses can maximize ROI and drive sustainable revenue growth.
- The results demonstrate the classic Pareto principle: a small share of customers (top RFM segments) generate the majority of revenue.

This actionable segmentation lays the foundation for smarter customer targeting, effective retention campaigns, and data-driven business decisions.

## 🔗 Project Owner
**Mert Ovet**  
[LinkedIn: linkedin.com/in/mertovet](https://linkedin.com/in/mertovet)

