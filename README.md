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

## 🔗 Project Owner
**Mert Ovet**  
[LinkedIn: linkedin.com/in/mertovet](https://linkedin.com/in/mertovet)

