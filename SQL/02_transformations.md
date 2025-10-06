# Transformations to Sliver

This file contains all SQL transformation logic used to clean and normalize the original Olist dataset. Each raw CSV file was ingested into the **Bronze layer**, and then transformed into clean, query-ready tables in the **Silver layer** using Athena SQL.

All outputs were written to S3 in **Parquet format** with **Snappy compression**, and partitioned where applicable.

---

## 🧹 Cleaned Tables Summary (Silver Layer)

### 1. `orders_clean`
- Cast all timestamps, lowercase IDs
- Derived `order_year` and `order_month` partitions
- Flagged bad records where delivery happened before purchase

```sql
CREATE TABLE olist_db.orders_clean
WITH (
    format = 'PARQUET', write_compression = 'SNAPPY',
    external_location = 's3://olist-ecommerce-analysis-project/silver/orders_clean/',
    partitioned_by = ARRAY['order_year', 'order_month']
) AS
SELECT
    -- Normalize identifiers to lowercase for consistent joins
    lower(col0) AS order_id,
    lower(col1) AS customer_id,
    lower(col2) AS order_status,
    
    -- Normalize timestamp column names to *_ts and cast explicitly
    TRY_CAST(col3 AS timestamp) AS order_purchase_ts,
    TRY_CAST(col4 AS timestamp) AS order_approved_ts,
    TRY_CAST(col5 AS timestamp) AS delivered_carrier_ts,
    TRY_CAST(col6 AS timestamp) AS delivered_customer_ts,
    TRY_CAST(col7 AS timestamp) AS estimated_delivery_ts,

    -- Data quality flag: delivered earlier than purchase ⇒ bad record
    CASE WHEN TRY_CAST(col6 AS timestamp) IS NOT null
        AND TRY_CAST(col3 AS timestamp) IS NOT null
        AND TRY_CAST(col6 AS timestamp) < TRY_CAST(col3 AS timestamp)
        THEN 1 ELSE 0 
    END AS bad_record,
    
     -- Partitioning columns derived from purchase timestamp
    year(TRY_CAST(col3 AS timestamp)) AS order_year, 
    month(TRY_CAST(col3 AS timestamp)) AS order_month
FROM olist_db.raw_orders;
````

### 2. `customers_clean`
- Lowercased city and ID fields
- Cast zip prefix to INT
- Normalized state to uppercase

```sql
CREATE TABLE olist_db.customer_clean
WITH (
    format = 'PARQUET', write_compression = 'SNAPPY',
    external_location = 's3://olist-ecommerce-analysis-project/silver/customers_clean/'
) AS
SELECT DISTINCT
    -- Row-level id key (lowercase for consistent joins)
    lower(customer_id) AS customer_id,
    
    -- Cross-order de-dup key
    lower(customer_unique_id) AS customer_unique_id,
    
    -- Zip prefix as integer (explicit cast)
    TRY_CAST(customer_zip_code_prefix AS integer) AS customer_zip_code_prefix,

    -- Normalize city to lowercase, state to uppercase
    lower(customer_city)  AS customer_city,
    upper(customer_state) AS customer_state
FROM olist_db.raw_customers
````

### 3. `order_items_clean`
- Cast numeric fields (price, freight)
- Calculated net/gross amount per line
- Joined with `orders_clean` to inherit time partitions

```sql
CREATE TABLE olist_db.order_items_clean
WITH (
    format = 'PARQUET', write_compression = 'SNAPPY',
    external_location = 's3://olist-ecommerce-analysis-project/silver/order_items_clean/',
    partitioned_by = ARRAY['order_year', 'order_month']
) AS
SELECT
    -- Normalize ids
    lower(i.order_id)                 AS order_id,
    TRY_CAST(i.order_item_id AS int)  AS order_item_id,
    lower(i.product_id)               AS product_id,
    lower(i.seller_id)                AS seller_id,
    
    -- Timestamps
    TRY_CAST(i.shipping_limit_date AS timestamp) AS shipping_limit_ts,
    
    -- Monetary fields (explicit cast to double)
    TRY_CAST(i.price         AS double) AS price,
    TRY_CAST(i.freight_value AS double) AS freight_value,
    
    -- Net amount and gross amount
    TRY_CAST(i.price AS double)                                           AS line_amount_net,
    (TRY_CAST(i.price AS double) + TRY_CAST(i.freight_value AS double))   AS line_amount_gross,
    
    -- Inherit order partitions (year/month) from the normalized orders table
    o.order_year,
    o.order_month
FROM olist_db.raw_order_items i
JOIN olist_db.orders_clean o ON i.order_id=o.order_id
WHERE i.price>=0 AND i.freight_value>=0;
````

### 4. `order_payments_clean`
- Normalized enums and IDs
- Filtered out negative values
- Joined with orders to get partitions

```sql
CREATE TABLE olist_db.order_payments_clean
WITH (
    format = 'PARQUET', write_compression = 'SNAPPY',
    external_location = 's3://olist-ecommerce-analysis-project/silver/order_payments_clean/',
    partitioned_by = ARRAY['order_year', 'order_month']
) AS
SELECT
  -- Normalize id + enums
  lower(p.order_id)         AS order_id,
  lower(p.payment_type)     AS payment_type,
  
  -- Explicit numeric casts
  TRY_CAST(p.payment_sequential AS int) AS payment_sequential,
  TRY_CAST(p.payment_installments AS int) AS payment_installments,
  TRY_CAST(p.payment_value AS double) AS payment_value,
  
  -- Inherit partitions from orders
  o.order_year, 
  o.order_month
FROM olist_db.raw_payments p
JOIN olist_db.orders_clean o 
ON p.order_id=o.order_id
WHERE p.payment_value>=0;
````

### 5. `order_reviews_clean`
- Cast `review_score` to INT
- Created time partitions by review creation date
- Preserved nullable fields for message/title

```sql
CREATE TABLE olist_db.order_reviews_clean
WITH (
    format = 'PARQUET', write_compression = 'SNAPPY',
    external_location = 's3://olist-ecommerce-analysis-project/silver/order_reviews_clean/',
    partitioned_by = ARRAY['review_year','review_month']
) AS
SELECT
  -- Normalize identifiers for consistent downstream joins
  lower(review_id)  AS review_id,
  lower(order_id)   AS order_id,

  -- Explicit numeric cast;
  TRY_CAST(review_score AS int) AS review_score,
  
  -- Keep original text;
  review_comment_title, 
  review_comment_message,
  
  -- Normalize timestamps to *_ts and cast explicitly
  TRY_CAST(review_creation_date AS timestamp) AS review_creation_ts,
  TRY_CAST(review_answer_timestamp AS timestamp) AS review_answer_ts,
  
  -- Partitions based on review creation timestamp
  year(TRY_CAST(review_creation_date AS timestamp)) AS review_year,
  month(TRY_CAST(review_creation_date AS timestamp)) AS review_month
FROM olist_db.raw_order_reviews
WHERE review_score BETWEEN 1 AND 5 OR review_score IS NULL;
````

### 6. `products_clean`
- Fixed typos in raw CSV column names (e.g. `lenght`)
- Calculated product volume when dimensions were valid
- All numeric fields cast explicitly

```sql
CREATE TABLE olist_db.products_clean
WITH (
    format = 'PARQUET', write_compression = 'SNAPPY',
    external_location = 's3://olist-ecommerce-analysis-project/silver/products_clean/'
) AS
SELECT
    -- Normalize id + category name
    lower(product_id)                AS product_id,
    lower(product_category_name)     AS product_category_name,
  
    -- The raw CSV uses misspelled fields: product_name_lenght, product_description_lenght
    TRY_CAST(product_name_lenght AS int) AS product_name_length,
    TRY_CAST(product_description_lenght AS int) AS product_description_length,
    TRY_CAST(product_length_cm AS double) AS product_length_cm,
  
    -- Physical attributes as doubles
    TRY_CAST(product_photos_qty AS int) AS product_photos_qty,
    TRY_CAST(product_weight_g AS double) AS product_weight_g,
    TRY_CAST(product_height_cm AS double) AS product_height_cm,
    TRY_CAST(product_width_cm AS double) AS product_width_cm,
  
    -- Volume only when all three dims are present and positive (NULL otherwise)
    CASE 
        WHEN product_length_cm>0 
            AND product_height_cm>0 
            AND product_width_cm>0
        THEN product_length_cm*product_height_cm*product_width_cm 
    ELSE NULL 
    END AS product_volume_cm3
FROM olist_db.raw_products;
````

### 7. `sellers_clean`
- Lowercase city, uppercase state
- Cast zip prefix
- De-duplicated rows

```sql
CREATE TABLE olist_db.sellers_clean
WITH (
    format = 'PARQUET', write_compression = 'SNAPPY',
    external_location = 's3://olist-ecommerce-analysis-project/silver/sellers_clean/'
) AS
SELECT DISTINCT
    -- Normalize id/city/state for robust joins and grouping
    lower(seller_id)                              AS seller_id,
    TRY_CAST(seller_zip_code_prefix AS int) AS seller_zip_code_prefix,
    lower(seller_city) AS seller_city,
    upper(seller_state) AS seller_state
FROM olist_db.raw_sellers;
````

### 8. `geolocation_clean`
- Cast latitude/longitude to DOUBLE
- Basic range validation (e.g. positive coords)
- Standardized casing for city/state

```sql
CREATE TABLE olist_db.geolocation_clean
WITH (
    format = 'PARQUET', write_compression = 'SNAPPY',
    external_location = 's3://olist-ecommerce-analysis-project/silver/geolocation_clean/'
) AS
SELECT
    -- Zip prefix as integer
    TRY_CAST(geolocation_zip_code_prefix AS int) AS zip_code_prefix,
    
    -- Coordinates as double with basic geographic range validation
    TRY_CAST(geolocation_lat AS double) AS lat,
    TRY_CAST(geolocation_lng AS double) AS lng,
    
    -- Normalize city/state casing
    lower(geolocation_city) AS geolocation_city,
    upper(geolocation_state) AS geolocation_state  
FROM olist_db.raw_geolocation
````

### 9. `product_category_translation_clean`
- Translated Portuguese category names to English
- Raw file had generic columns; cast `col0` and `col1`

```sql
CREATE TABLE olist_db.product_category_translation_clean
WITH (
    format = 'PARQUET', write_compression = 'SNAPPY',
    external_location = 's3://olist-ecommerce-analysis-project/silver/product_category_translation_clean/'
) AS
SELECT
    -- Raw CSV came in with generic column names (col0 / col1)
    lower(col0) AS product_category_name,
    lower(col1) AS product_category_name_en
FROM olist_db.raw_product_category
````

