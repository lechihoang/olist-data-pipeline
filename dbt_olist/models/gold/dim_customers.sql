-- Dimension table for Customers
-- Creates a table in gold layer with customer information

SELECT
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state,
    customer_zip_code_prefix,
    geolocation_lat,
    geolocation_lng
FROM {{ source('olist_silver', 'customers') }}