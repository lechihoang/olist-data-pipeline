{{
  config(
    materialized='incremental',
    unique_key=['order_id', 'order_item_id']
  )
}}

/*
    fct_order_items - Order Line Items Fact Table
    
    Grain: One row per order item
    
    Foreign Keys:
    - customer_unique_id -> dim_customers (SCD2, use is_current=TRUE for current state)
    - product_id -> dim_products (SCD2)
    - seller_id -> dim_sellers (SCD2)
    
    For point-in-time analysis, join with dimension tables using:
    WHERE order_purchase_timestamp BETWEEN dim.valid_from AND COALESCE(dim.valid_to, '9999-12-31')
*/

WITH order_items AS (
    SELECT * FROM {{ source('olist_silver', 'order_items') }}
),

orders AS (
    SELECT * FROM {{ source('olist_silver', 'orders') }}
    WHERE 
        order_status IN ('delivered', 'shipped')
        {% if is_incremental() %}
          AND order_purchase_timestamp > (
            SELECT COALESCE(DATE_SUB(MAX(order_purchase_timestamp), INTERVAL 3 DAY), '1900-01-01')
            FROM {{ this }}
          )
        {% endif %}
),

customers AS (
    SELECT 
        customer_id,
        customer_unique_id
    FROM {{ source('olist_silver', 'customers') }}
)

SELECT
    -- Keys
    it.order_id,
    it.order_item_id,
    it.product_id,
    it.seller_id,
    c.customer_unique_id,  -- FK to dim_customers (SCD2)
    
    -- Timestamps
    ord.order_purchase_timestamp,
    ord.order_delivered_customer_date,
    ord.order_estimated_delivery_date,
    it.shipping_limit_date,
    
    -- Measures
    it.price,
    it.freight_value
    
FROM order_items AS it
INNER JOIN orders AS ord
    ON it.order_id = ord.order_id
INNER JOIN customers AS c
    ON ord.customer_id = c.customer_id
