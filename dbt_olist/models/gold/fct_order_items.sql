{{
  config(
    materialized='incremental',
    unique_key=['order_id', 'order_item_id']
  )
}}

WITH order_items AS (
    SELECT * FROM {{ source('olist_silver', 'order_items') }}
),
orders AS (
    SELECT * FROM {{ source('olist_silver', 'orders') }}
    WHERE 
        order_status IN ('delivered', 'shipped')
        {% if is_incremental() %}
          AND order_purchase_timestamp > (
            SELECT COALESCE(DATE_SUB(MAX(order_date), 3), '1900-01-01')
            FROM {{ this }}
          )
        {% endif %}
)
SELECT
    it.order_id,
    it.order_item_id,
    it.product_id,
    it.seller_id,
    ord.customer_id,
    CAST(ord.order_purchase_timestamp AS DATE) AS order_date,
    ord.order_delivered_customer_date,
    ord.order_estimated_delivery_date,
    it.price,
    it.freight_value
    
FROM order_items AS it
LEFT JOIN orders AS ord
    ON it.order_id = ord.order_id
WHERE
    ord.order_id IS NOT NULL