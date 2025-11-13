-- Tên file: dbt_olist/models/gold/fct_order_items.sql
{{
  config(
    materialized='incremental',
    unique_key=['order_id', 'order_item_id']
  )
}}

SELECT
    it.order_id,
    it.order_item_id,
    it.product_id,
    it.seller_id,
    ord.customer_id,
    CAST(ord.order_purchase_timestamp AS DATE) AS order_date,
    it.price,
    it.freight_value
    
FROM {{ source('olist_silver', 'order_items') }} AS it
LEFT JOIN {{ source('olist_silver', 'orders') }} AS ord
    ON it.order_id = ord.order_id
WHERE ord.order_status IN ('delivered', 'shipped')

{% if is_incremental() %}
  AND ord.order_purchase_timestamp > (SELECT DATE_ADD(MAX(order_purchase_timestamp), -3) FROM {{ this }})
{% endif %}