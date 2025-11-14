{{
  config(
    materialized='incremental',
    unique_key=['order_id', 'order_item_id']
  )
}}

{% if is_incremental() %}
  {%- set max_timestamp_query -%}
    SELECT DATE_ADD(MAX(order_date), -3) FROM {{ this }}
  {%- endset -%}
  
  {%- set max_loaded_timestamp_result = run_query(max_timestamp_query) -%}
  
  {%- if execute and max_loaded_timestamp_result.rows[0][0] is not none -%}
    {%- set max_loaded_timestamp = max_loaded_timestamp_result.rows[0][0] -%}
  {%- else -%}
    {%- set max_loaded_timestamp = '1900-01-01 00:00:00' -%} 
  {%- endif -%}
{% endif %}

WITH order_items AS (
    SELECT * FROM {{ source('olist_silver', 'order_items') }}
),
orders AS (
    SELECT * FROM {{ source('olist_silver', 'orders') }}
    WHERE 
        order_status IN ('delivered', 'shipped')
        {% if is_incremental() %}
          AND order_purchase_timestamp > '{{ max_loaded_timestamp }}'
        {% endif %}
)
SELECT
    it.order_id,
    it.order_item_id,
    it.product_id,
    it.seller_id,
    ord.customer_id,
    CAST(ord.order_purchase_timestamp AS DATE) AS order_date,
    it.price,
    it.freight_value
    
FROM order_items AS it
LEFT JOIN orders AS ord
    ON it.order_id = ord.order_id
WHERE
    ord.order_id IS NOT NULL