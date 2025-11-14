{{
  config(
    materialized='incremental',
    unique_key=['order_id', 'payment_sequential']
  )
}}

{% if is_incremental() %}
  {%- set max_timestamp_query -%}
    SELECT DATE_ADD(MAX(order_date), -3) FROM {{ this }} 
  {%- endset -%}
  
  {%- set max_loaded_timestamp_result = run_query(max_timestamp_query) -%}
  
  {%- if execute and max_loaded_timestamp_result.rows -%}
    {%- set max_loaded_timestamp = max_loaded_timestamp_result.rows[0][0] -%}
  {%- else -%}
    {%- set max_loaded_timestamp = '1900-01-01 00:00:00' -%} 
  {%- endif -%}
{% endif %}

WITH payments AS (
    SELECT * FROM {{ source('olist_silver', 'order_payments') }}
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
    p.order_id,
    p.payment_sequential,
    o.customer_id,
    CAST(o.order_purchase_timestamp AS DATE) AS order_date, 
    p.payment_type,
    p.payment_installments,
    p.payment_value
    
FROM payments AS p
LEFT JOIN orders AS o
    ON p.order_id = o.order_id
WHERE
    o.order_id IS NOT NULL