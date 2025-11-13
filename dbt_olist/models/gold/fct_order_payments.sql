{{
  config(
    materialized='incremental',
    unique_key=['order_id', 'payment_sequential']
  )
}}

-- ⭐️ SỬA LỖI: Lấy mốc thời gian (timestamp) "mới nhất" TỪ CỘT "ĐÍCH"
{% if is_incremental() %}
  {%- set max_timestamp_query -%}
    SELECT DATE_ADD(MAX(order_date), -3) FROM {{ this }} -- ⭐️ SỬA: Dùng 'order_date'
  {%- endset -%}
  
  {%- set max_loaded_timestamp_result = run_query(max_timestamp_query) -%}
  
  {%- if execute and max_loaded_timestamp_result.rows -%}
    {%- set max_loaded_timestamp = max_loaded_timestamp_result.rows[0][0] -%}
  {%- else -%}
    {%- set max_loaded_timestamp = '1900-01-01 00:00:00' -%} 
  {%- endif -%}
{% endif %}

-- Lấy các bảng nguồn
WITH payments AS (
    SELECT * FROM {{ source('olist_silver', 'order_payments') }}
),
orders AS (
    SELECT * FROM {{ source('olist_silver', 'orders') }}
    WHERE 
        order_status IN ('delivered', 'shipped')
        -- ⭐️ SỬA LỖI: Dùng "biến" (variable) Jinja
        {% if is_incremental() %}
          -- ⭐️ SỬA: So sánh 'order_purchase_timestamp' (cột nguồn)
          AND order_purchase_timestamp > '{{ max_loaded_timestamp }}'
        {% endif %}
)

-- Xây dựng bảng Fact
SELECT
    p.order_id,
    p.payment_sequential,
    o.customer_id,
    CAST(o.order_purchase_timestamp AS DATE) AS order_date, -- ⭐️ Đổi tên ở đây
    p.payment_type,
    p.payment_installments,
    p.payment_value
    
FROM payments AS p
LEFT JOIN orders AS o
    ON p.order_id = o.order_id
WHERE
    o.order_id IS NOT NULL