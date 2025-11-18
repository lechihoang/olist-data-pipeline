{{
  config(
    materialized='incremental',
    unique_key=['order_id', 'payment_sequential']
  )
}}

WITH payments AS (
    SELECT * FROM {{ source('olist_silver', 'order_payments') }}
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