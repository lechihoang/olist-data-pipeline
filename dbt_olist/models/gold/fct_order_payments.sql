-- Tên file: dbt_olist/models/gold/fct_order_payments.sql
{{
  config(
    materialized='incremental',
    unique_key=['order_id', 'payment_sequential']
  )
}}

SELECT
    p.order_id,
    p.payment_sequential,
    o.customer_id,
    CAST(o.order_purchase_timestamp AS DATE) AS order_date,
    p.payment_type,
    p.payment_installments,
    p.payment_value
    
FROM {{ source('olist_silver', 'order_payments') }} AS p
LEFT JOIN {{ source('olist_silver', 'orders') }} AS o
    ON p.order_id = o.order_id
WHERE o.order_status IN ('delivered', 'shipped')

{% if is_incremental() %}
  AND o.order_purchase_timestamp > (SELECT DATE_ADD(MAX(order_purchase_timestamp), -3) FROM {{ this }})
{% endif %}