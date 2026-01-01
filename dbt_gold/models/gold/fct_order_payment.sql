{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'payment_sequential'],
        incremental_strategy='merge'
    )
}}

WITH payment AS (
    SELECT 
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value
    FROM {{ source('olist_silver', 'order_payment') }}
),

order_data AS (
    SELECT 
        order_id,
        order_purchase_timestamp
    FROM {{ source('olist_silver', 'order') }}
),

payment_with_timestamp AS (
    SELECT
        p.order_id,
        p.payment_sequential,
        p.payment_type,
        p.payment_installments,
        p.payment_value,
        o.order_purchase_timestamp
        
    FROM payment AS p
    INNER JOIN order_data AS o
        ON p.order_id = o.order_id
)

SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
FROM payment_with_timestamp

{% if is_incremental() %}
WHERE order_purchase_timestamp > (
    SELECT COALESCE(
        DATEADD(DAY, -3, MAX(order_purchase_timestamp)),
        TIMESTAMP '1900-01-01'
    )
    FROM {{ this }} t
    INNER JOIN {{ source('olist_silver', 'order') }} o ON t.order_id = o.order_id
)
{% endif %}
