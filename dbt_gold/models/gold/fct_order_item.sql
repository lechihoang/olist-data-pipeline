{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'order_item_id'],
        incremental_strategy='merge'
    )
}}

WITH order_item AS (
    SELECT 
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value
    FROM {{ source('olist_silver', 'order_item') }}
),

order_data AS (
    SELECT 
        order_id,
        order_purchase_timestamp
    FROM {{ source('olist_silver', 'order') }}
),

order_item_with_timestamp AS (
    SELECT
        it.order_id,
        it.order_item_id,
        it.product_id,
        it.seller_id,
        it.shipping_limit_date,
        it.price,
        it.freight_value,
        o.order_purchase_timestamp
        
    FROM order_item AS it
    INNER JOIN order_data AS o
        ON it.order_id = o.order_id
)

SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value
FROM order_item_with_timestamp

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
