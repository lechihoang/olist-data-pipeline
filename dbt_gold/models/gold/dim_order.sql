{{
    config(
        materialized='incremental',
        unique_key='order_sk',
        incremental_strategy='merge',
        merge_update_columns=['valid_to', 'is_current']
    )
}}

WITH snapshot_order AS (
    SELECT 
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        dbt_valid_from,
        dbt_valid_to,
        dbt_updated_at
    FROM {{ ref('snapshot_order') }}
),

customer AS (
    SELECT 
        customer_id,
        customer_unique_id
    FROM {{ source('olist_silver', 'customer') }}
),

snapshot_with_customer AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['o.order_id', 'o.dbt_valid_from']) }} AS order_sk,
        o.order_id,
        c.customer_unique_id,
        o.order_status,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,
        
        CASE 
            WHEN o.order_delivered_customer_date IS NOT NULL 
                 AND o.order_purchase_timestamp IS NOT NULL
            THEN DATEDIFF(DAY, o.order_purchase_timestamp, o.order_delivered_customer_date)
            ELSE NULL
        END AS delivery_days,
        
        CASE 
            WHEN o.order_delivered_carrier_date IS NOT NULL 
                 AND o.order_approved_at IS NOT NULL
            THEN DATEDIFF(DAY, o.order_approved_at, o.order_delivered_carrier_date)
            ELSE NULL
        END AS shipping_days,
        
        CASE 
            WHEN o.order_delivered_customer_date IS NOT NULL 
                 AND o.order_delivered_carrier_date IS NOT NULL
            THEN DATEDIFF(DAY, o.order_delivered_carrier_date, o.order_delivered_customer_date)
            ELSE NULL
        END AS carrier_days,
        
        CASE 
            WHEN o.order_approved_at IS NOT NULL 
                 AND o.order_purchase_timestamp IS NOT NULL
            THEN DATEDIFF(HOUR, o.order_purchase_timestamp, o.order_approved_at)
            ELSE NULL
        END AS approval_hours,
        
        CASE 
            WHEN o.order_delivered_customer_date IS NOT NULL 
                 AND o.order_estimated_delivery_date IS NOT NULL
            THEN o.order_delivered_customer_date <= o.order_estimated_delivery_date
            ELSE NULL
        END AS is_on_time,
        
        CASE 
            WHEN o.order_status = 'delivered' THEN TRUE
            ELSE FALSE
        END AS is_delivered,
        
        o.dbt_valid_from AS valid_from,
        o.dbt_valid_to AS valid_to,
        CASE WHEN o.dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current,
        o.dbt_updated_at

    FROM snapshot_order AS o
    INNER JOIN customer AS c
        ON o.customer_id = c.customer_id
)

SELECT * FROM snapshot_with_customer

{% if is_incremental() %}
WHERE order_sk NOT IN (SELECT order_sk FROM {{ this }})
   OR dbt_updated_at > (SELECT COALESCE(MAX(dbt_updated_at), '1900-01-01') FROM {{ this }})
{% endif %}
