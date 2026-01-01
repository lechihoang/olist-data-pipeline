{% snapshot snapshot_order %}
{{
    config(
        target_database=var('catalog'),
        target_schema=var('gold_schema'),
        unique_key='order_id',
        strategy='check',
        check_cols=['order_status'],
        invalidate_hard_deletes=True
    )
}}

/*
    SCD Type 2 Snapshot for Order
    
    - Tracks changes in order_status over time
    - Uses order_id as the unique key
    - Captures the full order lifecycle: created → approved → shipped → delivered
    
    dbt automatically adds:
    - dbt_scd_id: Unique ID for each version
    - dbt_updated_at: Timestamp when record was updated
    - dbt_valid_from: When this version became effective
    - dbt_valid_to: When this version expired (NULL = current)
    
    Note: Delivery timestamps are NOT tracked as they follow a "fill-in" pattern
    (NULL → value, set once) rather than a "change" pattern.
*/

SELECT 
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
FROM {{ source('olist_silver', 'order') }}

{% endsnapshot %}
