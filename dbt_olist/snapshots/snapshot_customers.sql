{% snapshot snapshot_customers %}
{{
    config(
        target_database=var('catalog'),
        target_schema=var('gold_schema'),
        unique_key='customer_unique_id',
        strategy='check',
        check_cols=['customer_zip_code_prefix', 'customer_city', 'customer_state'],
        invalidate_hard_deletes=True
    )
}}

/*
    SCD Type 2 Snapshot for Customers
    
    - Tracks changes in customer address (zip, city, state) over time
    - Uses customer_unique_id as the unique key (represents a person)
    - Deduplicates by customer_unique_id, keeping the most recent customer_id record
    
    dbt automatically adds:
    - dbt_scd_id: Unique ID for each version
    - dbt_updated_at: Timestamp when record was updated
    - dbt_valid_from: When this version became effective
    - dbt_valid_to: When this version expired (NULL = current)
*/

WITH source AS (
    SELECT 
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        geolocation_lat,
        geolocation_lng,
        -- Keep the most recent customer_id for each customer_unique_id
        ROW_NUMBER() OVER (
            PARTITION BY customer_unique_id 
            ORDER BY customer_id DESC
        ) AS rn
    FROM {{ source('olist_silver', 'customers') }}
)

SELECT 
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    geolocation_lat,
    geolocation_lng
FROM source
WHERE rn = 1

{% endsnapshot %}
