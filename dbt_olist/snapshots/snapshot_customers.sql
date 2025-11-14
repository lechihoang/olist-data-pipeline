{% snapshot snapshot_customers %}
    
{{
    config(
      target_database='olist_project',
      target_schema='gold',
      unique_key='customer_unique_id',
      strategy='check',
      check_cols=['customer_zip_code_prefix', 'customer_city', 'customer_state']
    )
}}

WITH source AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_unique_id 
            ORDER BY customer_id DESC
        ) AS rn
    FROM {{ source('olist_silver', 'customers') }}
)
SELECT 
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    geolocation_lat,
    geolocation_lng
FROM source
WHERE rn = 1

{% endsnapshot %}