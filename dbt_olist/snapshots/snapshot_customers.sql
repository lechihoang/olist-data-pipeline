{% snapshot snapshot_customers %}
    
{{
    config(
      unique_key='customer_unique_id',
      strategy='check',
      check_cols=['customer_zip_code_prefix', 'customer_city', 'customer_state']
    )
}}

SELECT 
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    geolocation_lat,
    geolocation_lng
FROM {{ source('olist_silver', 'customers') }}

{% endsnapshot %}