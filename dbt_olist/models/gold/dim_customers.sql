{{
    config(
        materialized='table'
    )
}}

/*
    dim_customers - SCD Type 2 Customer Dimension
    
    Source: snapshot_customers (dbt snapshot with SCD2 tracking)
    
    Key columns:
    - customer_sk: Surrogate key (unique per version)
    - customer_unique_id: Natural key (unique per person)
    - valid_from, valid_to, is_current: SCD Type 2 tracking columns
    
    Usage in queries:
    - Current state: WHERE is_current = TRUE
    - Point-in-time: WHERE timestamp BETWEEN valid_from AND COALESCE(valid_to, '9999-12-31')
*/

SELECT
    -- Surrogate Key (hash of natural key + valid_from for uniqueness)
    {{ dbt_utils.generate_surrogate_key(['customer_unique_id', 'dbt_valid_from']) }} AS customer_sk,
    
    -- Natural Key
    customer_unique_id,
    
    -- Attributes
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    geolocation_lat,
    geolocation_lng,
    
    -- SCD Type 2 Columns
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to,
    CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current

FROM {{ ref('snapshot_customers') }}
