{{
    config(
        materialized='table'
    )
}}

/*
    dim_sellers - SCD Type 2 Seller Dimension
    
    Source: snapshot_sellers (dbt snapshot with SCD2 tracking)
    
    Key columns:
    - seller_sk: Surrogate key (unique per version)
    - seller_id: Natural key (unique per seller)
    - valid_from, valid_to, is_current: SCD Type 2 tracking columns
    
    Usage in queries:
    - Current state: WHERE is_current = TRUE
    - Point-in-time: WHERE timestamp BETWEEN valid_from AND COALESCE(valid_to, '9999-12-31')
*/

SELECT
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key(['seller_id', 'dbt_valid_from']) }} AS seller_sk,
    
    -- Natural Key
    seller_id,
    
    -- Attributes
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    geolocation_lat,
    geolocation_lng,
    
    -- SCD Type 2 Columns
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to,
    CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current

FROM {{ ref('snapshot_sellers') }}
