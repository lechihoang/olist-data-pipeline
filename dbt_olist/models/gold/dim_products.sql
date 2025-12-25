{{
    config(
        materialized='table'
    )
}}

/*
    dim_products - SCD Type 2 Product Dimension
    
    Source: snapshot_products (dbt snapshot with SCD2 tracking)
    
    Key columns:
    - product_sk: Surrogate key (unique per version)
    - product_id: Natural key (unique per product)
    - valid_from, valid_to, is_current: SCD Type 2 tracking columns
    
    Usage in queries:
    - Current state: WHERE is_current = TRUE
    - Point-in-time: WHERE timestamp BETWEEN valid_from AND COALESCE(valid_to, '9999-12-31')
*/

SELECT
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key(['product_id', 'dbt_valid_from']) }} AS product_sk,
    
    -- Natural Key
    product_id,
    
    -- Attributes
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    
    -- SCD Type 2 Columns
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to,
    CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current

FROM {{ ref('snapshot_products') }}
