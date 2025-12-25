{% snapshot snapshot_products %}
{{
    config(
        target_database=var('catalog'),
        target_schema=var('gold_schema'),
        unique_key='product_id',
        strategy='check',
        check_cols=['product_category_name'],
        invalidate_hard_deletes=True
    )
}}

/*
    SCD Type 2 Snapshot for Products
    
    - Tracks changes in product category over time
    - Uses product_id as the unique key
    
    dbt automatically adds:
    - dbt_scd_id: Unique ID for each version
    - dbt_updated_at: Timestamp when record was updated
    - dbt_valid_from: When this version became effective
    - dbt_valid_to: When this version expired (NULL = current)
*/

SELECT 
    product_id,
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
FROM {{ source('olist_silver', 'products') }}

{% endsnapshot %}
