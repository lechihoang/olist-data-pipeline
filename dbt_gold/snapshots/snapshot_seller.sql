{% snapshot snapshot_seller %}
{{
    config(
        target_database=var('catalog'),
        target_schema=var('gold_schema'),
        unique_key='seller_id',
        strategy='check',
        check_cols=['seller_zip_code_prefix', 'seller_city', 'seller_state'],
        invalidate_hard_deletes=True
    )
}}

/*
    SCD Type 2 Snapshot for Seller
    
    - Tracks changes in seller address (zip, city, state) over time
    - Uses seller_id as the unique key
    
    dbt automatically adds:
    - dbt_scd_id: Unique ID for each version
    - dbt_updated_at: Timestamp when record was updated
    - dbt_valid_from: When this version became effective
    - dbt_valid_to: When this version expired (NULL = current)
*/

WITH seller AS (
    SELECT * FROM {{ source('olist_silver', 'seller') }}
),
geolocation AS (
    SELECT * FROM {{ source('olist_silver', 'geolocation') }}
)

SELECT
    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,
    g.geolocation_lat,
    g.geolocation_lng
FROM seller AS s
LEFT JOIN geolocation AS g
    ON s.seller_zip_code_prefix = g.geolocation_zip_code_prefix

{% endsnapshot %}
