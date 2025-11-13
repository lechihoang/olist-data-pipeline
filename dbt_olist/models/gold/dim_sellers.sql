-- Tên file: dbt_olist/models/gold/dim_sellers.sql
WITH sellers AS (
    SELECT * FROM {{ source('olist_silver', 'sellers') }}
),
geolocation AS (
    SELECT * FROM {{ source('olist_silver', 'geolocation') }} -- ⭐️ Đọc bảng geo "sạch"
)
SELECT
    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,
    g.geolocation_lat, -- ⭐️ Làm giàu (enrich) tại đây
    g.geolocation_lng
FROM sellers AS s
LEFT JOIN geolocation AS g
    ON s.seller_zip_code_prefix = g.geolocation_zip_code_prefix