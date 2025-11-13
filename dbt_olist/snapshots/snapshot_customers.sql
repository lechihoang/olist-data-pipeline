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

-- ⭐️ SỬA LỖI: DỌN DẸP (DEDUPLICATE) NGUỒN TRƯỚC
-- Đảm bảo chúng ta chỉ trả về 1 dòng DUY NHẤT cho mỗi 'customer_unique_id'
WITH source AS (
    SELECT 
        *,
        -- 1. Đánh số (row_number) cho các đơn hàng của cùng 1 khách hàng,
        --    lấy đơn hàng "mới nhất" (dựa trên 'customer_id') lên đầu (số 1)
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
WHERE rn = 1 -- ⭐️ 2. Chỉ chọn dòng "mới nhất" (số 1)

{% endsnapshot %}