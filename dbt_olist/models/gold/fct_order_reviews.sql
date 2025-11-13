-- Tên file: dbt_olist/models/gold/fct_order_reviews.sql
{{
  config(
    materialized='incremental',
    unique_key='review_id'
  )
}}

SELECT
    r.review_id,
    r.order_id,
    o.customer_id,
    CAST(r.review_creation_date AS DATE) AS review_date,
    r.review_comment_message,
    r.review_score
    
FROM {{ source('olist_silver', 'order_reviews') }} AS r
LEFT JOIN {{ source('olist_silver', 'orders') }} AS o
    ON r.order_id = o.order_id

{% if is_incremental() %}
  AND r.review_creation_date > (SELECT DATE_ADD(MAX(review_creation_date), -3) FROM {{ this }})
{% endif %}