{{
  config(
    materialized='incremental',
    unique_key=['review_id', 'order_id']
  )
}}

{% if is_incremental() %}
  {%- set max_timestamp_query -%}
    SELECT DATE_ADD(MAX(review_date), -3) FROM {{ this }}
  {%- endset -%}
  
  {%- set max_loaded_timestamp_result = run_query(max_timestamp_query) -%}
  
  {%- if execute and max_loaded_timestamp_result.rows[0][0] is not none -%}
    {%- set max_loaded_timestamp = max_loaded_timestamp_result.rows[0][0] -%}
  {%- else -%}
    {%- set max_loaded_timestamp = '1900-01-01 00:00:00' -%} 
  {%- endif -%}
{% endif %}

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
  WHERE r.review_creation_date > '{{ max_loaded_timestamp }}'
{% endif %}