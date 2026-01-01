{{
    config(
        materialized='incremental',
        unique_key=['review_id', 'order_id'],
        incremental_strategy='merge'
    )
}}

WITH review AS (
    SELECT 
        review_id,
        order_id,
        review_score,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp
    FROM {{ source('olist_silver', 'order_review') }}
),

order_data AS (
    SELECT order_id
    FROM {{ source('olist_silver', 'order') }}
)

SELECT
    r.review_id,
    r.order_id,
    r.review_creation_date,
    r.review_answer_timestamp,
    r.review_comment_message,
    r.review_score
    
FROM review AS r
INNER JOIN order_data AS o
    ON r.order_id = o.order_id

{% if is_incremental() %}
WHERE r.review_creation_date > (
    SELECT COALESCE(
        DATEADD(DAY, -3, MAX(review_creation_date)),
        TIMESTAMP '1900-01-01'
    )
    FROM {{ this }}
)
{% endif %}
