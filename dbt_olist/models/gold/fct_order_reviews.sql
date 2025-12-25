{{
  config(
    materialized='incremental',
    unique_key=['review_id', 'order_id']
  )
}}

/*
    fct_order_reviews - Order Reviews Fact Table
    
    Grain: One row per review
    
    Foreign Keys:
    - customer_unique_id -> dim_customers (SCD2, use is_current=TRUE for current state)
    
    For point-in-time analysis, join with dimension tables using:
    WHERE review_creation_date BETWEEN dim.valid_from AND COALESCE(dim.valid_to, '9999-12-31')
*/

WITH reviews AS (
    SELECT * FROM {{ source('olist_silver', 'order_reviews') }}
),

orders AS (
    SELECT 
        order_id,
        customer_id
    FROM {{ source('olist_silver', 'orders') }}
),

customers AS (
    SELECT 
        customer_id,
        customer_unique_id
    FROM {{ source('olist_silver', 'customers') }}
)

SELECT
    -- Keys
    r.review_id,
    r.order_id,
    c.customer_unique_id,  -- FK to dim_customers (SCD2)
    
    -- Timestamps
    r.review_creation_date,
    r.review_answer_timestamp,
    
    -- Attributes
    r.review_comment_message,
    
    -- Measures
    r.review_score
    
FROM reviews AS r
INNER JOIN orders AS o
    ON r.order_id = o.order_id
INNER JOIN customers AS c
    ON o.customer_id = c.customer_id

{% if is_incremental() %}
WHERE r.review_creation_date > (
    SELECT COALESCE(DATE_SUB(MAX(review_creation_date), INTERVAL 3 DAY), '1900-01-01')
    FROM {{ this }}
)
{% endif %}
