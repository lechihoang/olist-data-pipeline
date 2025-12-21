-- Property 7: Row Count Consistency for Order Items
-- Validates: Requirements 5.1
-- For any execution of the pipeline, the count of rows in fct_order_items SHALL equal
-- the count of order_items in silver layer that have matching orders with status 'delivered' or 'shipped'.
-- This test returns a row if counts don't match (0 rows = pass)

WITH gold_count AS (
    SELECT COUNT(*) AS cnt
    FROM {{ ref('fct_order_items') }}
),
silver_count AS (
    SELECT COUNT(*) AS cnt
    FROM {{ source('olist_silver', 'order_items') }} oi
    INNER JOIN {{ source('olist_silver', 'orders') }} o
        ON oi.order_id = o.order_id
    WHERE o.order_status IN ('delivered', 'shipped')
)
SELECT
    g.cnt AS gold_row_count,
    s.cnt AS silver_row_count,
    g.cnt - s.cnt AS difference
FROM gold_count g
CROSS JOIN silver_count s
WHERE g.cnt != s.cnt
