-- Property 10: Price Aggregation Consistency
-- Validates: Requirements 6.2
-- For any execution of the pipeline, the sum of price in fct_order_items SHALL equal
-- the sum of price in silver.order_items for delivered/shipped orders.
-- This test returns a row if sums don't match (0 rows = pass)

WITH gold_sum AS (
    SELECT COALESCE(SUM(price), 0) AS total_price
    FROM {{ ref('fct_order_items') }}
),
silver_sum AS (
    SELECT COALESCE(SUM(oi.price), 0) AS total_price
    FROM {{ source('olist_silver', 'order_items') }} oi
    INNER JOIN {{ source('olist_silver', 'orders') }} o
        ON oi.order_id = o.order_id
    WHERE o.order_status IN ('delivered', 'shipped')
)
SELECT
    g.total_price AS gold_total_price,
    s.total_price AS silver_total_price,
    ABS(g.total_price - s.total_price) AS difference
FROM gold_sum g
CROSS JOIN silver_sum s
WHERE ABS(g.total_price - s.total_price) > 0.01
