-- Property 9: Payment Value Aggregation Consistency
-- Validates: Requirements 6.1
-- For any execution of the pipeline, the sum of payment_value in fct_order_payments SHALL equal
-- the sum of payment_value in silver.order_payments for valid orders (delivered/shipped).
-- This test returns a row if sums don't match (0 rows = pass)

WITH gold_sum AS (
    SELECT COALESCE(SUM(payment_value), 0) AS total_value
    FROM {{ ref('fct_order_payments') }}
),
silver_sum AS (
    SELECT COALESCE(SUM(op.payment_value), 0) AS total_value
    FROM {{ source('olist_silver', 'order_payments') }} op
    INNER JOIN {{ source('olist_silver', 'orders') }} o
        ON op.order_id = o.order_id
    WHERE o.order_status IN ('delivered', 'shipped')
)
SELECT
    g.total_value AS gold_total_payment_value,
    s.total_value AS silver_total_payment_value,
    ABS(g.total_value - s.total_value) AS difference
FROM gold_sum g
CROSS JOIN silver_sum s
WHERE ABS(g.total_value - s.total_value) > 0.01
