WITH gold_sum AS (
    SELECT COALESCE(SUM(payment_value), 0) AS total_value
    FROM {{ ref('fct_order_payment') }}
),
silver_sum AS (
    SELECT COALESCE(SUM(op.payment_value), 0) AS total_value
    FROM {{ source('olist_silver', 'order_payment') }} op
    INNER JOIN {{ source('olist_silver', 'order') }} o
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
