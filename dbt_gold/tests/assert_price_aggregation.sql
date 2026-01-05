WITH gold_sum AS (
    SELECT COALESCE(SUM(price), 0) AS total_price
    FROM {{ ref('fct_order_item') }}
),
silver_sum AS (
    SELECT COALESCE(SUM(oi.price), 0) AS total_price
    FROM {{ source('olist_silver', 'order_item') }} oi
    INNER JOIN {{ source('olist_silver', 'order') }} o
        ON oi.order_id = o.order_id
)
SELECT
    g.total_price AS gold_total_price,
    s.total_price AS silver_total_price,
    ABS(g.total_price - s.total_price) AS difference
FROM gold_sum g
CROSS JOIN silver_sum s
WHERE ABS(g.total_price - s.total_price) > 0.01
