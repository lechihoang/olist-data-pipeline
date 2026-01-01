WITH gold_count AS (
    SELECT COUNT(*) AS cnt
    FROM {{ ref('fct_order_item') }}
),
silver_count AS (
    SELECT COUNT(*) AS cnt
    FROM {{ source('olist_silver', 'order_item') }} oi
    INNER JOIN {{ source('olist_silver', 'order') }} o
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
