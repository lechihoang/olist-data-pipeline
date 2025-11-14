WITH all_dates AS (
    SELECT DISTINCT
        CAST(order_purchase_timestamp AS DATE) AS order_date
    FROM {{ source('olist_silver', 'orders') }}
    WHERE order_purchase_timestamp IS NOT NULL
)
SELECT
    order_date AS date_id,
    order_date,
    YEAR(order_date) AS year,
    MONTH(order_date) AS month,
    DAY(order_date) AS day,
    QUARTER(order_date) AS quarter,
    DAYOFWEEK(order_date) AS day_of_week,
    DAYNAME(order_date) AS day_name
FROM all_dates