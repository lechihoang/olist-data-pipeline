-- Property 6: Delivery Date Consistency
-- Validates: Requirements 4.1
-- For any row in fct_order_items where order_delivered_customer_date is not null,
-- the order_delivered_customer_date SHALL be greater than or equal to order_date.
-- This test returns rows that violate the property (0 rows = pass)

SELECT
    order_id,
    order_item_id,
    order_date,
    order_delivered_customer_date
FROM {{ ref('fct_order_items') }}
WHERE order_delivered_customer_date IS NOT NULL
  AND order_delivered_customer_date < order_date
