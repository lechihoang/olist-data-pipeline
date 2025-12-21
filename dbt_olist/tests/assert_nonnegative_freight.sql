-- Property 3: Non-negative Freight Values
-- Validates: Requirements 2.2
-- For any row in fct_order_items, the freight_value SHALL be greater than or equal to zero.
-- This test returns rows that violate the property (0 rows = pass)

SELECT
    order_id,
    order_item_id,
    freight_value
FROM {{ ref('fct_order_items') }}
WHERE freight_value < 0 OR freight_value IS NULL
