-- Property 2: Positive Price Values
-- Validates: Requirements 2.1
-- For any row in fct_order_items, the price value SHALL be strictly greater than zero.
-- This test returns rows that violate the property (0 rows = pass)

SELECT
    order_id,
    order_item_id,
    price
FROM {{ ref('fct_order_items') }}
WHERE price <= 0 OR price IS NULL
