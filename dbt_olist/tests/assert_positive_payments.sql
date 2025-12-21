-- Property 4: Positive Payment Values
-- Validates: Requirements 2.3
-- For any row in fct_order_payments, the payment_value SHALL be strictly greater than zero.
-- This test returns rows that violate the property (0 rows = pass)

SELECT
    order_id,
    payment_sequential,
    payment_value
FROM {{ ref('fct_order_payments') }}
WHERE payment_value <= 0 OR payment_value IS NULL
