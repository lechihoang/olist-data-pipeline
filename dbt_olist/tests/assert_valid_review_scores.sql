-- Property 5: Valid Review Score Range
-- Validates: Requirements 3.1, 3.2
-- For any row in fct_order_reviews, the review_score SHALL be between 1 and 5 inclusive and not null.
-- This test returns rows that violate the property (0 rows = pass)

SELECT
    review_id,
    order_id,
    review_score
FROM {{ ref('fct_order_reviews') }}
WHERE review_score IS NULL
   OR review_score < 1
   OR review_score > 5
