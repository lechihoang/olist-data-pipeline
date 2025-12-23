{{
  config(
    materialized='table'
  )
}}

{#
  Customer Segments Dimension Table
  
  Source: ML K-Means clustering output
  Purpose: Map cluster IDs to business-friendly segment names
  
  Segment Mapping (adjust based on EDA notebook analysis):
  - Champions: Low recency, High frequency, High monetary
  - Loyal Customers: Low recency, Medium frequency, Medium monetary
  - Potential Loyalists: Medium recency, Low frequency, Low monetary
  - At Risk: High recency, Low frequency, Low monetary
#}

WITH customer_segments AS (
    SELECT 
        customer_unique_id,
        recency_days,
        frequency,
        monetary,
        cluster_id,
        model_version,
        processed_timestamp
    FROM {{ source('olist_ml', 'customer_segments') }}
),

segment_labels AS (
    SELECT
        customer_unique_id,
        recency_days,
        frequency,
        monetary,
        cluster_id,
        -- Mapping cluster_id to segment_name
        -- NOTE: Adjust this mapping based on actual cluster profiles from EDA notebook
        CASE cluster_id
            WHEN 0 THEN 'Champions'
            WHEN 1 THEN 'Loyal Customers'
            WHEN 2 THEN 'Potential Loyalists'
            WHEN 3 THEN 'At Risk'
            ELSE 'Unknown'
        END AS segment_name,
        -- Segment description for business context
        CASE cluster_id
            WHEN 0 THEN 'Best customers - Recent, frequent buyers with high spending'
            WHEN 1 THEN 'Loyal customers - Regular buyers with moderate spending'
            WHEN 2 THEN 'New or occasional buyers - Potential to become loyal'
            WHEN 3 THEN 'Inactive customers - Haven''t purchased recently'
            ELSE 'Unclassified customers'
        END AS segment_description,
        model_version,
        processed_timestamp
    FROM customer_segments
)

SELECT * FROM segment_labels
