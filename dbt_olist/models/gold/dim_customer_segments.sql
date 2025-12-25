{{
    config(
        materialized='table'
    )
}}

/*
    Customer Segments Dimension Table
    
    Source: ML pipeline output (K-Means clustering on RFM features)
    
    Segments:
      - Cluster 0: New/Low Spenders (38%) - Low recency, low monetary
      - Cluster 1: Big Spenders (30%) - Medium recency, high monetary
      - Cluster 2: At Risk (29%) - High recency, medium monetary  
      - Cluster 3: Loyal Customers (3%) - Medium recency, high frequency, high monetary
    
    RFM Features:
      - Recency: Days since last purchase
      - Frequency: Number of unique orders
      - Monetary: Total spending (BRL)
*/

SELECT
    customer_unique_id,
    recency_days,
    frequency,
    CAST(monetary AS DECIMAL(10,2)) AS monetary,
    cluster_id,
    segment_name,
    processed_timestamp
FROM {{ source('ml', 'customer_segments') }}
