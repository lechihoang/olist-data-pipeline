{{
  config(
    materialized='incremental',
    unique_key=['order_id', 'payment_sequential']
  )
}}

/*
    fct_order_payments - Order Payments Fact Table
    
    Grain: One row per payment transaction
    
    Foreign Keys:
    - customer_unique_id -> dim_customers (SCD2, use is_current=TRUE for current state)
    
    For point-in-time analysis, join with dimension tables using:
    WHERE order_purchase_timestamp BETWEEN dim.valid_from AND COALESCE(dim.valid_to, '9999-12-31')
*/

WITH payments AS (
    SELECT * FROM {{ source('olist_silver', 'order_payments') }}
),

orders AS (
    SELECT * FROM {{ source('olist_silver', 'orders') }}
    WHERE 
        order_status IN ('delivered', 'shipped')
        {% if is_incremental() %}
          AND order_purchase_timestamp > (
            SELECT COALESCE(DATE_SUB(MAX(order_purchase_timestamp), INTERVAL 3 DAY), '1900-01-01')
            FROM {{ this }}
          )
        {% endif %}
),

customers AS (
    SELECT 
        customer_id,
        customer_unique_id
    FROM {{ source('olist_silver', 'customers') }}
)

SELECT
    -- Keys
    p.order_id,
    p.payment_sequential,
    c.customer_unique_id,  -- FK to dim_customers (SCD2)
    
    -- Timestamps
    o.order_purchase_timestamp,
    
    -- Attributes
    p.payment_type,
    p.payment_installments,
    
    -- Measures
    p.payment_value
    
FROM payments AS p
INNER JOIN orders AS o
    ON p.order_id = o.order_id
INNER JOIN customers AS c
    ON o.customer_id = c.customer_id
