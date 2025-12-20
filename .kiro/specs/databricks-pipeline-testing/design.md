# Design Document: Databricks Pipeline Testing

## Overview

Thiết kế này mô tả cách thêm Data Quality Tests và tổ chức Task Groups cho Olist Data Pipeline trên Databricks. Sử dụng Databricks Asset Bundles (DAB) để định nghĩa workflow trong `databricks.yml`.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        OLIST DATA PIPELINE DAG                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────── BRONZE GROUP ───────────────┐                             │
│  │                                             │                             │
│  │  ┌─────────────────────────────────────┐   │                             │
│  │  │         run_bronze (9 tasks)        │   │                             │
│  │  │  orders, customers, order_items,    │   │                             │
│  │  │  order_payments, products, sellers, │   │                             │
│  │  │  geolocation, order_reviews,        │   │                             │
│  │  │  product_translation                │   │                             │
│  │  └─────────────────┬───────────────────┘   │                             │
│  │                    │                        │                             │
│  │                    ▼                        │                             │
│  │  ┌─────────────────────────────────────┐   │                             │
│  │  │        test_bronze (9 tests)        │   │                             │
│  │  │  Validate: NOT NULL, row counts,    │   │                             │
│  │  │  data types for each table          │   │                             │
│  │  └─────────────────────────────────────┘   │                             │
│  └─────────────────────┬───────────────────────┘                             │
│                        │                                                     │
│                        ▼                                                     │
│  ┌─────────────── SILVER GROUP ───────────────┐                             │
│  │                                             │                             │
│  │  ┌─────────────────────────────────────┐   │                             │
│  │  │         run_silver (8 tasks)        │   │                             │
│  │  │  geolocation, customers, products,  │   │                             │
│  │  │  sellers, orders, order_items,      │   │                             │
│  │  │  order_payments, order_reviews      │   │                             │
│  │  └─────────────────┬───────────────────┘   │                             │
│  │                    │                        │                             │
│  │                    ▼                        │                             │
│  │  ┌─────────────────────────────────────┐   │                             │
│  │  │        test_silver (8 tests)        │   │                             │
│  │  │  Validate: uniqueness, data ranges, │   │                             │
│  │  │  referential integrity              │   │                             │
│  │  └─────────────────────────────────────┘   │                             │
│  └─────────────────────┬───────────────────────┘                             │
│                        │                                                     │
│                        ▼                                                     │
│  ┌─────────────── GOLD GROUP ─────────────────┐                             │
│  │                                             │                             │
│  │  ┌─────────────────────────────────────┐   │                             │
│  │  │      run_gold_dims (4 tasks)        │   │                             │
│  │  │  dim_customers, dim_products,       │   │                             │
│  │  │  dim_sellers, dim_datetimes         │   │                             │
│  │  └─────────────────┬───────────────────┘   │                             │
│  │                    │                        │                             │
│  │                    ▼                        │                             │
│  │  ┌─────────────────────────────────────┐   │                             │
│  │  │      run_gold_facts (3 tasks)       │   │                             │
│  │  │  fct_order_items, fct_order_payments│   │                             │
│  │  │  fct_order_reviews                  │   │                             │
│  │  └─────────────────────────────────────┘   │                             │
│  └─────────────────────────────────────────────┘                             │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Components and Interfaces

### 1. Test Notebooks Structure

```
src/tasks/
├── bronze/
│   └── bronze.py                    # Existing - generic loader
├── silver/
│   ├── silver_customers.py          # Existing
│   ├── silver_orders.py             # Existing
│   └── ...                          # Other silver tasks
├── tests/
│   ├── bronze/
│   │   ├── test_bronze_orders.py
│   │   ├── test_bronze_customers.py
│   │   ├── test_bronze_order_items.py
│   │   ├── test_bronze_order_payments.py
│   │   ├── test_bronze_products.py
│   │   ├── test_bronze_sellers.py
│   │   ├── test_bronze_geolocation.py
│   │   ├── test_bronze_order_reviews.py
│   │   └── test_bronze_product_translation.py
│   └── silver/
│       ├── test_silver_customers.py
│       ├── test_silver_orders.py
│       ├── test_silver_order_items.py
│       ├── test_silver_order_payments.py
│       ├── test_silver_products.py
│       ├── test_silver_sellers.py
│       ├── test_silver_geolocation.py
│       └── test_silver_order_reviews.py
└── gold/
    ├── gold_dim_customers.py
    ├── gold_dim_products.py
    ├── gold_dim_sellers.py
    ├── gold_dim_datetimes.py
    ├── gold_fct_order_items.py
    ├── gold_fct_order_payments.py
    └── gold_fct_order_reviews.py
```

### 2. Test Notebook Interface

Mỗi test notebook sẽ có cấu trúc chung:

```python
# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

def run_tests(spark, table_name):
    """Run data quality tests and raise exception if failed"""
    results = []
    
    # Test 1: Row count > 0
    # Test 2: Primary key not null
    # Test 3: Specific validations
    
    if any(not r['passed'] for r in results):
        raise Exception(f"Data quality tests failed for {table_name}")
    
    return results
```

### 3. Gold Task Interface

Mỗi Gold task sẽ chạy một dbt model cụ thể:

```python
# Databricks notebook source
# Chạy dbt model cụ thể thay vì toàn bộ project

# Parameters: model_name (e.g., "dim_customers")
```

## Data Models

### Test Results Schema

```python
test_result = {
    "table_name": str,      # e.g., "bronze.orders_raw"
    "test_name": str,       # e.g., "not_null_order_id"
    "passed": bool,
    "details": str,         # Error message if failed
    "row_count": int,
    "timestamp": datetime
}
```

### Bronze Test Validations

| Table | Primary Key | Additional Checks |
|-------|-------------|-------------------|
| orders_raw | order_id | NOT NULL |
| customers_raw | customer_id | customer_unique_id NOT NULL |
| order_items_raw | order_id + order_item_id | product_id NOT NULL |
| order_payments_raw | order_id + payment_sequential | payment_value >= 0 |
| products_raw | product_id | NOT NULL |
| sellers_raw | seller_id | NOT NULL |
| geolocation_raw | geolocation_zip_code_prefix | NOT NULL |
| order_reviews_raw | review_id | order_id NOT NULL |
| product_category_name_translation_raw | product_category_name | NOT NULL |

### Silver Test Validations

| Table | Uniqueness Check | Additional Checks |
|-------|------------------|-------------------|
| customers | customer_id | customer_state UPPERCASE |
| orders | order_id | order_status IN valid values |
| order_items | order_id + order_item_id | price >= 0, freight_value >= 0 |
| order_payments | order_id + payment_sequential | payment_value >= 0, payment_type valid |
| products | product_id | - |
| sellers | seller_id | seller_state UPPERCASE |
| geolocation | geolocation_zip_code_prefix | UNIQUE after dedupe |
| order_reviews | review_id | review_score BETWEEN 1 AND 5 |

## Databricks DAG Configuration

### Task Dependencies trong databricks.yml

```yaml
tasks:
  # ============ BRONZE GROUP ============
  # run_bronze tasks (9 tasks - chạy song song)
  - task_key: load_bronze_orders
    ...
  - task_key: load_bronze_customers
    ...
  # ... (other bronze tasks)
  
  # test_bronze tasks (9 tests - depends on respective bronze task)
  - task_key: test_bronze_orders
    depends_on:
      - task_key: load_bronze_orders
    notebook_task:
      notebook_path: src/tasks/tests/bronze/test_bronze_orders.py
      
  - task_key: test_bronze_customers
    depends_on:
      - task_key: load_bronze_customers
    notebook_task:
      notebook_path: src/tasks/tests/bronze/test_bronze_customers.py
  # ... (other bronze tests)
  
  # ============ SILVER GROUP ============
  # run_silver tasks (depends on ALL bronze tests)
  - task_key: process_silver_geolocation
    depends_on:
      - task_key: test_bronze_geolocation
    ...
    
  - task_key: process_silver_customers
    depends_on:
      - task_key: test_bronze_customers
      - task_key: process_silver_geolocation
    ...
  # ... (other silver tasks)
  
  # test_silver tasks (depends on respective silver task)
  - task_key: test_silver_customers
    depends_on:
      - task_key: process_silver_customers
    notebook_task:
      notebook_path: src/tasks/tests/silver/test_silver_customers.py
  # ... (other silver tests)
  
  # ============ GOLD GROUP ============
  # run_gold_dims (depends on ALL silver tests)
  - task_key: run_gold_dim_customers
    depends_on:
      - task_key: test_silver_customers
      - task_key: test_silver_geolocation
    dbt_task:
      commands: ["dbt run --select dim_customers"]
      
  - task_key: run_gold_dim_products
    depends_on:
      - task_key: test_silver_products
    dbt_task:
      commands: ["dbt run --select dim_products"]
  # ... (other dim tasks)
  
  # run_gold_facts (depends on dims)
  - task_key: run_gold_fct_order_items
    depends_on:
      - task_key: run_gold_dim_customers
      - task_key: run_gold_dim_products
      - task_key: run_gold_dim_sellers
    dbt_task:
      commands: ["dbt run --select fct_order_items"]
  # ... (other fact tasks)
```

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system-essentially, a formal statement about what the system should do.*

### Property 1: Bronze Test Completeness
*For any* Bronze table, the corresponding test task SHALL verify that the primary key column contains no NULL values before Silver processing begins.
**Validates: Requirements 1.1-1.9**

### Property 2: Silver Test Uniqueness
*For any* Silver table with a defined primary key, the test task SHALL verify that the primary key is unique across all rows.
**Validates: Requirements 2.1-2.8**

### Property 3: DAG Dependency Integrity
*For any* Silver task, it SHALL only execute after ALL its dependent Bronze tests have passed successfully.
**Validates: Requirements 5.2, 5.3**

### Property 4: Gold Task Isolation
*For any* Gold dbt model, it SHALL be executed as a separate task with its own dependencies and can be monitored independently.
**Validates: Requirements 3.1-3.4**

## Error Handling

1. **Test Failure**: Khi test fail, notebook sẽ raise Exception → Task fail → Pipeline stop
2. **Partial Failure**: Nếu 1 Bronze test fail, các Silver tasks phụ thuộc sẽ không chạy
3. **Logging**: Mỗi test ghi log chi tiết vào notebook output

## Testing Strategy

### Unit Tests
- Test từng validation function riêng lẻ
- Mock Spark DataFrame để test logic

### Integration Tests  
- Chạy full pipeline trên sample data
- Verify DAG dependencies hoạt động đúng

### Property-Based Tests
- Verify test notebooks raise exception khi data invalid
- Verify DAG structure matches requirements
