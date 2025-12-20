# Requirements Document

## Introduction

Tài liệu này mô tả các yêu cầu cho việc thêm Data Quality Tests vào Databricks Pipeline của dự án Olist E-commerce. Pipeline hiện tại có 3 tầng (Bronze → Silver → Gold) và cần bổ sung các test tasks để đảm bảo chất lượng dữ liệu sau mỗi tầng xử lý.

Dữ liệu nguồn: [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

## Glossary

- **Pipeline**: Databricks Asset Bundle workflow xử lý dữ liệu qua các tầng
- **Bronze_Layer**: Tầng raw data, nạp dữ liệu từ CSV vào Delta tables
- **Silver_Layer**: Tầng cleaned data, làm sạch và chuẩn hóa dữ liệu
- **Gold_Layer**: Tầng analytics, tạo dimension và fact tables bằng dbt
- **Test_Task**: Databricks notebook kiểm tra chất lượng dữ liệu
- **DAG**: Directed Acyclic Graph định nghĩa thứ tự chạy các tasks
- **Data_Quality_Check**: Kiểm tra tính toàn vẹn và chính xác của dữ liệu

## Requirements

### Requirement 1: Bronze Layer Data Quality Tests

**User Story:** As a data engineer, I want to validate Bronze layer data after ingestion, so that I can ensure raw data is loaded correctly before processing.

#### Acceptance Criteria

1. WHEN Bronze ingestion completes for orders_raw, THE Test_Task SHALL verify that order_id column contains no NULL values
2. WHEN Bronze ingestion completes for customers_raw, THE Test_Task SHALL verify that customer_id and customer_unique_id columns contain no NULL values
3. WHEN Bronze ingestion completes for order_items_raw, THE Test_Task SHALL verify that order_id and product_id columns contain no NULL values
4. WHEN Bronze ingestion completes for order_payments_raw, THE Test_Task SHALL verify that order_id column contains no NULL values and payment_value >= 0
5. WHEN Bronze ingestion completes for products_raw, THE Test_Task SHALL verify that product_id column contains no NULL values
6. WHEN Bronze ingestion completes for sellers_raw, THE Test_Task SHALL verify that seller_id column contains no NULL values
7. WHEN Bronze ingestion completes for geolocation_raw, THE Test_Task SHALL verify that geolocation_zip_code_prefix column contains no NULL values
8. WHEN Bronze ingestion completes for order_reviews_raw, THE Test_Task SHALL verify that review_id and order_id columns contain no NULL values
9. WHEN Bronze ingestion completes for product_category_name_translation_raw, THE Test_Task SHALL verify that product_category_name column contains no NULL values
10. WHEN any Bronze test fails, THE Pipeline SHALL stop execution and report the failure

### Requirement 2: Silver Layer Data Quality Tests

**User Story:** As a data engineer, I want to validate Silver layer data after transformation, so that I can ensure data is cleaned and enriched correctly.

#### Acceptance Criteria

1. WHEN Silver processing completes for customers, THE Test_Task SHALL verify that customer_id is unique and customer_state is uppercase
2. WHEN Silver processing completes for orders, THE Test_Task SHALL verify that order_id is unique and order_status is valid
3. WHEN Silver processing completes for order_items, THE Test_Task SHALL verify that order_id and product_id combination exists and price >= 0
4. WHEN Silver processing completes for order_payments, THE Test_Task SHALL verify that payment_value >= 0 and payment_type is valid
5. WHEN Silver processing completes for products, THE Test_Task SHALL verify that product_id is unique
6. WHEN Silver processing completes for sellers, THE Test_Task SHALL verify that seller_id is unique and seller_state is uppercase
7. WHEN Silver processing completes for geolocation, THE Test_Task SHALL verify that geolocation_zip_code_prefix is unique after deduplication
8. WHEN Silver processing completes for order_reviews, THE Test_Task SHALL verify that review_id is unique and review_score is between 1 and 5
9. WHEN any Silver test fails, THE Pipeline SHALL stop execution and report the failure

### Requirement 3: Gold Layer Task Separation

**User Story:** As a data engineer, I want Gold layer models to run as separate tasks, so that I can monitor and debug each model independently.

#### Acceptance Criteria

1. THE Pipeline SHALL have separate tasks for dim_customers, dim_products, dim_sellers, dim_datetimes models
2. THE Pipeline SHALL have separate tasks for fct_order_items, fct_order_payments, fct_order_reviews models
3. WHEN a Gold task runs, THE Task SHALL execute only its specific dbt model
4. WHEN dimension models complete, THE fact models SHALL start execution (fact depends on dimensions)

### Requirement 4: Task Groups Organization

**User Story:** As a data engineer, I want tasks organized into logical groups, so that I can easily monitor and manage each layer of the pipeline.

#### Acceptance Criteria

1. THE Pipeline SHALL organize Bronze layer into task group containing run_bronze and test_bronze sub-groups
2. THE Pipeline SHALL organize Silver layer into task group containing run_silver and test_silver sub-groups
3. THE Pipeline SHALL organize Gold layer into task group containing run_gold_dims and run_gold_facts sub-groups
4. WHEN viewing the DAG, THE task groups SHALL be visually grouped and collapsible

### Requirement 5: DAG Structure Update

**User Story:** As a data engineer, I want the DAG to include test tasks between layers, so that data quality is validated at each stage.

#### Acceptance Criteria

1. THE DAG SHALL follow structure: Bronze Group (run → test) → Silver Group (run → test) → Gold Group (dims → facts)
2. WHEN all run_bronze tasks complete, THE test_bronze tasks SHALL run in parallel
3. WHEN all test_bronze tasks pass, THE run_silver tasks SHALL start execution
4. WHEN all run_silver tasks complete, THE test_silver tasks SHALL run in parallel
5. WHEN all test_silver tasks pass, THE run_gold_dims tasks SHALL start execution
6. WHEN all run_gold_dims tasks complete, THE run_gold_facts tasks SHALL start execution
