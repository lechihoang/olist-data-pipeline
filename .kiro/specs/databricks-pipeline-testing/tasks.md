# Implementation Plan: Databricks Pipeline Testing

## Overview

Triển khai Data Quality Tests và Task Groups cho Olist Data Pipeline. Tạo test notebooks cho Bronze/Silver layers và tách Gold layer thành các tasks riêng.

## Tasks

- [x] 1. Tạo cấu trúc thư mục tests và gold
  - Tạo `src/tasks/tests/bronze/` và `src/tasks/tests/silver/`
  - Tạo `src/tasks/gold/`
  - _Requirements: 1.1-1.9, 2.1-2.8, 3.1-3.2_

- [x] 2. Tạo Bronze Test Notebooks
  - [x] 2.1 Tạo test_bronze_orders.py
    - Verify order_id NOT NULL, row count > 0
    - _Requirements: 1.1_
  - [x] 2.2 Tạo test_bronze_customers.py
    - Verify customer_id, customer_unique_id NOT NULL
    - _Requirements: 1.2_
  - [x] 2.3 Tạo test_bronze_order_items.py
    - Verify order_id, product_id NOT NULL
    - _Requirements: 1.3_
  - [x] 2.4 Tạo test_bronze_order_payments.py
    - Verify order_id NOT NULL, payment_value >= 0
    - _Requirements: 1.4_
  - [x] 2.5 Tạo test_bronze_products.py
    - Verify product_id NOT NULL
    - _Requirements: 1.5_
  - [x] 2.6 Tạo test_bronze_sellers.py
    - Verify seller_id NOT NULL
    - _Requirements: 1.6_
  - [x] 2.7 Tạo test_bronze_geolocation.py
    - Verify geolocation_zip_code_prefix NOT NULL
    - _Requirements: 1.7_
  - [x] 2.8 Tạo test_bronze_order_reviews.py
    - Verify review_id, order_id NOT NULL
    - _Requirements: 1.8_
  - [x] 2.9 Tạo test_bronze_product_translation.py
    - Verify product_category_name NOT NULL
    - _Requirements: 1.9_

- [x] 3. Checkpoint - Verify Bronze Tests
  - Ensure all Bronze test notebooks created correctly

- [x] 4. Tạo Silver Test Notebooks
  - [x] 4.1 Tạo test_silver_customers.py
    - Verify customer_id UNIQUE, customer_state UPPERCASE
    - _Requirements: 2.1_
  - [x] 4.2 Tạo test_silver_orders.py
    - Verify order_id UNIQUE, order_status valid
    - _Requirements: 2.2_
  - [x] 4.3 Tạo test_silver_order_items.py
    - Verify order_id + order_item_id, price >= 0
    - _Requirements: 2.3_
  - [x] 4.4 Tạo test_silver_order_payments.py
    - Verify payment_value >= 0, payment_type valid
    - _Requirements: 2.4_
  - [x] 4.5 Tạo test_silver_products.py
    - Verify product_id UNIQUE
    - _Requirements: 2.5_
  - [x] 4.6 Tạo test_silver_sellers.py
    - Verify seller_id UNIQUE, seller_state UPPERCASE
    - _Requirements: 2.6_
  - [x] 4.7 Tạo test_silver_geolocation.py
    - Verify geolocation_zip_code_prefix UNIQUE
    - _Requirements: 2.7_
  - [x] 4.8 Tạo test_silver_order_reviews.py
    - Verify review_id UNIQUE, review_score 1-5
    - _Requirements: 2.8_

- [x] 5. Checkpoint - Verify Silver Tests
  - Ensure all Silver test notebooks created correctly

- [x] 6. Cập nhật databricks.yml với DAG mới
  - [ ] 8.1 Thêm Bronze test tasks với dependencies
    - Mỗi test depends on tương ứng bronze load task
    - _Requirements: 5.2_
  - [ ] 8.2 Cập nhật Silver tasks dependencies
    - Silver tasks depends on Bronze tests thay vì Bronze loads
    - _Requirements: 5.3_
  - [ ] 8.3 Thêm Silver test tasks với dependencies
    - Mỗi test depends on tương ứng silver process task
    - _Requirements: 5.4_
  - [ ] 8.4 Thêm Gold dimension tasks với dependencies
    - Dims depends on Silver tests
    - _Requirements: 5.5_
  - [ ] 8.5 Thêm Gold fact tasks với dependencies
    - Facts depends on Dims
    - _Requirements: 5.6_

- [ ] 7. Final Checkpoint
  - Review toàn bộ databricks.yml
  - Verify DAG structure đúng theo requirements

## Notes

- Tất cả test notebooks sử dụng pattern: raise Exception nếu test fail
- Gold tasks sử dụng dbt_task thay vì notebook_task
- DAG flow: Bronze Load → Bronze Test → Silver Process → Silver Test → Gold Dims → Gold Facts
