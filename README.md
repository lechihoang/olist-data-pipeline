# Brazilian E-Commerce ELT Pipeline

An end-to-end ELT data pipeline built on Databricks, implementing the Medallion Architecture (Bronze-Silver-Gold) with Delta Lake. This project processes the Brazilian E-Commerce dataset from Olist, transforming raw CSV files into analytics-ready dimensional models.

## Data Source

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

This dataset contains approximately 100,000 orders from 2016-2018, including:
- Order and customer information
- Product catalog and seller data
- Payment and review details
- Geolocation coordinates

## Architecture

<!-- TODO: Add architecture diagram -->
![Architecture Diagram](img/architecture.png)

```
CSV Files --> Volume --> Bronze --> Silver --> Gold --> BI Tools
             (landing)   (raw)    (cleaned)  (dimensional)
```

## Tech Stack

| Category | Technology |
|----------|------------|
| Platform | Databricks, Unity Catalog |
| Storage | Delta Lake |
| Processing | PySpark |
| Transformation | dbt-databricks |
| Orchestration | Databricks Asset Bundles (DAB) |
| Data Quality | Pandera, dbt tests |
| Language | Python, SQL |

## Data Pipeline Details

### Bronze Layer - Raw Data Ingestion

The Bronze layer ingests raw CSV files into Delta tables with minimal transformation, preserving the original data.

**What it does:**
- Loads CSV files from landing zone into Delta tables (incremental or full load)
- Preserves original data as-is, only adds tracking metadata (source file, ingestion time)
- Enforces schema from YAML definitions to catch data type issues early
- Tracks processed files to avoid duplicate ingestion

**Tables:**

| Table | Source File | Load Pattern |
|-------|-------------|--------------|
| order_raw | order.csv | Incremental (Auto Loader) |
| customer_raw | customer.csv | Incremental (Auto Loader) |
| order_item_raw | order_item.csv | Incremental (Auto Loader) |
| order_payment_raw | order_payment.csv | Incremental (Auto Loader) |
| order_review_raw | order_review.csv | Incremental (Auto Loader) |
| product_raw | product.csv | Incremental (Auto Loader) |
| seller_raw | seller.csv | Incremental (Auto Loader) |
| geolocation_raw | geolocation.csv | Full load (static data) |
| product_category_name_translation_raw | translation.csv | Full load (static data) |

### Silver Layer - Cleaned and Enriched Data

The Silver layer applies data quality rules, standardization, and enrichment through lookups.

**What it does:**
- Cleans: removes duplicates, standardizes text (uppercase city/state), fixes typos
- Enriches: joins with lookup tables (geolocation coordinates, category translations)
- Validates: enforces business rules (price >= 0, score 1-5, valid date sequences)
- Processes incrementally with upsert logic (insert new records, update existing ones)

**Tables:**

| Table | Transformations |
|-------|-----------------|
| geolocation | Deduplicate by zip_code_prefix, average lat/lng coordinates |
| customer | JOIN with geolocation for lat/lng, uppercase city/state |
| product | JOIN with translation table for English category names, fix column typos (lenght to length) |
| seller | Uppercase city/state for consistency |
| order | Validate timestamps, filter invalid date sequences |
| order_item | Validate price >= 0, freight_value >= 0 |
| order_payment | Validate payment_value >= 0, payment_type not empty |
| order_review | Parse timestamps, validate review_score 1-5 |

### Gold Layer - Dimensional Models

The Gold layer implements a Star Schema optimized for analytics and BI tools.

**What it does:**
- Builds dimension tables with SCD Type 2 for historical tracking
- Builds fact tables for transactional data (orders, payments, reviews)
- Pre-computes metrics for BI performance optimization

**Dimension Tables (SCD Type 2):**

| Table | Description |
|-------|-------------|
| dim_customer | Customer dimension with geolocation coordinates. Tracks changes in address (zip_code, city, state) over time. |
| dim_product | Product catalog with English category names translated from Portuguese. Tracks changes in category. Includes physical attributes (weight, dimensions). |
| dim_seller | Seller dimension with geolocation coordinates. Tracks changes in seller address over time. |
| dim_order | Order header with full status history tracking (created, approved, shipped, delivered). Includes pre-computed delivery metrics for BI performance. |

SCD Type 2 columns: `valid_from`, `valid_to`, `is_current`

**Fact Tables:**

| Table | Description |
|-------|-------------|
| fct_order_item | Order line items containing price, freight_value, and shipping_limit_date. Grain: one row per item. |
| fct_order_payment | Payment transactions with payment_type, installments, and payment_value. Grain: one row per payment. |
| fct_order_review | Customer reviews with review_score (1-5), timestamps, and comment text. Grain: one row per review. |

### Use Cases

The Gold layer data serves two main purposes:

| Use Case | Description |
|----------|-------------|
| **Business Intelligence** | Powers dashboards in Power BI for sales analytics, customer insights, and operational metrics |
| **Machine Learning** | Serves as feature source for ML models (e.g., Customer Segmentation in `notebook/customer_segmentation.ipynb`) |

## Data Model

<!-- TODO: Add Star Schema ERD diagram -->
![Data Model](img/data_model.png)

## Pipeline DAG

<!-- TODO: Add Databricks DAG screenshot -->
![Pipeline DAG](img/pipeline_dag.png)

## Project Structure

```
brazilian-e-commerce-elt/
├── src/
│   ├── schema/                 # JSON schema definitions (9 files)
│   ├── task/
│   │   ├── bronze/             # Bronze ingestion (2 notebooks)
│   │   └── silver/             # Silver transformation (8 notebooks)
│   └── test/
│       ├── bronze/             # Pandera tests (9 files)
│       └── silver/             # Pandera tests (8 files)
├── dbt_gold/
│   ├── models/gold/            # Dimension and fact models (7 models)
│   ├── snapshots/              # SCD Type 2 snapshots (4 snapshots)
│   └── tests/                  # Singular tests (9 tests)
├── script/
│   ├── setup.sql               # Unity Catalog setup script
│   └── upload.py               # Data upload utility
├── notebook/                   # EDA and ML notebooks
├── databricks.yml              # Pipeline definition (DAB)
└── requirements.txt
```

## Key Features

**Incremental Processing**
- Tracks processed files to avoid reprocessing
- Upserts new/changed records efficiently
- Processes only new data in each pipeline run

**Data Quality**
- Schema validation at Bronze and Silver layers
- Built-in tests for uniqueness, null checks, and valid values
- Custom business rules validation

**Change Data Capture**
- SCD Type 2 for full history tracking
- Tracks changes in customer, product, seller, and order status over time
- Easy current-state queries with is_current flag

**Data Governance**
- Centralized metadata management with Unity Catalog
- Organized schemas: bronze, silver, gold, staging
- Volume-based file management with access controls

## Getting Started

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Databricks CLI installed and configured
- Python 3.10+

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/brazilian-e-commerce-elt.git
cd brazilian-e-commerce-elt
```

2. Configure Databricks CLI:
```bash
# Install Databricks CLI
pip install databricks-cli

# Configure authentication
databricks configure --token
```
When prompted:
- **Databricks Host**: Copy from your workspace URL (e.g., `https://dbc-xxxxx.cloud.databricks.com`)
- **Token**: Generate at User Settings → Developer → Access Tokens → Generate New Token

3. Create environment file:
```bash
cp .env.example .env
# Edit .env if you want to change default catalog/schema names
```

4. Run the setup script in Databricks SQL Editor to create catalog, schemas, and volumes:
```sql
CREATE CATALOG IF NOT EXISTS olist_project;
USE CATALOG olist_project;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS staging;

CREATE VOLUME IF NOT EXISTS staging.lakehouse;
CREATE VOLUME IF NOT EXISTS staging.checkpoints;
```

5. Upload data files:
```bash
pip install databricks-sdk python-dotenv
python script/upload.py
```

6. Deploy and run the pipeline:
```bash
databricks bundle deploy
databricks bundle run olist_pipeline_job
```

## Notebooks

<!-- TODO: Add notebook descriptions -->
