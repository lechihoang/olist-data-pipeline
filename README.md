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

**Processing Pattern:**
- Auto Loader with `trigger(availableNow=True)` for incremental batch ingestion
- Schema enforcement using JSON schema definitions
- Metadata columns added: `_source_file`, `_ingest_timestamp`
- Checkpoints track processed files to avoid reprocessing

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

**Processing Pattern:**
- Incremental batch using `readStream` with `trigger(availableNow=True)`
- Delta Lake MERGE for upsert operations (insert new, update existing)
- Checkpoints track processed records

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

**Processing Pattern:**
- dbt snapshots for SCD Type 2 change tracking
- dbt incremental models for efficient updates

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
- Auto Loader tracks processed files via checkpoints
- Delta Lake MERGE enables efficient upserts
- dbt incremental models process only new/changed data

**Data Quality**
- Pandera schema validation for Bronze and Silver layers
- dbt built-in tests: unique, not_null, accepted_values, relationships
- Custom singular tests for business rules validation

**Change Data Capture**
- SCD Type 2 implemented via dbt snapshots
- Full history tracking for customer, product, seller, and order status changes
- `is_current` flag for easy current-state queries

**Data Governance**
- Unity Catalog for centralized metadata management
- Organized schemas: bronze, silver, gold, staging
- Volume-based file management with access controls

## Getting Started

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Databricks CLI configured
- Python 3.10+

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/brazilian-e-commerce-elt.git
cd brazilian-e-commerce-elt
```

2. Run the setup script in Databricks SQL Editor:
```sql
-- Execute script/setup.sql to create catalog, schemas, and volumes
```

3. Upload data files:
```bash
pip install databricks-sdk python-dotenv
python script/upload.py
```

4. Deploy and run the pipeline:
```bash
databricks bundle deploy
databricks bundle run olist_pipeline_job
```

## Notebooks

<!-- TODO: Add notebook descriptions -->
