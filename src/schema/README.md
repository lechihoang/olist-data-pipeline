# Schema Registry

Central schema definitions for the Olist E-commerce Data Pipeline.

## Overview

This directory contains YAML schema definitions for all tables in the Bronze and Silver layers. These schemas serve as the **Single Source of Truth** for:

- **Bronze Layer**: Schema enforcement during Auto Loader ingestion
- **Silver Layer**: Documentation of cleaned/enriched data structures

## Directory Structure

```
src/schema/
├── README.md                 # This file
├── registry.yaml             # Central registry index
├── bronze/                   # Bronze layer schemas (used by Auto Loader)
│   ├── customer.yaml
│   ├── order.yaml
│   ├── order_item.yaml
│   ├── order_payment.yaml
│   ├── order_review.yaml
│   ├── product.yaml
│   ├── seller.yaml
│   ├── geolocation.yaml
│   └── product_category_translation.yaml
└── silver/                   # Silver layer schemas (documentation)
    ├── customer.yaml
    ├── order.yaml
    ├── order_item.yaml
    ├── order_payment.yaml
    ├── order_review.yaml
    ├── product.yaml
    ├── seller.yaml
    └── geolocation.yaml
```

## Schema Format

Each YAML file follows this structure:

```yaml
name: <entity_name>
layer: bronze|silver
version: "1.0.0"
description: |
  Description of the table and its purpose.

source:
  file: <source_file.csv>       # Bronze only
  format: csv
  table: <source_table>         # Silver only

columns:
  - name: <column_name>
    type: string|integer|double|timestamp|boolean
    nullable: true|false
    description: Column description
    tags: [primary_key, pii, business_key]  # Optional

metadata:
  owner: data-engineering
  created_at: "YYYY-MM-DD"
  updated_at: "YYYY-MM-DD"
```

## Supported Data Types

| Type | Spark Type | Description |
|------|------------|-------------|
| `string` | StringType | Text data |
| `integer` | IntegerType | Whole numbers |
| `double` | DoubleType | Decimal numbers |
| `timestamp` | TimestampType | Date and time |
| `boolean` | BooleanType | True/False |

## Usage

### Bronze Layer (Auto Loader)

```python
from schema.loader import load_spark_schema

schema = load_spark_schema("customer", layer="bronze")

spark.readStream
    .format("cloudFiles")
    .schema(schema)
    .load(path)
```

### Silver Layer (Documentation Only)

Silver schemas are for documentation purposes. They describe the expected output schema after transformations but are not enforced in code.

## Schema Evolution

When modifying schemas:

1. Update the YAML file
2. Increment the `version` field
3. Update `updated_at` in metadata
4. Test the pipeline with new schema

## Related Documentation

- [dbt Gold Layer Schemas](../../dbt_gold/models/gold/schema.yml)
- [Pandera Test Schemas](../test/)
