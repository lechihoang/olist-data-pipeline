# Databricks notebook source
# Test Bronze Product Translation - Validates data quality for product_category_name_translation_raw table
# Using Pandera for PySpark (compatible with Databricks Serverless)

import os
from dotenv import load_dotenv
import pandera.pyspark as pa
from pandera.pyspark import DataFrameModel, Field
import pyspark.sql.types as T

load_dotenv()


# --- SCHEMA DEFINITION ---
class BronzeProductTranslationSchema(DataFrameModel):
    """
    Pandera schema for bronze.product_category_name_translation_raw table.
    Validates schema structure and data quality.
    """
    
    # Define columns with their types and constraints
    product_category_name: T.StringType() = Field(nullable=False)  # Primary key - NOT NULL
    product_category_name_english: T.StringType() = Field(nullable=True)

    # Custom dataframe-level check for row count
    @pa.dataframe_check
    def min_row_count(cls, df) -> bool:
        """Ensure DataFrame has at least 1 row."""
        return df.count() >= 1

    # Custom check for uniqueness of product_category_name
    @pa.dataframe_check
    def unique_category_name(cls, df) -> bool:
        """Ensure product_category_name is unique."""
        total_count = df.count()
        distinct_count = df.select("product_category_name").distinct().count()
        return total_count == distinct_count


def run_tests(spark, table_name: str) -> dict:
    """
    Run data quality tests using Pandera.
    
    Args:
        spark: SparkSession
        table_name: Full table name (catalog.schema.table)
    
    Returns:
        dict with 'success' boolean and 'errors' details
    """
    # 1. Read table
    df = spark.table(table_name)
    
    # 2. Validate with Pandera schema
    df_validated = BronzeProductTranslationSchema.validate(check_obj=df)
    
    # 3. Collect errors from validation
    errors = df_validated.pandera.errors
    
    # 4. Build result
    result = {
        "success": len(errors) == 0,
        "errors": errors,
        "row_count": df.count(),
        "schema_name": "BronzeProductTranslationSchema"
    }
    
    return result


# --- ENTRYPOINT ---
# spark is already available in Databricks notebooks
CATALOG = os.getenv("CATALOG", "olist_project")
BRONZE_SCHEMA = os.getenv("BRONZE_SCHEMA", "bronze")
TABLE_NAME = "product_category_name_translation_raw"

print(f"Config loaded: catalog={CATALOG}, bronze={BRONZE_SCHEMA}")

full_table_name = f"{CATALOG}.{BRONZE_SCHEMA}.{TABLE_NAME}"

print(f"--- Running Data Quality Tests for {full_table_name} ---")
print(f"Using Pandera schema: BronzeProductTranslationSchema")

result = run_tests(spark, full_table_name)

# Print results
if result["success"]:
    print(f"--- All tests PASSED for {full_table_name} ---")
    print(f"  Row count: {result['row_count']}")
else:
    print(f"--- Some tests FAILED for {full_table_name} ---")
    print(f"  Row count: {result['row_count']}")
    print(f"  Errors: {result['errors']}")
    raise Exception(f"Data quality tests FAILED for {full_table_name}: {result['errors']}")
