# Databricks notebook source
# Test Bronze Products - Validates data quality for products_raw table
# Using Pandera for PySpark (compatible with Databricks Serverless)

import os
from dotenv import load_dotenv
import pandera.pyspark as pa
from pandera.pyspark import DataFrameModel, Field
import pyspark.sql.types as T

load_dotenv()


# --- SCHEMA DEFINITION ---
class BronzeProductsSchema(DataFrameModel):
    """
    Pandera schema for bronze.products_raw table.
    Validates schema structure and data quality.
    """
    
    # Define columns with their types and constraints
    product_id: T.StringType() = Field(nullable=False)  # Primary key - NOT NULL
    product_category_name: T.StringType() = Field(nullable=True)
    product_name_lenght: T.IntegerType() = Field(nullable=True)  # Note: typo in original data
    product_description_lenght: T.IntegerType() = Field(nullable=True)  # Note: typo in original data
    product_photos_qty: T.IntegerType() = Field(nullable=True)
    product_weight_g: T.DoubleType() = Field(nullable=True)
    product_length_cm: T.DoubleType() = Field(nullable=True)
    product_height_cm: T.DoubleType() = Field(nullable=True)
    product_width_cm: T.DoubleType() = Field(nullable=True)

    # Custom dataframe-level check for row count
    @pa.dataframe_check
    def min_row_count(cls, df) -> bool:
        """Ensure DataFrame has at least 1 row."""
        return df.count() >= 1


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
    df_validated = BronzeProductsSchema.validate(check_obj=df)
    
    # 3. Collect errors from validation
    errors = df_validated.pandera.errors
    
    # 4. Build result
    result = {
        "success": len(errors) == 0,
        "errors": errors,
        "row_count": df.count(),
        "schema_name": "BronzeProductsSchema"
    }
    
    return result


# --- ENTRYPOINT ---
# spark is already available in Databricks notebooks
CATALOG = os.getenv("CATALOG", "olist_project")
BRONZE_SCHEMA = os.getenv("BRONZE_SCHEMA", "bronze")
TABLE_NAME = "products_raw"

print(f"Config loaded: catalog={CATALOG}, bronze={BRONZE_SCHEMA}")

full_table_name = f"{CATALOG}.{BRONZE_SCHEMA}.{TABLE_NAME}"

print(f"--- Running Data Quality Tests for {full_table_name} ---")
print(f"Using Pandera schema: BronzeProductsSchema")

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
