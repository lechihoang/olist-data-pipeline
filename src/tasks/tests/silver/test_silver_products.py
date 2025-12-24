# Databricks notebook source
# Test Silver Products - Validates data quality for silver products table
# Using Pandera for PySpark (compatible with Databricks Serverless)

import os
from dotenv import load_dotenv
import pandera.pyspark as pa
from pandera.pyspark import DataFrameModel, Field
import pyspark.sql.types as T
from pyspark.sql import functions as F

load_dotenv()


# --- SCHEMA DEFINITION ---
class SilverProductsSchema(DataFrameModel):
    """
    Pandera schema for silver.products table.
    Validates schema structure and data quality.
    """
    
    # Define columns with their types and constraints
    product_id: T.StringType() = Field(nullable=False)  # Primary key - NOT NULL
    product_category_name: T.StringType() = Field(nullable=True)
    product_name_length: T.IntegerType() = Field(nullable=True)  # Fixed typo from raw
    product_description_length: T.IntegerType() = Field(nullable=True)  # Fixed typo from raw
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

    # Custom check for uniqueness of product_id
    @pa.dataframe_check
    def unique_product_id(cls, df) -> bool:
        """Ensure product_id is unique."""
        total_count = df.count()
        distinct_count = df.select("product_id").distinct().count()
        return total_count == distinct_count

    # Custom check for product_weight_g >= 0 (allow NULLs)
    @pa.dataframe_check
    def valid_weight(cls, df) -> bool:
        """Ensure product_weight_g >= 0 when not null."""
        invalid_count = df.filter(
            (F.col("product_weight_g").isNotNull()) & (F.col("product_weight_g") < 0)
        ).count()
        return invalid_count == 0

    # Custom check for product_length_cm >= 0 (allow NULLs)
    @pa.dataframe_check
    def valid_length(cls, df) -> bool:
        """Ensure product_length_cm >= 0 when not null."""
        invalid_count = df.filter(
            (F.col("product_length_cm").isNotNull()) & (F.col("product_length_cm") < 0)
        ).count()
        return invalid_count == 0

    # Custom check for product_height_cm >= 0 (allow NULLs)
    @pa.dataframe_check
    def valid_height(cls, df) -> bool:
        """Ensure product_height_cm >= 0 when not null."""
        invalid_count = df.filter(
            (F.col("product_height_cm").isNotNull()) & (F.col("product_height_cm") < 0)
        ).count()
        return invalid_count == 0

    # Custom check for product_width_cm >= 0 (allow NULLs)
    @pa.dataframe_check
    def valid_width(cls, df) -> bool:
        """Ensure product_width_cm >= 0 when not null."""
        invalid_count = df.filter(
            (F.col("product_width_cm").isNotNull()) & (F.col("product_width_cm") < 0)
        ).count()
        return invalid_count == 0


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
    df_validated = SilverProductsSchema.validate(check_obj=df)
    
    # 3. Collect errors from validation
    errors = df_validated.pandera.errors
    
    # 4. Build result
    result = {
        "success": len(errors) == 0,
        "errors": errors,
        "row_count": df.count(),
        "schema_name": "SilverProductsSchema"
    }
    
    return result


# --- ENTRYPOINT ---
# spark is already available in Databricks notebooks
CATALOG = os.getenv("CATALOG", "olist_project")
SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "silver")
TABLE_NAME = "products"

print(f"Config loaded: catalog={CATALOG}, silver={SILVER_SCHEMA}")

full_table_name = f"{CATALOG}.{SILVER_SCHEMA}.{TABLE_NAME}"

print(f"--- Running Data Quality Tests for {full_table_name} ---")
print(f"Using Pandera schema: SilverProductsSchema")

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
