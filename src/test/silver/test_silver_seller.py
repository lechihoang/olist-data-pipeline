# Databricks notebook source
# Test Silver Seller - Validates data quality for silver seller table
# Using Pandera for PySpark (compatible with Databricks Serverless)

import os
from dotenv import load_dotenv
import pandera.pyspark as pa
from pandera.pyspark import DataFrameModel, Field
import pyspark.sql.types as T
from pyspark.sql import functions as F

load_dotenv()


# --- SCHEMA DEFINITION ---
class SilverSellerSchema(DataFrameModel):
    """
    Pandera schema for silver.seller table.
    Validates schema structure and data quality.
    """
    
    # Define columns with their types and constraints
    seller_id: T.StringType() = Field(nullable=False)  # Primary key - NOT NULL
    seller_zip_code_prefix: T.StringType() = Field(nullable=True)
    seller_city: T.StringType() = Field(nullable=True)
    seller_state: T.StringType() = Field(nullable=True)

    # Custom dataframe-level check for row count
    @pa.dataframe_check
    def min_row_count(cls, df) -> bool:
        """Ensure DataFrame has at least 1 row."""
        return df.count() >= 1

    # Custom check for uniqueness of seller_id
    @pa.dataframe_check
    def unique_seller_id(cls, df) -> bool:
        """Ensure seller_id is unique."""
        total_count = df.count()
        distinct_count = df.select("seller_id").distinct().count()
        return total_count == distinct_count

    # Custom check for state format (uppercase 2-letter code)
    @pa.dataframe_check
    def valid_state_format(cls, df) -> bool:
        """Ensure seller_state matches ^[A-Z]{2}$ pattern."""
        pattern = r"^[A-Z]{2}$"
        invalid_count = df.filter(
            (F.col("seller_state").isNotNull()) & 
            (~F.col("seller_state").rlike(pattern))
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
    df_validated = SilverSellerSchema.validate(check_obj=df)
    
    # 3. Collect errors from validation
    errors = df_validated.pandera.errors
    
    # 4. Build result
    result = {
        "success": len(errors) == 0,
        "errors": errors,
        "row_count": df.count(),
        "schema_name": "SilverSellerSchema"
    }
    
    return result


# --- ENTRYPOINT ---
# spark is already available in Databricks notebooks
CATALOG = os.getenv("CATALOG", "olist_project")
SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "silver")
TABLE_NAME = "seller"

print(f"Config loaded: catalog={CATALOG}, silver={SILVER_SCHEMA}")

full_table_name = f"{CATALOG}.{SILVER_SCHEMA}.{TABLE_NAME}"

print(f"--- Running Data Quality Tests for {full_table_name} ---")
print(f"Using Pandera schema: SilverSellerSchema")

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
