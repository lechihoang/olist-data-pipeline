# Databricks notebook source
# Test Silver Customer - Validates data quality for silver customer table
# Using Pandera for PySpark (compatible with Databricks Serverless)

import os
import re
import logging
from dotenv import load_dotenv
import pandera.pyspark as pa

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
from pandera.pyspark import DataFrameModel, Field
import pyspark.sql.types as T
from pyspark.sql import functions as F

load_dotenv()


# --- SCHEMA DEFINITION ---
class SilverCustomerSchema(DataFrameModel):
    """
    Pandera schema for silver.customer table.
    Validates schema structure and data quality.
    """
    
    # Define columns with their types and constraints
    customer_id: T.StringType() = Field(nullable=False)  # Primary key - NOT NULL
    customer_unique_id: T.StringType() = Field(nullable=False)  # NOT NULL
    customer_zip_code_prefix: T.StringType() = Field(nullable=True)
    customer_city: T.StringType() = Field(nullable=True)
    customer_state: T.StringType() = Field(nullable=True)
    geolocation_lat: T.DoubleType() = Field(nullable=True)
    geolocation_lng: T.DoubleType() = Field(nullable=True)

    # Custom dataframe-level check for row count
    @pa.dataframe_check
    def min_row_count(cls, df) -> bool:
        """Ensure DataFrame has at least 1 row."""
        return df.count() >= 1

    # Custom check for uniqueness of customer_id
    @pa.dataframe_check
    def unique_customer_id(cls, df) -> bool:
        """Ensure customer_id is unique."""
        total_count = df.count()
        distinct_count = df.select("customer_id").distinct().count()
        return total_count == distinct_count

    # Custom check for state format (uppercase 2-letter code)
    @pa.dataframe_check
    def valid_state_format(cls, df) -> bool:
        """Ensure customer_state matches ^[A-Z]{2}$ pattern."""
        pattern = r"^[A-Z]{2}$"
        invalid_count = df.filter(
            (F.col("customer_state").isNotNull()) & 
            (~F.col("customer_state").rlike(pattern))
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
    df_validated = SilverCustomerSchema.validate(check_obj=df)
    
    # 3. Collect errors from validation
    errors = df_validated.pandera.errors
    
    # 4. Build result
    result = {
        "success": len(errors) == 0,
        "errors": errors,
        "row_count": df.count(),
        "schema_name": "SilverCustomerSchema"
    }
    
    return result


# --- ENTRYPOINT ---
# spark is already available in Databricks notebooks
CATALOG = os.getenv("CATALOG", "olist_project")
SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "silver")
TABLE_NAME = "customer"

logger.info(f"Config loaded: catalog={CATALOG}, silver={SILVER_SCHEMA}")

full_table_name = f"{CATALOG}.{SILVER_SCHEMA}.{TABLE_NAME}"

logger.info(f"--- Running Data Quality Tests for {full_table_name} ---")
logger.info(f"Using Pandera schema: SilverCustomerSchema")

result = run_tests(spark, full_table_name)

# Log results
if result["success"]:
    logger.info(f"--- All tests PASSED for {full_table_name} ---")
    logger.info(f"  Row count: {result['row_count']}")
else:
    logger.error(f"--- Some tests FAILED for {full_table_name} ---")
    logger.error(f"  Row count: {result['row_count']}")
    logger.error(f"  Errors: {result['errors']}")
    raise Exception(f"Data quality tests FAILED for {full_table_name}: {result['errors']}")
