# Databricks notebook source
# Test Bronze Product - Validates data quality for product_raw table
# Using Pandera for PySpark (compatible with Databricks Serverless)

import os
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

load_dotenv()


# --- SCHEMA DEFINITION ---
class BronzeProductSchema(DataFrameModel):
    """
    Pandera schema for bronze.product_raw table.
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
    df_validated = BronzeProductSchema.validate(check_obj=df)
    
    # 3. Collect errors from validation
    errors = df_validated.pandera.errors
    
    # 4. Build result
    result = {
        "success": len(errors) == 0,
        "errors": errors,
        "row_count": df.count(),
        "schema_name": "BronzeProductSchema"
    }
    
    return result


# --- ENTRYPOINT ---
# spark is already available in Databricks notebooks
CATALOG = os.getenv("CATALOG", "olist_project")
BRONZE_SCHEMA = os.getenv("BRONZE_SCHEMA", "bronze")
TABLE_NAME = "product_raw"

logger.info(f"Config loaded: catalog={CATALOG}, bronze={BRONZE_SCHEMA}")

full_table_name = f"{CATALOG}.{BRONZE_SCHEMA}.{TABLE_NAME}"

logger.info(f"--- Running Data Quality Tests for {full_table_name} ---")
logger.info(f"Using Pandera schema: BronzeProductSchema")

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
