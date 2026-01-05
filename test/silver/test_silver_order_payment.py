# Databricks notebook source
# Test Silver Order Payment - Validates data quality for silver order_payment table
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
from pyspark.sql import functions as F

load_dotenv()


# --- SCHEMA DEFINITION ---
class SilverOrderPaymentSchema(DataFrameModel):
    """
    Pandera schema for silver.order_payment table.
    Validates schema structure and data quality.
    """
    
    # Define columns with their types and constraints
    order_id: T.StringType() = Field(nullable=False)  # Composite key - NOT NULL
    payment_sequential: T.IntegerType() = Field(nullable=False)  # Composite key - NOT NULL
    payment_type: T.StringType() = Field(nullable=False)  # NOT NULL
    payment_installments: T.IntegerType() = Field(nullable=True)
    payment_value: T.DecimalType(10, 2) = Field(nullable=True)

    # Custom dataframe-level check for row count
    @pa.dataframe_check
    def min_row_count(cls, df) -> bool:
        """Ensure DataFrame has at least 1 row."""
        return df.count() >= 1

    # Custom check for composite key uniqueness
    @pa.dataframe_check
    def unique_composite_key(cls, df) -> bool:
        """Ensure (order_id, payment_sequential) is unique."""
        total_count = df.count()
        distinct_count = df.select("order_id", "payment_sequential").distinct().count()
        return total_count == distinct_count

    # Custom check for payment_sequential >= 1
    @pa.dataframe_check
    def valid_payment_sequential(cls, df) -> bool:
        """Ensure payment_sequential >= 1."""
        invalid_count = df.filter(F.col("payment_sequential") < 1).count()
        return invalid_count == 0

    # Custom check for payment_installments >= 1
    @pa.dataframe_check
    def valid_installments(cls, df) -> bool:
        """Ensure payment_installments >= 1."""
        invalid_count = df.filter(
            (F.col("payment_installments").isNotNull()) & (F.col("payment_installments") < 1)
        ).count()
        return invalid_count == 0

    # Custom check for payment_value >= 0
    @pa.dataframe_check
    def valid_payment_value(cls, df) -> bool:
        """Ensure payment_value >= 0."""
        invalid_count = df.filter(
            (F.col("payment_value").isNotNull()) & (F.col("payment_value") < 0)
        ).count()
        return invalid_count == 0

    # Custom check for valid payment_type values
    @pa.dataframe_check
    def valid_payment_type(cls, df) -> bool:
        """Ensure payment_type is in valid set."""
        valid_types = ["credit_card", "boleto", "voucher", "debit_card", "not_defined"]
        invalid_count = df.filter(
            ~F.col("payment_type").isin(valid_types)
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
    df_validated = SilverOrderPaymentSchema.validate(check_obj=df)
    
    # 3. Collect errors from validation
    errors = df_validated.pandera.errors
    
    # 4. Build result
    result = {
        "success": len(errors) == 0,
        "errors": errors,
        "row_count": df.count(),
        "schema_name": "SilverOrderPaymentSchema"
    }
    
    return result


# --- ENTRYPOINT ---
# spark is already available in Databricks notebooks
CATALOG = os.getenv("CATALOG", "olist_project")
SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "silver")
TABLE_NAME = "order_payment"

logger.info(f"Config loaded: catalog={CATALOG}, silver={SILVER_SCHEMA}")

full_table_name = f"{CATALOG}.{SILVER_SCHEMA}.{TABLE_NAME}"

logger.info(f"--- Running Data Quality Tests for {full_table_name} ---")
logger.info(f"Using Pandera schema: SilverOrderPaymentSchema")

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
