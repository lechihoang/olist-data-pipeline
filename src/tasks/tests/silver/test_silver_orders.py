# Databricks notebook source
# Test Silver Orders - Validates data quality for silver orders table
# Using Pandera for PySpark (compatible with Databricks Serverless)

import os
from dotenv import load_dotenv
import pandera.pyspark as pa
from pandera.pyspark import DataFrameModel, Field
import pyspark.sql.types as T
from pyspark.sql import functions as F

load_dotenv()


# --- SCHEMA DEFINITION ---
class SilverOrdersSchema(DataFrameModel):
    """
    Pandera schema for silver.orders table.
    Validates schema structure and data quality.
    """
    
    # Define columns with their types and constraints
    order_id: T.StringType() = Field(nullable=False)  # Primary key - NOT NULL
    customer_id: T.StringType() = Field(nullable=True)
    order_status: T.StringType() = Field(nullable=False)  # NOT NULL
    order_purchase_timestamp: T.TimestampType() = Field(nullable=False)  # NOT NULL
    order_approved_at: T.TimestampType() = Field(nullable=True)
    order_delivered_carrier_date: T.TimestampType() = Field(nullable=True)
    order_delivered_customer_date: T.TimestampType() = Field(nullable=True)
    order_estimated_delivery_date: T.TimestampType() = Field(nullable=True)

    # Custom dataframe-level check for row count
    @pa.dataframe_check
    def min_row_count(cls, df) -> bool:
        """Ensure DataFrame has at least 1 row."""
        return df.count() >= 1

    # Custom check for uniqueness of order_id
    @pa.dataframe_check
    def unique_order_id(cls, df) -> bool:
        """Ensure order_id is unique."""
        total_count = df.count()
        distinct_count = df.select("order_id").distinct().count()
        return total_count == distinct_count

    # Custom check for valid order_status values
    @pa.dataframe_check
    def valid_order_status(cls, df) -> bool:
        """Ensure order_status is in valid set."""
        valid_statuses = [
            "delivered",
            "shipped",
            "canceled",
            "unavailable",
            "invoiced",
            "processing",
            "created",
            "approved"
        ]
        invalid_count = df.filter(
            ~F.col("order_status").isin(valid_statuses)
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
    df_validated = SilverOrdersSchema.validate(check_obj=df)
    
    # 3. Collect errors from validation
    errors = df_validated.pandera.errors
    
    # 4. Build result
    result = {
        "success": len(errors) == 0,
        "errors": errors,
        "row_count": df.count(),
        "schema_name": "SilverOrdersSchema"
    }
    
    return result


# --- ENTRYPOINT ---
# spark is already available in Databricks notebooks
CATALOG = os.getenv("CATALOG", "olist_project")
SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "silver")
TABLE_NAME = "orders"

print(f"Config loaded: catalog={CATALOG}, silver={SILVER_SCHEMA}")

full_table_name = f"{CATALOG}.{SILVER_SCHEMA}.{TABLE_NAME}"

print(f"--- Running Data Quality Tests for {full_table_name} ---")
print(f"Using Pandera schema: SilverOrdersSchema")

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
