# Databricks notebook source
# Test Silver Order - Validates data quality for silver order table
# Using Pandera for PySpark (compatible with Databricks Serverless)

import os
from dotenv import load_dotenv
import pandera.pyspark as pa
from pandera.pyspark import DataFrameModel, Field
import pyspark.sql.types as T
from pyspark.sql import functions as F

load_dotenv()


# --- HELPER FUNCTIONS ---
def check_timestamp_format(df, col_name: str) -> bool:
    """
    Check if timestamp column follows YYYY-MM-DD HH:MM:SS format.
    Example: 2017-09-19 09:45:35
    """
    pattern = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"
    invalid_count = df.filter(
        F.col(col_name).isNotNull() &
        ~F.col(col_name).cast("string").rlike(pattern)
    ).count()
    return invalid_count == 0


# --- SCHEMA DEFINITION ---
class SilverOrderSchema(DataFrameModel):
    """
    Pandera schema for silver.order table.
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

    # --- TIMESTAMP FORMAT VALIDATION ---
    @pa.dataframe_check
    def valid_order_purchase_timestamp_format(cls, df) -> bool:
        """Ensure order_purchase_timestamp follows YYYY-MM-DD HH:MM:SS format."""
        return check_timestamp_format(df, "order_purchase_timestamp")

    @pa.dataframe_check
    def valid_order_approved_at_format(cls, df) -> bool:
        """Ensure order_approved_at follows YYYY-MM-DD HH:MM:SS format."""
        return check_timestamp_format(df, "order_approved_at")

    @pa.dataframe_check
    def valid_order_delivered_carrier_date_format(cls, df) -> bool:
        """Ensure order_delivered_carrier_date follows YYYY-MM-DD HH:MM:SS format."""
        return check_timestamp_format(df, "order_delivered_carrier_date")

    @pa.dataframe_check
    def valid_order_delivered_customer_date_format(cls, df) -> bool:
        """Ensure order_delivered_customer_date follows YYYY-MM-DD HH:MM:SS format."""
        return check_timestamp_format(df, "order_delivered_customer_date")

    @pa.dataframe_check
    def valid_order_estimated_delivery_date_format(cls, df) -> bool:
        """Ensure order_estimated_delivery_date follows YYYY-MM-DD HH:MM:SS format."""
        return check_timestamp_format(df, "order_estimated_delivery_date")


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
    df_validated = SilverOrderSchema.validate(check_obj=df)
    
    # 3. Collect errors from validation
    errors = df_validated.pandera.errors
    
    # 4. Build result
    result = {
        "success": len(errors) == 0,
        "errors": errors,
        "row_count": df.count(),
        "schema_name": "SilverOrderSchema"
    }
    
    return result


# --- ENTRYPOINT ---
# spark is already available in Databricks notebooks
CATALOG = os.getenv("CATALOG", "olist_project")
SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "silver")
TABLE_NAME = "order"

print(f"Config loaded: catalog={CATALOG}, silver={SILVER_SCHEMA}")

full_table_name = f"{CATALOG}.{SILVER_SCHEMA}.{TABLE_NAME}"

print(f"--- Running Data Quality Tests for {full_table_name} ---")
print(f"Using Pandera schema: SilverOrderSchema")

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
