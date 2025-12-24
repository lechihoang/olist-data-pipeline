# Databricks notebook source
# Test Silver Order Reviews - Validates data quality for silver order_reviews table
# Using Pandera for PySpark (compatible with Databricks Serverless)

import os
from dotenv import load_dotenv
import pandera.pyspark as pa
from pandera.pyspark import DataFrameModel, Field
import pyspark.sql.types as T
from pyspark.sql import functions as F

load_dotenv()


# --- SCHEMA DEFINITION ---
class SilverOrderReviewsSchema(DataFrameModel):
    """
    Pandera schema for silver.order_reviews table.
    Validates schema structure and data quality.
    """
    
    # Define columns with their types and constraints
    review_id: T.StringType() = Field(nullable=False)  # Composite key - NOT NULL
    order_id: T.StringType() = Field(nullable=False)  # Composite key - NOT NULL
    review_score: T.IntegerType() = Field(nullable=True)
    review_comment_title: T.StringType() = Field(nullable=True)
    review_comment_message: T.StringType() = Field(nullable=True)
    review_creation_date: T.TimestampType() = Field(nullable=False)  # NOT NULL
    review_answer_timestamp: T.TimestampType() = Field(nullable=True)

    # Custom dataframe-level check for row count
    @pa.dataframe_check
    def min_row_count(cls, df) -> bool:
        """Ensure DataFrame has at least 1 row."""
        return df.count() >= 1

    # Custom check for composite key uniqueness
    @pa.dataframe_check
    def unique_composite_key(cls, df) -> bool:
        """Ensure (review_id, order_id) is unique."""
        total_count = df.count()
        distinct_count = df.select("review_id", "order_id").distinct().count()
        return total_count == distinct_count

    # Custom check for review_score between 1 and 5
    @pa.dataframe_check
    def valid_review_score(cls, df) -> bool:
        """Ensure review_score is between 1 and 5."""
        invalid_count = df.filter(
            (F.col("review_score").isNotNull()) & 
            ((F.col("review_score") < 1) | (F.col("review_score") > 5))
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
    df_validated = SilverOrderReviewsSchema.validate(check_obj=df)
    
    # 3. Collect errors from validation
    errors = df_validated.pandera.errors
    
    # 4. Build result
    result = {
        "success": len(errors) == 0,
        "errors": errors,
        "row_count": df.count(),
        "schema_name": "SilverOrderReviewsSchema"
    }
    
    return result


# --- ENTRYPOINT ---
# spark is already available in Databricks notebooks
CATALOG = os.getenv("CATALOG", "olist_project")
SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "silver")
TABLE_NAME = "order_reviews"

print(f"Config loaded: catalog={CATALOG}, silver={SILVER_SCHEMA}")

full_table_name = f"{CATALOG}.{SILVER_SCHEMA}.{TABLE_NAME}"

print(f"--- Running Data Quality Tests for {full_table_name} ---")
print(f"Using Pandera schema: SilverOrderReviewsSchema")

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
