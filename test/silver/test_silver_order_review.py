# Test Silver Order Review - Validates data quality for silver order_review table

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
class SilverOrderReviewSchema(DataFrameModel):
    """
    Pandera schema for silver.order_review table.
    Validates schema structure and data quality.
    """
    
    review_id: T.StringType() = Field(nullable=False)
    order_id: T.StringType() = Field(nullable=False)
    review_score: T.IntegerType() = Field(nullable=True)
    review_comment_title: T.StringType() = Field(nullable=True)
    review_comment_message: T.StringType() = Field(nullable=True)
    review_creation_date: T.TimestampType() = Field(nullable=False)
    review_answer_timestamp: T.TimestampType() = Field(nullable=True)

    @pa.dataframe_check
    def min_row_count(cls, df) -> bool:
        """Ensure DataFrame has at least 1 row."""
        return df.count() >= 1

    @pa.dataframe_check
    def unique_composite_key(cls, df) -> bool:
        """Ensure (review_id, order_id) is unique."""
        total_count = df.count()
        distinct_count = df.select("review_id", "order_id").distinct().count()
        return total_count == distinct_count

    @pa.dataframe_check
    def valid_review_score(cls, df) -> bool:
        """Ensure review_score is between 1 and 5."""
        invalid_count = df.filter(
            (F.col("review_score").isNotNull()) & 
            ((F.col("review_score") < 1) | (F.col("review_score") > 5))
        ).count()
        return invalid_count == 0

    @pa.dataframe_check
    def valid_review_creation_date_format(cls, df) -> bool:
        """Ensure review_creation_date follows YYYY-MM-DD HH:MM:SS format."""
        return check_timestamp_format(df, "review_creation_date")

    @pa.dataframe_check
    def valid_review_answer_timestamp_format(cls, df) -> bool:
        """Ensure review_answer_timestamp follows YYYY-MM-DD HH:MM:SS format."""
        return check_timestamp_format(df, "review_answer_timestamp")


def run_tests(spark, table_name: str) -> dict:
    """
    Run data quality tests using Pandera.
    
    Args:
        spark: SparkSession
        table_name: Full table name (catalog.schema.table)
    
    Returns:
        dict with 'success' boolean and 'errors' details
    """
    df = spark.table(table_name)
    
    df_validated = SilverOrderReviewSchema.validate(check_obj=df)
    
    errors = df_validated.pandera.errors
    
    result = {
        "success": len(errors) == 0,
        "errors": errors,
        "row_count": df.count(),
        "schema_name": "SilverOrderReviewSchema"
    }
    
    return result


# --- ENTRYPOINT ---
CATALOG = os.getenv("CATALOG", "olist_project")
SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "silver")
TABLE_NAME = "order_review"

logger.info(f"Config loaded: catalog={CATALOG}, silver={SILVER_SCHEMA}")

full_table_name = f"{CATALOG}.{SILVER_SCHEMA}.{TABLE_NAME}"

logger.info(f"--- Running Data Quality Tests for {full_table_name} ---")
logger.info(f"Using Pandera schema: SilverOrderReviewSchema")

result = run_tests(spark, full_table_name)

if result["success"]:
    logger.info(f"--- All tests PASSED for {full_table_name} ---")
    logger.info(f"  Row count: {result['row_count']}")
else:
    logger.error(f"--- Some tests FAILED for {full_table_name} ---")
    logger.error(f"  Row count: {result['row_count']}")
    logger.error(f"  Errors: {result['errors']}")
    raise Exception(f"Data quality tests FAILED for {full_table_name}: {result['errors']}")
