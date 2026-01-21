# Test Silver Seller - Validates data quality for silver seller table

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
class SilverSellerSchema(DataFrameModel):
    """
    Pandera schema for silver.seller table.
    Validates schema structure and data quality.
    """
    
    seller_id: T.StringType() = Field(nullable=False)
    seller_zip_code_prefix: T.StringType() = Field(nullable=True)
    seller_city: T.StringType() = Field(nullable=True)
    seller_state: T.StringType() = Field(nullable=True)

    @pa.dataframe_check
    def min_row_count(cls, df) -> bool:
        """Ensure DataFrame has at least 1 row."""
        return df.count() >= 1

    @pa.dataframe_check
    def unique_seller_id(cls, df) -> bool:
        """Ensure seller_id is unique."""
        total_count = df.count()
        distinct_count = df.select("seller_id").distinct().count()
        return total_count == distinct_count

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
    df = spark.table(table_name)
    
    df_validated = SilverSellerSchema.validate(check_obj=df)
    
    errors = df_validated.pandera.errors
    
    result = {
        "success": len(errors) == 0,
        "errors": errors,
        "row_count": df.count(),
        "schema_name": "SilverSellerSchema"
    }
    
    return result


# --- ENTRYPOINT ---
CATALOG = os.getenv("CATALOG", "olist_project")
SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "silver")
TABLE_NAME = "seller"

logger.info(f"Config loaded: catalog={CATALOG}, silver={SILVER_SCHEMA}")

full_table_name = f"{CATALOG}.{SILVER_SCHEMA}.{TABLE_NAME}"

logger.info(f"--- Running Data Quality Tests for {full_table_name} ---")
logger.info(f"Using Pandera schema: SilverSellerSchema")

result = run_tests(spark, full_table_name)

if result["success"]:
    logger.info(f"--- All tests PASSED for {full_table_name} ---")
    logger.info(f"  Row count: {result['row_count']}")
else:
    logger.error(f"--- Some tests FAILED for {full_table_name} ---")
    logger.error(f"  Row count: {result['row_count']}")
    logger.error(f"  Errors: {result['errors']}")
    raise Exception(f"Data quality tests FAILED for {full_table_name}: {result['errors']}")
