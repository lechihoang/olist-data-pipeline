# Test Bronze Seller - Validates data quality for seller_raw table

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
class BronzeSellerSchema(DataFrameModel):
    """
    Pandera schema for bronze.seller_raw table.
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
    
    df_validated = BronzeSellerSchema.validate(check_obj=df)
    
    errors = df_validated.pandera.errors
    
    result = {
        "success": len(errors) == 0,
        "errors": errors,
        "row_count": df.count(),
        "schema_name": "BronzeSellerSchema"
    }
    
    return result


# --- ENTRYPOINT ---
CATALOG = os.getenv("CATALOG", "olist_project")
BRONZE_SCHEMA = os.getenv("BRONZE_SCHEMA", "bronze")
TABLE_NAME = "seller_raw"

logger.info(f"Config loaded: catalog={CATALOG}, bronze={BRONZE_SCHEMA}")

full_table_name = f"{CATALOG}.{BRONZE_SCHEMA}.{TABLE_NAME}"

logger.info(f"--- Running Data Quality Tests for {full_table_name} ---")
logger.info(f"Using Pandera schema: BronzeSellerSchema")

result = run_tests(spark, full_table_name)

if result["success"]:
    logger.info(f"--- All tests PASSED for {full_table_name} ---")
    logger.info(f"  Row count: {result['row_count']}")
else:
    logger.error(f"--- Some tests FAILED for {full_table_name} ---")
    logger.error(f"  Row count: {result['row_count']}")
    logger.error(f"  Errors: {result['errors']}")
    raise Exception(f"Data quality tests FAILED for {full_table_name}: {result['errors']}")
