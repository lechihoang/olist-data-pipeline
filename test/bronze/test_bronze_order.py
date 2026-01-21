# Test Bronze Order - Validates data quality for order_raw table

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
class BronzeOrderSchema(DataFrameModel):
    """
    Pandera schema for bronze.order_raw table.
    Validates schema structure and data quality.
    """
    
    order_id: T.StringType() = Field(nullable=False)
    customer_id: T.StringType() = Field(nullable=False)
    order_status: T.StringType() = Field(nullable=True)
    order_purchase_timestamp: T.StringType() = Field(nullable=True)
    order_approved_at: T.StringType() = Field(nullable=True)
    order_delivered_carrier_date: T.StringType() = Field(nullable=True)
    order_delivered_customer_date: T.StringType() = Field(nullable=True)
    order_estimated_delivery_date: T.StringType() = Field(nullable=True)

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
    
    df_validated = BronzeOrderSchema.validate(check_obj=df)
    
    errors = df_validated.pandera.errors
    
    result = {
        "success": len(errors) == 0,
        "errors": errors,
        "row_count": df.count(),
        "schema_name": "BronzeOrderSchema"
    }
    
    return result


# --- ENTRYPOINT ---
CATALOG = os.getenv("CATALOG", "olist_project")
BRONZE_SCHEMA = os.getenv("BRONZE_SCHEMA", "bronze")
TABLE_NAME = "order_raw"

logger.info(f"Config loaded: catalog={CATALOG}, bronze={BRONZE_SCHEMA}")

full_table_name = f"{CATALOG}.{BRONZE_SCHEMA}.{TABLE_NAME}"

logger.info(f"--- Running Data Quality Tests for {full_table_name} ---")
logger.info(f"Using Pandera schema: BronzeOrderSchema")

result = run_tests(spark, full_table_name)

if result["success"]:
    logger.info(f"--- All tests PASSED for {full_table_name} ---")
    logger.info(f"  Row count: {result['row_count']}")
else:
    logger.error(f"--- Some tests FAILED for {full_table_name} ---")
    logger.error(f"  Row count: {result['row_count']}")
    logger.error(f"  Errors: {result['errors']}")
    raise Exception(f"Data quality tests FAILED for {full_table_name}: {result['errors']}")
