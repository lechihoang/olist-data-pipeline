# Test Silver Product - Validates data quality for silver product table

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
class SilverProductSchema(DataFrameModel):
    """
    Pandera schema for silver.product table.
    Validates schema structure and data quality.
    """
    
    product_id: T.StringType() = Field(nullable=False)
    product_category_name: T.StringType() = Field(nullable=True)
    product_name_length: T.IntegerType() = Field(nullable=True)
    product_description_length: T.IntegerType() = Field(nullable=True)
    product_photos_qty: T.IntegerType() = Field(nullable=True)
    product_weight_g: T.DoubleType() = Field(nullable=True)
    product_length_cm: T.DoubleType() = Field(nullable=True)
    product_height_cm: T.DoubleType() = Field(nullable=True)
    product_width_cm: T.DoubleType() = Field(nullable=True)

    @pa.dataframe_check
    def min_row_count(cls, df) -> bool:
        """Ensure DataFrame has at least 1 row."""
        return df.count() >= 1

    @pa.dataframe_check
    def unique_product_id(cls, df) -> bool:
        """Ensure product_id is unique."""
        total_count = df.count()
        distinct_count = df.select("product_id").distinct().count()
        return total_count == distinct_count

    @pa.dataframe_check
    def valid_weight(cls, df) -> bool:
        """Ensure product_weight_g >= 0 when not null."""
        invalid_count = df.filter(
            (F.col("product_weight_g").isNotNull()) & (F.col("product_weight_g") < 0)
        ).count()
        return invalid_count == 0

    @pa.dataframe_check
    def valid_length(cls, df) -> bool:
        """Ensure product_length_cm >= 0 when not null."""
        invalid_count = df.filter(
            (F.col("product_length_cm").isNotNull()) & (F.col("product_length_cm") < 0)
        ).count()
        return invalid_count == 0

    @pa.dataframe_check
    def valid_height(cls, df) -> bool:
        """Ensure product_height_cm >= 0 when not null."""
        invalid_count = df.filter(
            (F.col("product_height_cm").isNotNull()) & (F.col("product_height_cm") < 0)
        ).count()
        return invalid_count == 0

    @pa.dataframe_check
    def valid_width(cls, df) -> bool:
        """Ensure product_width_cm >= 0 when not null."""
        invalid_count = df.filter(
            (F.col("product_width_cm").isNotNull()) & (F.col("product_width_cm") < 0)
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
    
    df_validated = SilverProductSchema.validate(check_obj=df)
    
    errors = df_validated.pandera.errors
    
    result = {
        "success": len(errors) == 0,
        "errors": errors,
        "row_count": df.count(),
        "schema_name": "SilverProductSchema"
    }
    
    return result


# --- ENTRYPOINT ---
CATALOG = os.getenv("CATALOG", "olist_project")
SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "silver")
TABLE_NAME = "product"

logger.info(f"Config loaded: catalog={CATALOG}, silver={SILVER_SCHEMA}")

full_table_name = f"{CATALOG}.{SILVER_SCHEMA}.{TABLE_NAME}"

logger.info(f"--- Running Data Quality Tests for {full_table_name} ---")
logger.info(f"Using Pandera schema: SilverProductSchema")

result = run_tests(spark, full_table_name)

if result["success"]:
    logger.info(f"--- All tests PASSED for {full_table_name} ---")
    logger.info(f"  Row count: {result['row_count']}")
else:
    logger.error(f"--- Some tests FAILED for {full_table_name} ---")
    logger.error(f"  Row count: {result['row_count']}")
    logger.error(f"  Errors: {result['errors']}")
    raise Exception(f"Data quality tests FAILED for {full_table_name}: {result['errors']}")
