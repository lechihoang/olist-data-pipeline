# Databricks notebook source
# Test Silver Products - Validates data quality for silver products table
# Using Great Expectations for production-grade testing

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import great_expectations as gx

load_dotenv()

def run_tests(spark: SparkSession, table_name: str):
    """Run data quality tests using Great Expectations"""
    
    df = spark.table(table_name)
    
    # Create ephemeral GX context
    context = gx.get_context(mode="ephemeral")
    
    # Create Spark datasource
    datasource = context.sources.add_or_update_spark("spark_ds")
    data_asset = datasource.add_dataframe_asset(name="products")
    batch_request = data_asset.build_batch_request(dataframe=df)
    
    # Create expectation suite
    suite = context.add_or_update_expectation_suite("silver_products_suite")
    
    # Create validator
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite
    )
    
    # === ROW COUNT ===
    validator.expect_table_row_count_to_be_between(min_value=1)
    
    # === SCHEMA VALIDATION ===
    validator.expect_table_columns_to_match_set(
        column_set=[
            "product_id",
            "product_category_name",
            "product_name_length",
            "product_description_length",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm"
        ],
        exact_match=False
    )
    
    # === UNIQUENESS ===
    validator.expect_column_values_to_be_unique("product_id")
    
    # === NOT NULL ===
    validator.expect_column_values_to_not_be_null("product_id")
    
    # === VALUE RANGE (from WHERE clause in transformation) ===
    # Note: These columns can be NULL, but if not NULL, must be >= 0
    validator.expect_column_values_to_be_between(
        column="product_weight_g",
        min_value=0,
        mostly=1.0  # Allow NULLs
    )
    
    validator.expect_column_values_to_be_between(
        column="product_length_cm",
        min_value=0,
        mostly=1.0
    )
    
    validator.expect_column_values_to_be_between(
        column="product_height_cm",
        min_value=0,
        mostly=1.0
    )
    
    validator.expect_column_values_to_be_between(
        column="product_width_cm",
        min_value=0,
        mostly=1.0
    )
    
    return validator.validate()


# --- ENTRYPOINT ---
if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    
    CATALOG = os.getenv("CATALOG", "olist_project")
    SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "silver")
    TABLE_NAME = "products"
    
    print(f"Config loaded: catalog={CATALOG}, silver={SILVER_SCHEMA}")
    
    full_table_name = f"{CATALOG}.{SILVER_SCHEMA}.{TABLE_NAME}"
    
    print(f"--- Running Data Quality Tests for {full_table_name} ---")
    
    results = run_tests(spark, full_table_name)
    
    # Print results
    for r in results.results:
        status = "PASSED" if r.success else "FAILED"
        exp_type = r.expectation_config.expectation_type
        print(f"  [{status}] {exp_type}")
    
    # Raise exception if any test failed
    if not results.success:
        failed = [r.expectation_config.expectation_type for r in results.results if not r.success]
        raise Exception(f"Data quality tests FAILED for {full_table_name}: {failed}")
    
    print(f"--- All tests PASSED for {full_table_name} ---")
