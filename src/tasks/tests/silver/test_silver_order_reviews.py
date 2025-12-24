# Databricks notebook source
# Test Silver Order Reviews - Validates data quality for silver order_reviews table
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
    data_asset = datasource.add_dataframe_asset(name="order_reviews")
    batch_request = data_asset.build_batch_request(dataframe=df)
    
    # Create expectation suite
    suite = context.add_or_update_expectation_suite("silver_order_reviews_suite")
    
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
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp"
        ],
        exact_match=False
    )
    
    # === COMPOSITE KEY UNIQUENESS ===
    # Note: review_id alone has duplicates, but (review_id, order_id) is unique
    validator.expect_compound_columns_to_be_unique(
        column_list=["review_id", "order_id"]
    )
    
    # === NOT NULL ===
    validator.expect_column_values_to_not_be_null("review_id")
    validator.expect_column_values_to_not_be_null("order_id")
    validator.expect_column_values_to_not_be_null("review_creation_date")
    
    # === VALUE RANGE (from WHERE clause in transformation) ===
    validator.expect_column_values_to_be_between(
        column="review_score",
        min_value=1,
        max_value=5
    )
    
    return validator.validate()


# --- ENTRYPOINT ---
if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    
    CATALOG = os.getenv("CATALOG", "olist_project")
    SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "silver")
    TABLE_NAME = "order_reviews"
    
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
