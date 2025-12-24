# Databricks notebook source
# Test Silver Orders - Validates data quality for silver orders table
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
    data_asset = datasource.add_dataframe_asset(name="orders")
    batch_request = data_asset.build_batch_request(dataframe=df)
    
    # Create expectation suite
    suite = context.add_or_update_expectation_suite("silver_orders_suite")
    
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
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date"
        ],
        exact_match=False
    )
    
    # === UNIQUENESS ===
    validator.expect_column_values_to_be_unique("order_id")
    
    # === NOT NULL ===
    validator.expect_column_values_to_not_be_null("order_id")
    validator.expect_column_values_to_not_be_null("order_status")
    validator.expect_column_values_to_not_be_null("order_purchase_timestamp")
    
    # === VALUE SET VALIDATION ===
    validator.expect_column_values_to_be_in_set(
        column="order_status",
        value_set=[
            "delivered",
            "shipped",
            "canceled",
            "unavailable",
            "invoiced",
            "processing",
            "created",
            "approved"
        ]
    )
    
    return validator.validate()


# --- ENTRYPOINT ---
if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    
    CATALOG = os.getenv("CATALOG", "olist_project")
    SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "silver")
    TABLE_NAME = "orders"
    
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
