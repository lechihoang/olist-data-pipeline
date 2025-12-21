# Databricks notebook source
# Test Silver Order Items - Validates data quality for silver order_items table
# Requirements: 2.3 - Verify order_id + order_item_id composite key, price >= 0

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws

# Load .env file from root folder
load_dotenv()

def run_tests(spark: SparkSession, table_name: str) -> list:
    """Run data quality tests for silver order_items table"""
    results = []
    
    df = spark.table(table_name)
    
    # Test 1: Row count > 0
    row_count = df.count()
    results.append({
        "test_name": "row_count_positive",
        "passed": row_count > 0,
        "details": f"Row count: {row_count}",
        "row_count": row_count
    })
    
    # Test 2: order_id + order_item_id composite key UNIQUE
    composite_key_df = df.select(concat_ws("_", col("order_id"), col("order_item_id")).alias("composite_key"))
    distinct_count = composite_key_df.distinct().count()
    results.append({
        "test_name": "composite_key_unique",
        "passed": distinct_count == row_count,
        "details": f"Total rows: {row_count}, Distinct (order_id, order_item_id): {distinct_count}",
        "row_count": row_count
    })
    
    # Test 3: price >= 0
    negative_price_count = df.filter(col("price") < 0).count()
    results.append({
        "test_name": "price_non_negative",
        "passed": negative_price_count == 0,
        "details": f"Negative price count: {negative_price_count}",
        "row_count": row_count
    })
    
    # Test 4: freight_value >= 0
    negative_freight_count = df.filter(col("freight_value") < 0).count()
    results.append({
        "test_name": "freight_value_non_negative",
        "passed": negative_freight_count == 0,
        "details": f"Negative freight_value count: {negative_freight_count}",
        "row_count": row_count
    })
    
    return results

# --- ENTRYPOINT ---
if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    
    CATALOG = os.getenv("CATALOG", "olist_project")
    SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "silver")
    TABLE_NAME = "order_items"
    
    print(f"Config loaded: catalog={CATALOG}, silver={SILVER_SCHEMA}")
    
    full_table_name = f"{CATALOG}.{SILVER_SCHEMA}.{TABLE_NAME}"
    
    print(f"--- Running Data Quality Tests for {full_table_name} ---")
    
    results = run_tests(spark, full_table_name)
    
    # Print results
    for r in results:
        status = "✅ PASSED" if r["passed"] else "❌ FAILED"
        print(f"  {status}: {r['test_name']} - {r['details']}")
    
    # Raise exception if any test failed
    failed_tests = [r for r in results if not r["passed"]]
    if failed_tests:
        raise Exception(f"Data quality tests FAILED for {full_table_name}: {[r['test_name'] for r in failed_tests]}")
    
    print(f"--- All tests PASSED for {full_table_name} ---")
