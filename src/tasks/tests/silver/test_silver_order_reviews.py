# Databricks notebook source
# Test Silver Order Reviews - Validates data quality for silver order_reviews table
# Requirements: 2.8 - Verify review_id UNIQUE, review_score between 1 and 5

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Load .env file from root folder
load_dotenv()

def run_tests(spark: SparkSession, table_name: str) -> list:
    """Run data quality tests for silver order_reviews table"""
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
    
    # Test 2: review_id UNIQUE
    distinct_count = df.select("review_id").distinct().count()
    results.append({
        "test_name": "review_id_unique",
        "passed": distinct_count == row_count,
        "details": f"Total rows: {row_count}, Distinct review_id: {distinct_count}",
        "row_count": row_count
    })
    
    # Test 3: review_score between 1 and 5
    invalid_score_count = df.filter((col("review_score") < 1) | (col("review_score") > 5)).count()
    results.append({
        "test_name": "review_score_valid_range",
        "passed": invalid_score_count == 0,
        "details": f"Invalid review_score count (not in 1-5): {invalid_score_count}",
        "row_count": row_count
    })
    
    return results

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
    for r in results:
        status = "✅ PASSED" if r["passed"] else "❌ FAILED"
        print(f"  {status}: {r['test_name']} - {r['details']}")
    
    # Raise exception if any test failed
    failed_tests = [r for r in results if not r["passed"]]
    if failed_tests:
        raise Exception(f"Data quality tests FAILED for {full_table_name}: {[r['test_name'] for r in failed_tests]}")
    
    print(f"--- All tests PASSED for {full_table_name} ---")
