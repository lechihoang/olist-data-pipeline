# Databricks notebook source
# Test Bronze Sellers - Validates data quality for sellers_raw table
# Requirements: 1.6 - Verify seller_id NOT NULL

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Load .env file from root folder
load_dotenv()

def run_tests(spark: SparkSession, table_name: str) -> list:
    """Run data quality tests for bronze sellers table"""
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
    
    # Test 2: seller_id NOT NULL
    null_seller_id = df.filter(col("seller_id").isNull()).count()
    results.append({
        "test_name": "seller_id_not_null",
        "passed": null_seller_id == 0,
        "details": f"NULL seller_id count: {null_seller_id}",
        "row_count": row_count
    })
    
    return results

# --- ENTRYPOINT ---
if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    
    CATALOG = os.getenv("CATALOG", "olist_project")
    BRONZE_SCHEMA = os.getenv("BRONZE_SCHEMA", "bronze")
    TABLE_NAME = "sellers_raw"
    
    print(f"Config loaded: catalog={CATALOG}, bronze={BRONZE_SCHEMA}")
    
    full_table_name = f"{CATALOG}.{BRONZE_SCHEMA}.{TABLE_NAME}"
    
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
