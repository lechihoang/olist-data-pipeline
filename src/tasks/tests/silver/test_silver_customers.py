# Databricks notebook source
# Test Silver Customers - Validates data quality for silver customers table
# Requirements: 2.1 - Verify customer_id UNIQUE, customer_state UPPERCASE

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

# Load .env file from root folder
load_dotenv()

def run_tests(spark: SparkSession, table_name: str) -> list:
    """Run data quality tests for silver customers table"""
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
    
    # Test 2: customer_id UNIQUE
    distinct_count = df.select("customer_id").distinct().count()
    results.append({
        "test_name": "customer_id_unique",
        "passed": distinct_count == row_count,
        "details": f"Total rows: {row_count}, Distinct customer_id: {distinct_count}",
        "row_count": row_count
    })
    
    # Test 3: customer_state UPPERCASE
    non_uppercase_count = df.filter(col("customer_state") != upper(col("customer_state"))).count()
    results.append({
        "test_name": "customer_state_uppercase",
        "passed": non_uppercase_count == 0,
        "details": f"Non-uppercase customer_state count: {non_uppercase_count}",
        "row_count": row_count
    })
    
    return results

# --- ENTRYPOINT ---
if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    
    CATALOG = os.getenv("CATALOG", "olist_project")
    SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "silver")
    TABLE_NAME = "customers"
    
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
