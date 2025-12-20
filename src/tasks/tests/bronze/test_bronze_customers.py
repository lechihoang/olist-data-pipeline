# Databricks notebook source
# Test Bronze Customers - Validates data quality for customers_raw table
# Requirements: 1.2 - Verify customer_id, customer_unique_id NOT NULL

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def run_tests(spark: SparkSession, table_name: str) -> list:
    """Run data quality tests for bronze customers table"""
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
    
    # Test 2: customer_id NOT NULL
    null_customer_id = df.filter(col("customer_id").isNull()).count()
    results.append({
        "test_name": "customer_id_not_null",
        "passed": null_customer_id == 0,
        "details": f"NULL customer_id count: {null_customer_id}",
        "row_count": row_count
    })
    
    # Test 3: customer_unique_id NOT NULL
    null_unique_id = df.filter(col("customer_unique_id").isNull()).count()
    results.append({
        "test_name": "customer_unique_id_not_null",
        "passed": null_unique_id == 0,
        "details": f"NULL customer_unique_id count: {null_unique_id}",
        "row_count": row_count
    })
    
    return results

# --- ENTRYPOINT ---
if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    
    CATALOG = "olist_project"
    BRONZE_SCHEMA = "bronze"
    TABLE_NAME = "customers_raw"
    
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
