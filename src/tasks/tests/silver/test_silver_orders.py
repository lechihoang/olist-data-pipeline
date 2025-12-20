# Databricks notebook source
# Test Silver Orders - Validates data quality for silver orders table
# Requirements: 2.2 - Verify order_id UNIQUE, order_status valid

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Valid order statuses based on Olist dataset
VALID_ORDER_STATUSES = [
    "delivered",
    "shipped",
    "canceled",
    "unavailable",
    "invoiced",
    "processing",
    "created",
    "approved"
]

def run_tests(spark: SparkSession, table_name: str) -> list:
    """Run data quality tests for silver orders table"""
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
    
    # Test 2: order_id UNIQUE
    distinct_count = df.select("order_id").distinct().count()
    results.append({
        "test_name": "order_id_unique",
        "passed": distinct_count == row_count,
        "details": f"Total rows: {row_count}, Distinct order_id: {distinct_count}",
        "row_count": row_count
    })
    
    # Test 3: order_status valid
    invalid_status_count = df.filter(~col("order_status").isin(VALID_ORDER_STATUSES)).count()
    results.append({
        "test_name": "order_status_valid",
        "passed": invalid_status_count == 0,
        "details": f"Invalid order_status count: {invalid_status_count}",
        "row_count": row_count
    })
    
    return results

# --- ENTRYPOINT ---
if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    
    CATALOG = "olist_project"
    SILVER_SCHEMA = "silver"
    TABLE_NAME = "orders"
    
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
