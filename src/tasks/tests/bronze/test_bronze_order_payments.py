# Databricks notebook source
# Test Bronze Order Payments - Validates data quality for order_payments_raw table
# Requirements: 1.4 - Verify order_id NOT NULL, payment_value >= 0

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def run_tests(spark: SparkSession, table_name: str) -> list:
    """Run data quality tests for bronze order_payments table"""
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
    
    # Test 2: order_id NOT NULL
    null_order_id = df.filter(col("order_id").isNull()).count()
    results.append({
        "test_name": "order_id_not_null",
        "passed": null_order_id == 0,
        "details": f"NULL order_id count: {null_order_id}",
        "row_count": row_count
    })
    
    # Test 3: payment_value >= 0
    negative_payment = df.filter(col("payment_value") < 0).count()
    results.append({
        "test_name": "payment_value_non_negative",
        "passed": negative_payment == 0,
        "details": f"Negative payment_value count: {negative_payment}",
        "row_count": row_count
    })
    
    return results

# --- ENTRYPOINT ---
if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    
    CATALOG = "olist_project"
    BRONZE_SCHEMA = "bronze"
    TABLE_NAME = "order_payments_raw"
    
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
