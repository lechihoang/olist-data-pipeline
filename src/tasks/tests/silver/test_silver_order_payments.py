# Databricks notebook source
# Test Silver Order Payments - Validates data quality for silver order_payments table
# Requirements: 2.4 - Verify composite key (order_id, payment_sequential) UNIQUE, payment_value >= 0, payment_type valid

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Load .env file from root folder
load_dotenv()

# Valid payment types based on Olist dataset
VALID_PAYMENT_TYPES = [
    "credit_card",
    "boleto",
    "voucher",
    "debit_card",
    "not_defined"
]

def run_tests(spark: SparkSession, table_name: str) -> list:
    """Run data quality tests for silver order_payments table"""
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
    
    # Test 2: Composite key (order_id, payment_sequential) UNIQUE
    distinct_count = df.select("order_id", "payment_sequential").distinct().count()
    results.append({
        "test_name": "composite_key_unique",
        "passed": distinct_count == row_count,
        "details": f"Total rows: {row_count}, Distinct (order_id, payment_sequential): {distinct_count}",
        "row_count": row_count
    })
    
    # Test 3: payment_value >= 0
    negative_payment_count = df.filter(col("payment_value") < 0).count()
    results.append({
        "test_name": "payment_value_non_negative",
        "passed": negative_payment_count == 0,
        "details": f"Negative payment_value count: {negative_payment_count}",
        "row_count": row_count
    })
    
    # Test 4: payment_type valid
    invalid_type_count = df.filter(~col("payment_type").isin(VALID_PAYMENT_TYPES)).count()
    results.append({
        "test_name": "payment_type_valid",
        "passed": invalid_type_count == 0,
        "details": f"Invalid payment_type count: {invalid_type_count}",
        "row_count": row_count
    })
    
    return results

# --- ENTRYPOINT ---
if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    
    CATALOG = os.getenv("CATALOG", "olist_project")
    SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "silver")
    TABLE_NAME = "order_payments"
    
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
