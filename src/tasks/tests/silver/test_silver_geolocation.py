# Databricks notebook source
# Test Silver Geolocation - Validates data quality for silver geolocation table
# Requirements: 2.7 - Verify geolocation_zip_code_prefix UNIQUE after deduplication

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def run_tests(spark: SparkSession, table_name: str) -> list:
    """Run data quality tests for silver geolocation table"""
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
    
    # Test 2: geolocation_zip_code_prefix UNIQUE (after deduplication in silver)
    distinct_count = df.select("geolocation_zip_code_prefix").distinct().count()
    results.append({
        "test_name": "geolocation_zip_code_prefix_unique",
        "passed": distinct_count == row_count,
        "details": f"Total rows: {row_count}, Distinct zip_code_prefix: {distinct_count}",
        "row_count": row_count
    })
    
    return results

# --- ENTRYPOINT ---
if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    
    CATALOG = "olist_project"
    SILVER_SCHEMA = "silver"
    TABLE_NAME = "geolocation"
    
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
