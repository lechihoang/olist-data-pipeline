# Databricks notebook source

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, avg, first
from pyspark.dbutils import DBUtils 

def process_silver_geolocation(spark: SparkSession, bronze_table_name: str, silver_table_name: str):

  print(f"Bắt đầu xử lý Silver (Chế độ Batch Overwrite):")
  print(f"  ĐỌC TỪ: {bronze_table_name}")
  print(f"  GHI VÀO: {silver_table_name}")

  bronze_df = spark.read.table(bronze_table_name)

  silver_df = (bronze_df
      .select(
        col("geolocation_zip_code_prefix").cast("string"),
        col("geolocation_lat").cast("double"),
        col("geolocation_lng").cast("double"),
        col("geolocation_city").cast("string"),
        col("geolocation_state").cast("string")
      )
      .withColumn("geolocation_city", upper(col("geolocation_city")))
      .withColumn("geolocation_state", upper(col("geolocation_state")))
      .where("geolocation_zip_code_prefix IS NOT NULL")
      .groupBy("geolocation_zip_code_prefix")
      .agg(
          avg("geolocation_lat").alias("geolocation_lat"),
          avg("geolocation_lng").alias("geolocation_lng"),
          first("geolocation_city").alias("geolocation_city"), # Lấy 1 tên city
          first("geolocation_state").alias("geolocation_state") # Lấy 1 tên state
      )
    )

  (silver_df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(silver_table_name)
  )
  print(f"Hoàn thành GHI ĐÈ Bảng Silver: {silver_table_name}")

if __name__ == "__main__":
  spark = SparkSession.builder.getOrCreate()
  dbutils = DBUtils(spark)
  bronze_table_input = dbutils.widgets.get("bronze_table_input")
  silver_table_output = dbutils.widgets.get("silver_table_output")
  CATALOG = "olist_project"
  BRONZE_SCHEMA = "bronze"
  SILVER_SCHEMA = "silver"
  bronze_table_full_name = f"{CATALOG}.{BRONZE_SCHEMA}.{bronze_table_input}"
  silver_table_full_name = f"{CATALOG}.{SILVER_SCHEMA}.{silver_table_output}"
  process_silver_geolocation(spark, bronze_table_full_name, silver_table_full_name)