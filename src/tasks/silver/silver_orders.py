# Databricks notebook source

import sys
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable
from pyspark.dbutils import DBUtils 

# --- LOAD CONFIGURATION FROM .env ---
env_path = "/Workspace/${workspace.file_path}/.env"
load_dotenv(env_path)

# Configuration với fallback defaults
CATALOG = os.getenv("CATALOG", "olist_project")
BRONZE_SCHEMA = os.getenv("BRONZE_SCHEMA", "bronze")
SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "silver")
STAGING_SCHEMA = os.getenv("STAGING_SCHEMA", "staging")

def process_silver_orders(spark: SparkSession, bronze_table_name: str, silver_table_name: str, checkpoint_path: str):
    
    print(f"Bắt đầu xử lý Silver (Batch mode):")
    print(f"  ĐỌC TỪ: {bronze_table_name}")
    print(f"  GHI VÀO: {silver_table_name}")

    bronze_df = spark.readStream.table(bronze_table_name)

    silver_df = (bronze_df
      .select(
        col("order_id").cast("string"),
        col("customer_id").cast("string"),
        col("order_status").cast("string"),
        col("order_purchase_timestamp").cast("timestamp"),
        col("order_approved_at").cast("timestamp"),
        col("order_delivered_carrier_date").cast("timestamp"),
        col("order_delivered_customer_date").cast("timestamp"),
        col("order_estimated_delivery_date").cast("timestamp")
      )
      .where("""
          order_id IS NOT NULL
          AND order_status IS NOT NULL
          AND TRIM(order_status) != ''
          AND order_purchase_timestamp IS NOT NULL
          AND (
              order_estimated_delivery_date IS NULL 
              OR order_purchase_timestamp IS NULL
              OR order_estimated_delivery_date >= order_purchase_timestamp
          )
      """) 
    )

    def upsert_to_silver(micro_batch_df, batch_id):
        print(f"Đang xử lý batch {batch_id}...")
        
        DeltaTable.createIfNotExists(spark) \
            .tableName(silver_table_name) \
            .addColumns(micro_batch_df.schema) \
            .execute()
            
        silver_delta_table = DeltaTable.forName(spark, silver_table_name)

        (silver_delta_table.alias("s") 
            .merge(
                micro_batch_df.alias("b"),
                "s.order_id = b.order_id"  
            )
            .whenMatchedUpdateAll()     
            .whenNotMatchedInsertAll()  
            .execute()
        )

    (silver_df.writeStream
        .foreachBatch(upsert_to_silver)
        .outputMode("update")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )
    print(f"Hoàn thành xử lý Bảng Silver: {silver_table_name}")

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark) 
    
    # Print all resolved configuration values for debugging
    print("--- Configuration Loaded from .env ---")
    print(f"  CATALOG:        {CATALOG}")
    print(f"  BRONZE_SCHEMA:  {BRONZE_SCHEMA}")
    print(f"  SILVER_SCHEMA:  {SILVER_SCHEMA}")
    print(f"  STAGING_SCHEMA: {STAGING_SCHEMA}")
    print("--------------------------------------")
        
    bronze_table_input = dbutils.widgets.get("bronze_table_input")
    silver_table_output = dbutils.widgets.get("silver_table_output")

    bronze_table_full_name = f"{CATALOG}.{BRONZE_SCHEMA}.{bronze_table_input}"
    silver_table_full_name = f"{CATALOG}.{SILVER_SCHEMA}.{silver_table_output}"
    checkpoint_path = f"/Volumes/{CATALOG}/{STAGING_SCHEMA}/checkpoints/{silver_table_output}"

    process_silver_orders(spark, bronze_table_full_name, silver_table_full_name, checkpoint_path)