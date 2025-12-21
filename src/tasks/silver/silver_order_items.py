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

def process_silver_order_items(spark: SparkSession, bronze_table_name: str, silver_table_name: str, checkpoint_path: str):

    print(f"Bắt đầu xử lý Silver (Batch mode):")
    print(f"  ĐỌC TỪ: {bronze_table_name}")
    print(f"  GHI VÀO: {silver_table_name}")


    bronze_df = spark.readStream.table(bronze_table_name)
    silver_df = (bronze_df
        .select(
            col("order_id").cast("string"),
            col("order_item_id").cast("integer"),
            col("product_id").cast("string"),
            col("seller_id").cast("string"),
            col("shipping_limit_date").cast("timestamp"),
            col("price").cast("decimal(10, 2)"),
            col("freight_value").cast("decimal(10, 2)")
        )
        .where("""
            order_id IS NOT NULL 
            AND order_item_id IS NOT NULL
            AND order_item_id > 0
            AND price >= 0
            AND freight_value >= 0
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
                "s.order_id = b.order_id AND s.order_item_id = b.order_item_id"
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
    process_silver_order_items(spark, bronze_table_full_name, silver_table_full_name, checkpoint_path)