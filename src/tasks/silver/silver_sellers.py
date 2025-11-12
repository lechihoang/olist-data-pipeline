# Databricks notebook source

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper
from delta.tables import DeltaTable


def process_silver_sellers(spark: SparkSession, bronze_table_name: str, silver_table_name: str, checkpoint_path: str):
    
    print(f"Bắt đầu xử lý Silver (Batch mode):")
    print(f"  ĐỌC TỪ: {bronze_table_name}")
    print(f"  GHI VÀO: {silver_table_name}")

    
    bronze_df = spark.readStream.table(bronze_table_name)

    
    silver_df = (bronze_df
      
      .select(
        col("seller_id").cast("string"),
        col("seller_zip_code_prefix").cast("string"),
        col("seller_city").cast("string"),
        col("seller_state").cast("string")
      )
      
      .where("seller_id IS NOT NULL") 
      
      
      .withColumn("seller_city", upper(col("seller_city")))
      .withColumn("seller_state", upper(col("seller_state")))
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
                "s.seller_id = b.seller_id"
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
        
    bronze_table_input = dbutils.widgets.get("bronze_table_input")
    silver_table_output = dbutils.widgets.get("silver_table_output")
    
    CATALOG = "olist_project"
    BRONZE_SCHEMA = "bronze"
    SILVER_SCHEMA = "silver"
    STAGING_SCHEMA = "staging"

    bronze_table_full_name = f"{CATALOG}.{BRONZE_SCHEMA}.{bronze_table_input}"
    silver_table_full_name = f"{CATALOG}.{SILVER_SCHEMA}.{silver_table_output}"
    checkpoint_path = f"/Volumes/{CATALOG}/{STAGING_SCHEMA}/checkpoints/{silver_table_output}"

    process_silver_sellers(spark, bronze_table_full_name, silver_table_full_name, checkpoint_path)