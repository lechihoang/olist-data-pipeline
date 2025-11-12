# Databricks notebook source

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable


def process_silver_products(spark: SparkSession, bronze_products_table: str, bronze_translation_table: str, silver_products_table: str, checkpoint_path: str):
    
    print(f"Bắt đầu xử lý Silver (Batch mode):")
    print(f"  ĐỌC TỪ: {bronze_products_table} VÀ {bronze_translation_table}")
    print(f"  GHI VÀO: {silver_products_table}")


    bronze_df = spark.readStream.table(bronze_products_table)


    translation_df = (spark.read.table(bronze_translation_table)
                      .select("product_category_name", "product_category_name_english")
                      .distinct()
                     )

    silver_df = (bronze_df
        .join(
            translation_df, 
            bronze_df.product_category_name == translation_df.product_category_name, 
            "left"
        )
        .select(
            col("product_id").cast("string"),
            col("product_category_name_english").alias("product_category_name"), 
            
            col("product_name_lenght").cast("integer").alias("product_name_length"),
            col("product_description_lenght").cast("integer").alias("product_description_length"),
            col("product_photos_qty").cast("integer"),
            col("product_weight_g").cast("integer"),
            col("product_length_cm").cast("integer"),
            col("product_height_cm").cast("integer"),
            col("product_width_cm").cast("integer")
        )
        .where("product_id IS NOT NULL")
    )

    def upsert_to_silver(micro_batch_df, batch_id):
        
        print(f"Đang xử lý batch {batch_id}...")
        
        DeltaTable.createIfNotExists(spark) \
            .tableName(silver_products_table) \
            .addColumns(micro_batch_df.schema) \
            .execute()
            
        silver_delta_table = DeltaTable.forName(spark, silver_products_table)


        (silver_delta_table.alias("s") 
            .merge(
                micro_batch_df.alias("b"),
                "s.product_id = b.product_id"
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
    print(f"Hoàn thành xử lý Bảng Silver: {silver_products_table}")

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark) 
        
    
    bronze_products_table_input = dbutils.widgets.get("bronze_products_table_input")
    bronze_translation_table_input = dbutils.widgets.get("bronze_translation_table_input")
    silver_products_table_output = dbutils.widgets.get("silver_products_table_output")
    
    
    CATALOG = "olist_project"
    BRONZE_SCHEMA = "bronze"
    SILVER_SCHEMA = "silver"
    STAGING_SCHEMA = "staging"

    bronze_products_full_name = f"{CATALOG}.{BRONZE_SCHEMA}.{bronze_products_table_input}"
    bronze_translation_full_name = f"{CATALOG}.{BRONZE_SCHEMA}.{bronze_translation_table_input}"
    silver_products_full_name = f"{CATALOG}.{SILVER_SCHEMA}.{silver_products_table_output}"
    checkpoint_path = f"/Volumes/{CATALOG}/{STAGING_SCHEMA}/checkpoints/{silver_products_table_output}"

    
    process_silver_products(
        spark, 
        bronze_products_full_name, 
        bronze_translation_full_name, 
        silver_products_full_name, 
        checkpoint_path
    )