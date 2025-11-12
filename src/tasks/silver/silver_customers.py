# Databricks notebook source

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper
from delta.tables import DeltaTable

def process_silver_customers(spark: SparkSession, bronze_table_name: str, silver_geo_table: str, silver_table_name: str, checkpoint_path: str):

    print(f"Bắt đầu xử lý Silver (Batch mode):")
    print(f"  ĐỌC TỪ: {bronze_table_name}")
    print(f"  JOIN VỚI: {silver_geo_table}")
    print(f"  GHI VÀO: {silver_table_name}")

    bronze_df = spark.readStream.table(bronze_table_name)

    geo_lookup_df = (spark.read.table(silver_geo_table)
                     .select("geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng")
                    )

    silver_df = (bronze_df
      .join(
          geo_lookup_df,
          bronze_df.customer_zip_code_prefix == geo_lookup_df.geolocation_zip_code_prefix,
          "left" 
      )
      .select(
        col("customer_id").cast("string"),
        col("customer_unique_id").cast("string"), 
        col("customer_zip_code_prefix").cast("string"),
        col("customer_city").cast("string"),
        col("customer_state").cast("string"),
        col("geolocation_lat").cast("double"),
        col("geolocation_lng").cast("double")
      )
      .where("customer_id IS NOT NULL AND customer_unique_id IS NOT NULL") 
      .withColumn("customer_city", upper(col("customer_city")))
      .withColumn("customer_state", upper(col("customer_state")))
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
                "s.customer_id = b.customer_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    (silver_df.writeStream
        .foreachBatch(upsert_to_silver) # ⭐️ Gọi hàm merge ở trên
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
    silver_geo_table_input = dbutils.widgets.get("silver_geo_table_input")
    silver_table_output = dbutils.widgets.get("silver_table_output")
    CATALOG = "olist_project"
    BRONZE_SCHEMA = "bronze"
    SILVER_SCHEMA = "silver"
    STAGING_SCHEMA = "staging"
    bronze_table_full_name = f"{CATALOG}.{BRONZE_SCHEMA}.{bronze_table_input}"
    silver_geo_table_full_name = f"{CATALOG}.{SILVER_SCHEMA}.{silver_geo_table_input}"
    silver_table_full_name = f"{CATALOG}.{SILVER_SCHEMA}.{silver_table_output}"
    checkpoint_path = f"/Volumes/{CATALOG}/{STAGING_SCHEMA}/checkpoints/{silver_table_output}"
    process_silver_customers(
        spark, 
        bronze_table_full_name, 
        silver_geo_table_full_name,
        silver_table_full_name, 
        checkpoint_path
    )