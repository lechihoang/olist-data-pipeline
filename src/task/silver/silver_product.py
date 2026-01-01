# Databricks notebook source

import os
import logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable
from pyspark.dbutils import DBUtils

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

env_path = "/Workspace/${workspace.file_path}/.env"
load_dotenv(env_path)

CATALOG = os.getenv("CATALOG", "olist_project")
BRONZE_SCHEMA = os.getenv("BRONZE_SCHEMA", "bronze")
SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "silver")
STAGING_SCHEMA = os.getenv("STAGING_SCHEMA", "staging")


def process_silver_product(spark: SparkSession, bronze_product_table: str, bronze_translation_table: str, silver_product_table: str, checkpoint_path: str):
    
    logger.info(f"Starting Silver processing (batch mode):")
    logger.info(f"  Source: {bronze_product_table} AND {bronze_translation_table}")
    logger.info(f"  Target: {silver_product_table}")

    bronze_df = spark.readStream.table(bronze_product_table)

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
            col("product_weight_g").cast("double"),
            col("product_length_cm").cast("double"),
            col("product_height_cm").cast("double"),
            col("product_width_cm").cast("double")
        )
        .where("""
            product_id IS NOT NULL
            AND (product_weight_g IS NULL OR product_weight_g >= 0)
            AND (product_length_cm IS NULL OR product_length_cm >= 0)
            AND (product_height_cm IS NULL OR product_height_cm >= 0)
            AND (product_width_cm IS NULL OR product_width_cm >= 0)
        """)
    )

    def upsert_to_silver(micro_batch_df, batch_id):
        logger.info(f"Processing batch {batch_id}")
        
        DeltaTable.createIfNotExists(spark) \
            .tableName(silver_product_table) \
            .addColumns(micro_batch_df.schema) \
            .execute()
            
        silver_delta_table = DeltaTable.forName(spark, silver_product_table)

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
    logger.info(f"Completed Silver table: {silver_product_table}")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    
    logger.info("--- Configuration Loaded from .env ---")
    logger.info(f"  CATALOG:        {CATALOG}")
    logger.info(f"  BRONZE_SCHEMA:  {BRONZE_SCHEMA}")
    logger.info(f"  SILVER_SCHEMA:  {SILVER_SCHEMA}")
    logger.info(f"  STAGING_SCHEMA: {STAGING_SCHEMA}")
    logger.info("--------------------------------------")
    
    bronze_product_table_input = dbutils.widgets.get("bronze_product_table_input")
    bronze_translation_table_input = dbutils.widgets.get("bronze_translation_table_input")
    silver_product_table_output = dbutils.widgets.get("silver_product_table_output")

    bronze_product_full_name = f"{CATALOG}.{BRONZE_SCHEMA}.{bronze_product_table_input}"
    bronze_translation_full_name = f"{CATALOG}.{BRONZE_SCHEMA}.{bronze_translation_table_input}"
    silver_product_full_name = f"{CATALOG}.{SILVER_SCHEMA}.{silver_product_table_output}"
    checkpoint_path = f"/Volumes/{CATALOG}/{STAGING_SCHEMA}/checkpoints/{silver_product_table_output}"

    process_silver_product(
        spark,
        bronze_product_full_name,
        bronze_translation_full_name,
        silver_product_full_name,
        checkpoint_path
    )
