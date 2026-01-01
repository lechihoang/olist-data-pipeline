# Databricks notebook source

import os
import logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper
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


def process_silver_seller(spark: SparkSession, bronze_table_name: str, silver_table_name: str, checkpoint_path: str):
    
    logger.info(f"Starting Silver processing (batch mode):")
    logger.info(f"  Source: {bronze_table_name}")
    logger.info(f"  Target: {silver_table_name}")

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
        logger.info(f"Processing batch {batch_id}")
        
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
    logger.info(f"Completed Silver table: {silver_table_name}")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    
    logger.info("--- Configuration Loaded from .env ---")
    logger.info(f"  CATALOG:        {CATALOG}")
    logger.info(f"  BRONZE_SCHEMA:  {BRONZE_SCHEMA}")
    logger.info(f"  SILVER_SCHEMA:  {SILVER_SCHEMA}")
    logger.info(f"  STAGING_SCHEMA: {STAGING_SCHEMA}")
    logger.info("--------------------------------------")
        
    bronze_table_input = dbutils.widgets.get("bronze_table_input")
    silver_table_output = dbutils.widgets.get("silver_table_output")

    bronze_table_full_name = f"{CATALOG}.{BRONZE_SCHEMA}.{bronze_table_input}"
    silver_table_full_name = f"{CATALOG}.{SILVER_SCHEMA}.{silver_table_output}"
    checkpoint_path = f"/Volumes/{CATALOG}/{STAGING_SCHEMA}/checkpoints/{silver_table_output}"

    process_silver_seller(spark, bronze_table_full_name, silver_table_full_name, checkpoint_path)
