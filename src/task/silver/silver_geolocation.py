# Databricks notebook source

import os
import logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, avg, first
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


def process_silver_geolocation(spark: SparkSession, bronze_table_name: str, silver_table_name: str):

    logger.info(f"Starting Silver processing (batch overwrite mode):")
    logger.info(f"  Source: {bronze_table_name}")
    logger.info(f"  Target: {silver_table_name}")

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
            first("geolocation_city").alias("geolocation_city"),
            first("geolocation_state").alias("geolocation_state")
        )
    )

    (silver_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(silver_table_name)
    )
    logger.info(f"Completed overwriting Silver table: {silver_table_name}")


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
    process_silver_geolocation(spark, bronze_table_full_name, silver_table_full_name)
