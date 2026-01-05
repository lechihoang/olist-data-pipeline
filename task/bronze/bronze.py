# Databricks notebook source

import os
import logging
import yaml
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.dbutils import DBUtils
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType

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
LANDING_VOLUME = os.getenv("LANDING_VOLUME", "lakehouse")


def get_spark_type(type_name: str):
    type_map = {
        "string": StringType(),
        "integer": IntegerType(),
        "double": DoubleType(),
        "timestamp": TimestampType(),
        "boolean": BooleanType()
    }
    return type_map.get(type_name.lower(), StringType())


def load_schema_from_yaml(table_name: str, schema_base_path: str, layer: str = "bronze") -> StructType:
    """
    Load schema from YAML file in the schema registry.
    
    Args:
        table_name: Name of the table (e.g., 'customer_raw', 'order_raw')
        schema_base_path: Base path to schema directory
        layer: Layer name ('bronze' or 'silver')
    
    Returns:
        StructType for Spark DataFrame
    """
    schema_path = f"{schema_base_path}/{layer}/{table_name}.yaml"
    
    logger.info(f"  Reading schema from: {schema_path}")
    
    if not os.path.exists(schema_path):
        raise FileNotFoundError(
            f"Schema file not found at: {schema_path}. "
            f"Please check schema_base_path parameter or ensure {table_name}.yaml exists."
        )
    
    fields = []
    
    with open(schema_path, 'r') as f:
        schema_def = yaml.safe_load(f)
        for col_def in schema_def['columns']:
            fields.append(
                StructField(
                    col_def['name'],
                    get_spark_type(col_def['type']),
                    col_def.get('nullable', True)
                )
            )
            
    logger.info(f"  Schema loaded successfully (version: {schema_def.get('version', 'unknown')})")
    return StructType(fields)


def load_bronze(spark: SparkSession, landing_volume_path: str, bronze_table_name: str, checkpoint_path: str, schema_obj: StructType):
    
    logger.info(f"Starting Auto Loader (batch mode with explicit schema):")
    logger.info(f"  Source: {landing_volume_path}")
    logger.info(f"  Target: {bronze_table_name}")
    
    (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("multiLine", "true")
        .option("escape", "\"")
        .schema(schema_obj)
        .option("cloudFiles.schemaLocation", checkpoint_path)
        .option("maxFilesPerTrigger", 10)
        .load(landing_volume_path)
        
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_ingest_timestamp", current_timestamp())
        
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(bronze_table_name)
    )
    
    logger.info(f"Completed loading data into table: {bronze_table_name}")


if __name__ == "__main__":
    
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    
    logger.info("--- Configuration Loaded from .env ---")
    logger.info(f"  CATALOG:        {CATALOG}")
    logger.info(f"  BRONZE_SCHEMA:  {BRONZE_SCHEMA}")
    logger.info(f"  SILVER_SCHEMA:  {SILVER_SCHEMA}")
    logger.info(f"  STAGING_SCHEMA: {STAGING_SCHEMA}")
    logger.info(f"  LANDING_VOLUME: {LANDING_VOLUME}")
    logger.info("--------------------------------------")
        
    source_dir_name = dbutils.widgets.get("source_dir_name")
    target_table_name = dbutils.widgets.get("target_table_name")
    schema_base_path = dbutils.widgets.getArgument("schema_base_path", "/Workspace/${workspace.file_path}/schema")
    logger.info(f"  Schema base path: {schema_base_path}")
    
    loaded_schema = load_schema_from_yaml(target_table_name, schema_base_path)

    landing_volume_path = f"/Volumes/{CATALOG}/{STAGING_SCHEMA}/{LANDING_VOLUME}/{source_dir_name}"
    
    bronze_table_full_name = f"{CATALOG}.{BRONZE_SCHEMA}.{target_table_name}"
    
    checkpoint_path = f"/Volumes/{CATALOG}/{STAGING_SCHEMA}/checkpoints/bronze_{target_table_name}"
    
    logger.info("--- Task Configuration ---")
    logger.info(f"  Source Dir (Param):     {source_dir_name}")
    logger.info(f"  Target Table (Param):   {target_table_name}")
    logger.info(f"  Landing Volume Path:    {landing_volume_path}")
    logger.info(f"  Bronze Table Full Name: {bronze_table_full_name}")
    logger.info(f"  Checkpoint Path:        {checkpoint_path}")
    logger.info("--------------------------")
    
    load_bronze(spark, landing_volume_path, bronze_table_full_name, checkpoint_path, loaded_schema)
