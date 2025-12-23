# Databricks notebook source
# ⬆️ DÒNG NÀY RẤT QUAN TRỌNG, HÃY GIỮ NÓ

# Tên file: src/tasks/bronze/bronze_static.py
# --------------------
# ⚠️ CHẠY FILE NÀY TRÊN DATABRICKS JOB
# --------------------
# Mục đích: Load static/reference data (geolocation, product_translation)
# Không dùng Auto Loader vì data này không thay đổi
# Dùng spark.read.csv() + OVERWRITE mode

import json
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.dbutils import DBUtils
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType

# --- LOAD CONFIGURATION FROM .env ---
env_path = "/Workspace/${workspace.file_path}/.env"
load_dotenv(env_path)

# Configuration với fallback defaults
CATALOG = os.getenv("CATALOG", "olist_project")
BRONZE_SCHEMA = os.getenv("BRONZE_SCHEMA", "bronze")
STAGING_SCHEMA = os.getenv("STAGING_SCHEMA", "staging")
LANDING_VOLUME = os.getenv("LANDING_VOLUME", "landing_zone")

# --- HÀM HỖ TRỢ 1 (Giống bronze.py) ---
def get_spark_type(type_name: str):
    type_map = {
        "string": StringType(),
        "integer": IntegerType(),
        "double": DoubleType(),
        "timestamp": TimestampType(),
        "boolean": BooleanType()
    }
    return type_map.get(type_name.lower(), StringType())

# --- HÀM HỖ TRỢ 2 (Giống bronze.py) ---
def load_schema_from_json(table_name: str, schema_base_path: str) -> StructType:
    """
    Load schema từ JSON file với configurable base path.
    """
    schema_path = f"{schema_base_path}/{table_name}.json"
    
    print(f"  Đang đọc schema từ: {schema_path}")
    
    if not os.path.exists(schema_path):
        raise FileNotFoundError(
            f"Schema file không tồn tại tại đường dẫn: {schema_path}. "
            f"Vui lòng kiểm tra schema_base_path parameter hoặc đảm bảo file {table_name}.json tồn tại."
        )
    
    fields = []
    
    with open(schema_path, 'r') as f:
        schema_json = json.load(f)
        for col_def in schema_json:
            fields.append(
                StructField(
                    col_def['name'],
                    get_spark_type(col_def['type']),
                    col_def.get('nullable', True)
                )
            )
            
    print("  Đọc schema thành công.")
    return StructType(fields)

# --- HÀM LOGIC CHÍNH ---
def load_static_bronze(spark: SparkSession, landing_file_path: str, bronze_table_name: str, schema_obj: StructType):
    """
    Load static CSV file vào Bronze table với OVERWRITE mode.
    Dùng cho reference/lookup data không thay đổi (geolocation, product_translation).
    
    Khác với bronze.py (Auto Loader):
    - Dùng spark.read.csv() thay vì readStream
    - Mode OVERWRITE thay vì APPEND
    - Không cần checkpoint
    - Có conditional check: skip nếu table đã có data
    """
    print(f"Kiểm tra table: {bronze_table_name}")
    
    # ⭐️ Conditional check: Skip nếu table đã có data
    if spark.catalog.tableExists(bronze_table_name):
        existing_count = spark.table(bronze_table_name).count()
        if existing_count > 0:
            print(f"  Table đã tồn tại với {existing_count} rows.")
            print(f"  SKIP loading - Static data không thay đổi.")
            return  # Exit sớm, không load lại
    
    # Nếu table chưa có hoặc rỗng → Load data
    print(f"Bắt đầu Load Static Data (Batch Mode):")
    print(f"  ĐỌC TỪ: {landing_file_path}")
    print(f"  GHI VÀO: {bronze_table_name}")
    
    df = (spark.read
        .option("header", "true")
        .option("multiLine", "true")
        .option("escape", "\"")
        .schema(schema_obj)
        .csv(landing_file_path)
        .withColumn("_source_file", lit(landing_file_path))
        .withColumn("_ingest_timestamp", current_timestamp())
    )
    
    row_count = df.count()
    print(f"  Số dòng đọc được: {row_count}")
    
    df.write.mode("overwrite").saveAsTable(bronze_table_name)
    
    print(f"Hoàn thành nạp dữ liệu vào bảng: {bronze_table_name}")

# --- ĐIỂM BẮT ĐẦU CHẠY (ENTRYPOINT) ---
if __name__ == "__main__":
    
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    
    # Print all resolved configuration values for debugging
    print("--- Configuration Loaded from .env ---")
    print(f"  CATALOG:        {CATALOG}")
    print(f"  BRONZE_SCHEMA:  {BRONZE_SCHEMA}")
    print(f"  STAGING_SCHEMA: {STAGING_SCHEMA}")
    print(f"  LANDING_VOLUME: {LANDING_VOLUME}")
    print("--------------------------------------")
    
    # Đọc parameters từ widgets
    source_dir_name = dbutils.widgets.get("source_dir_name")
    source_file_name = dbutils.widgets.get("source_file_name")
    target_table_name = dbutils.widgets.get("target_table_name")
    schema_base_path = dbutils.widgets.getArgument("schema_base_path", "/Workspace/${workspace.file_path}/src/schemas")
    print(f"  Schema base path: {schema_base_path}")
    
    loaded_schema = load_schema_from_json(target_table_name, schema_base_path)
    
    landing_file_path = f"/Volumes/{CATALOG}/{STAGING_SCHEMA}/{LANDING_VOLUME}/{source_dir_name}/{source_file_name}"
    bronze_table_full_name = f"{CATALOG}.{BRONZE_SCHEMA}.{target_table_name}"
    
    print("--- Cấu hình Task ---")
    print(f"  Source Dir (Param):     {source_dir_name}")
    print(f"  Source File (Param):    {source_file_name}")
    print(f"  Target Table (Param):   {target_table_name}")
    print(f"  Landing File Path:      {landing_file_path}")
    print(f"  Bronze Table Full Name: {bronze_table_full_name}")
    print("-------------------------")
    
    load_static_bronze(spark, landing_file_path, bronze_table_full_name, loaded_schema)
