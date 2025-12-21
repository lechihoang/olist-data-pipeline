# Databricks notebook source
# ⬆️ DÒNG NÀY RẤT QUAN TRỌNG, HÃY GIỮ NÓ

# Tên file: src/tasks/bronze/bronze.py
# --------------------
# ⚠️ CHẠY FILE NÀY TRÊN DATABRICKS JOB
# --------------------
# (Phiên bản "Sửa lỗi Path": Dùng đường dẫn "Tuyệt đối" (hard-coded)
#  và "Sửa lỗi Syntax": Dùng dấu '.' cho tên bảng)

import sys
import json
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.dbutils import DBUtils
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType

# --- LOAD CONFIGURATION FROM .env ---
# Load .env file từ root folder (relative to workspace)
env_path = "/Workspace/${workspace.file_path}/.env"
load_dotenv(env_path)

# Configuration với fallback defaults
CATALOG = os.getenv("CATALOG", "olist_project")
BRONZE_SCHEMA = os.getenv("BRONZE_SCHEMA", "bronze")
SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "silver")
STAGING_SCHEMA = os.getenv("STAGING_SCHEMA", "staging")
LANDING_VOLUME = os.getenv("LANDING_VOLUME", "landing_zone")

# --- HÀM HỖ TRỢ 1 (Giữ nguyên) ---
def get_spark_type(type_name: str):
    type_map = {
        "string": StringType(),
        "integer": IntegerType(),
        "double": DoubleType(),
        "timestamp": TimestampType(),
        "boolean": BooleanType()
    }
    return type_map.get(type_name.lower(), StringType()) 

# --- HÀM HỖ TRỢ 2 (Dynamic Schema Path) ---
def load_schema_from_json(table_name: str, schema_base_path: str) -> StructType:
    """
    Load schema từ JSON file với configurable base path.
    
    Args:
        table_name: Tên bảng (sẽ được dùng làm tên file JSON)
        schema_base_path: Đường dẫn thư mục chứa schema files
        
    Returns:
        StructType schema object
        
    Raises:
        FileNotFoundError: Khi schema file không tồn tại tại đường dẫn đã cấu hình
    """
    schema_path = f"{schema_base_path}/{table_name}.json"
    
    print(f"  Đang đọc schema từ: {schema_path}")
    
    # Kiểm tra file tồn tại và raise error với message rõ ràng
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

# --- HÀM LOGIC CHÍNH (Giữ nguyên) ---
def load_bronze(spark: SparkSession, landing_volume_path: str, bronze_table_name: str, checkpoint_path: str, schema_obj: StructType):
    
    print(f"Bắt đầu Auto Loader (Chế độ Batch - Dùng Schema tường minh):")
    print(f"  ĐỌC TỪ: {landing_volume_path}")
    print(f"  GHI VÀO: {bronze_table_name}") # ⭐️ Tên đúng sẽ được in ra ở đây
    
    (spark.readStream
        # ... (code .format, .option, .schema... giữ nguyên) ...
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
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
        .toTable(bronze_table_name) # ⭐️ Ghi vào tên đúng
    )
    
    print(f"Hoàn thành nạp dữ liệu vào bảng: {bronze_table_name}")

# --- ĐIỂM BẮT ĐẦU CHẠY (ENTRYPOINT) ---
if __name__ == "__main__":
    
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark) 
    
    # Print all resolved configuration values for debugging (Requirement 1.6)
    print("--- Configuration Loaded from .env ---")
    print(f"  CATALOG:        {CATALOG}")
    print(f"  BRONZE_SCHEMA:  {BRONZE_SCHEMA}")
    print(f"  SILVER_SCHEMA:  {SILVER_SCHEMA}")
    print(f"  STAGING_SCHEMA: {STAGING_SCHEMA}")
    print(f"  LANDING_VOLUME: {LANDING_VOLUME}")
    print("--------------------------------------")
        
    source_dir_name = dbutils.widgets.get("source_dir_name")
    target_table_name = dbutils.widgets.get("target_table_name")
    
    # ⭐️ Đọc schema_base_path từ widget, sử dụng default nếu không được cung cấp
    try:
        schema_base_path = dbutils.widgets.get("schema_base_path")
        print(f"  Sử dụng schema_base_path từ widget: {schema_base_path}")
    except Exception:
        # Default: relative path từ bundle deployment location
        schema_base_path = "/Workspace/${workspace.file_path}/src/schemas"
        print(f"  Widget schema_base_path không được cung cấp, sử dụng default: {schema_base_path}")
    
    loaded_schema = load_schema_from_json(target_table_name, schema_base_path)

    landing_volume_path = f"/Volumes/{CATALOG}/{STAGING_SCHEMA}/{LANDING_VOLUME}/{source_dir_name}"
    
    # ⭐️ SỬA LỖI CÚ PHÁP: Đổi '/' thành '.'
    bronze_table_full_name = f"{CATALOG}.{BRONZE_SCHEMA}.{target_table_name}"
    
    checkpoint_path = f"/Volumes/{CATALOG}/{STAGING_SCHEMA}/checkpoints/{target_table_name}"
    
    print("--- Cấu hình Task ---")
    print(f"  Source Dir (Param):     {source_dir_name}")
    print(f"  Target Table (Param):   {target_table_name}")
    print(f"  Landing Volume Path:  {landing_volume_path}")
    print(f"  Bronze Table Full Name: {bronze_table_full_name}") # ⭐️ Sẽ in ra tên đúng
    print(f"  Checkpoint Path:      {checkpoint_path}")
    print("-------------------------")
    
    load_bronze(spark, landing_volume_path, bronze_table_full_name, checkpoint_path, loaded_schema)