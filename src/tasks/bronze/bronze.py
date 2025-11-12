# Databricks notebook source
# ⬆️ DÒNG NÀY RẤT QUAN TRỌNG, HÃY GIỮ NÓ

# Tên file: src/tasks/bronze/bronze.py
# --------------------
# ⚠️ CHẠY FILE NÀY TRÊN DATABRICKS JOB
# --------------------
# (Phiên bản "Sửa lỗi Path": Dùng 'os.path' để tìm đường dẫn tuyệt đối
#  từ vị trí của file này)

import sys
import json
import os # ⭐️ THÊM IMPORT NÀY
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.dbutils import DBUtils
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType

# --- HÀM HỖ TRỢ 1 (Giữ nguyên) ---
def get_spark_type(type_name: str):
    """
    Chuyển đổi tên kiểu (string) từ JSON thành đối tượng Kiểu (Type) của Spark.
    """
    type_map = {
        "string": StringType(),
        "integer": IntegerType(),
        "double": DoubleType(),
        "timestamp": TimestampType(),
        "boolean": BooleanType()
    }
    return type_map.get(type_name.lower(), StringType()) 

# --- HÀM HỖ TRỢ 2 (⭐️ ĐÃ SỬA LỖI PATH) ---
def load_schema_from_json(table_name: str) -> StructType:
    """
    Đọc file schema (ví dụ: 'orders_raw.json') 
    và biến nó thành một đối tượng StructType của Spark.
    (Sử dụng 'os.path' để tìm đường dẫn một cách an toàn).
    """
    
    # 1. Lấy đường dẫn ĐẦY ĐỦ của file 'bronze.py' này
    #    (ví dụ: /Workspace/Repos/user/repo/src/tasks/bronze/bronze.py)
    current_file_abs_path = os.path.abspath(__file__)
    
    # 2. Lấy thư mục 'src/' (là "cha" của "cha" của "cha" của file này)
    #    dirname(current_file_abs_path) -> .../src/tasks/bronze
    #    dirname(...) -> .../src/tasks
    #    dirname(...) -> .../src
    src_dir = os.path.dirname(os.path.dirname(os.path.dirname(current_file_abs_path)))
    
    # 3. "Đi xuống" thư mục 'schemas/'
    #    (ví dụ: /Workspace/Repos/user/repo/src/schemas/geolocation_raw.json)
    schema_path = os.path.join(src_dir, "schemas", f"{table_name}.json")
    
    print(f"  Đang đọc schema (tuyệt đối) từ: {schema_path}")
    
    fields = []
    
    # Mở file bằng đường dẫn tuyệt đối (sẽ tìm thấy)
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
    print(f"  GHI VÀO: {bronze_table_name}")
    
    (spark.readStream
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
        .toTable(bronze_table_name)
    )
    
    print(f"Hoàn thành nạp dữ liệu vào bảng: {bronze_table_name}")

# --- ĐIỂM BẮT ĐẦU CHẠY (Giữ nguyên) ---
if __name__ == "__main__":
    
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark) 
        
    source_dir_name = dbutils.widgets.get("source_dir_name")
    target_table_name = dbutils.widgets.get("target_table_name")
    
    loaded_schema = load_schema_from_json(target_table_name)
        
    CATALOG = "olist_project"
    STAGING_SCHEMA = "staging"
    BRONZE_SCHEMA = "bronze"
    LANDING_VOLUME = "landing_zone"

    landing_volume_path = f"/Volumes/{CATALOG}/{STAGING_SCHEMA}/{LANDING_VOLUME}/{source_dir_name}"
    bronze_table_full_name = f"{CATALOG}.{BRONZE_SCHEMA}.{target_table_name}"
    checkpoint_path = f"/Volumes/{CATALOG}/{STAGING_SCHEMA}/checkpoints/{target_table_name}"
    
    print("--- Cấu hình Task ---")
    print(f"  Source Dir (Param):     {source_dir_name}")
    print(f"  Target Table (Param):   {target_table_name}")
    print(f"  Landing Volume Path:  {landing_volume_path}")
    print(f"  Bronze Table Full Name: {bronze_table_full_name}")
    print(f"  Checkpoint Path:      {checkpoint_path}")
    print("-------------------------")
    
    load_bronze(spark, landing_volume_path, bronze_table_full_name, checkpoint_path, loaded_schema)