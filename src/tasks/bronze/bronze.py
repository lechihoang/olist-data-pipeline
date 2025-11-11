# Databricks notebook source
# Tên file: src/tasks/bronze.py
# --------------------
# ⚠️ CHẠY FILE NÀY TRÊN DATABRICKS JOB
# --------------------
# (Phiên bản đã sửa lỗi 'LANDING_VOLUME')

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name
from pyspark.dbutils import DBUtils


# --- HÀM LOGIC CHÍNH (TÁI SỬ DỤNG) ---
def load_bronze(spark: SparkSession, landing_volume_path: str, bronze_table_name: str, checkpoint_path: str):
    """
    Sử dụng Auto Loader (cloudFiles) để nạp dữ liệu từ landing_volume_path
    vào bảng Bronze (bronze_table_name) theo chế độ batch.
    """
    print(f"Bắt đầu Auto Loader (Chế độ Batch):")
    print(f"  ĐỌC TỪ: {landing_volume_path}")
    print(f"  GHI VÀO: {bronze_table_name}")
    
    # 1. Đọc Stream (sử dụng Auto Loader)
    (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")        # File CSV có header
        .option("inferSchema", "true")   # Tự động đoán kiểu dữ liệu
        .option("cloudFiles.schemaLocation", checkpoint_path) # Nơi lưu trạng thái
        .load(landing_volume_path)       # Đường dẫn đến thư mục con (vd: .../orders/)
        
        # 2. Thêm cột metadata
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingest_timestamp", current_timestamp())
        
        # 3. Ghi Stream (ở chế độ Batch)
        .writeStream
        .format("delta")
        .outputMode("append")          # Nối dữ liệu mới
        .option("checkpointLocation", checkpoint_path) # Dùng checkpoint 2 nơi
        .option("mergeSchema", "true") # Tự động thêm cột nếu CSV có cột mới
        .trigger(availableNow=True)  # <-- BẮT BUỘC: Biến nó thành Batch Job
        .toTable(bronze_table_name)  # Ghi vào bảng Unity Catalog
    )
    
    print(f"Hoàn thành nạp dữ liệu vào bảng: {bronze_table_name}")

# --- ĐIỂM BẮT ĐẦU CHẠY (ENTRYPOINT) ---
if __name__ == "__main__":
    
    # 1. Khởi tạo Spark và DBUtils
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    
    if not dbutils:
        print("Lỗi: Không thể khởi tạo dbutils. Script này phải chạy như một Databricks Job.")
        exit() # Thoát nếu không chạy trên Job
        
    # 2. Lấy tham số (widget) được truyền từ Databricks Job
    
    # Tên thư mục con trong landing_zone (vd: "orders", "customers")
    source_dir_name = dbutils.widgets.get("source_dir_name")
    
    # Tên bảng Bronze muốn ghi vào (vd: "orders_raw", "customers_raw")
    target_table_name = dbutils.widgets.get("target_table_name")
    
    # 3. Định nghĩa các đường dẫn cơ sở (Dùng Unity Catalog)
    CATALOG = "olist_project"
    STAGING_SCHEMA = "staging"
    BRONZE_SCHEMA = "bronze"
    LANDING_VOLUME = "landing_zone" # ⭐️ <--- DÒNG BỊ THIẾU ĐÃ ĐƯỢC THÊM VÀO

    # 4. Xây dựng các đường dẫn đầy đủ dựa trên tham số
    
    # Đường dẫn nguồn (nơi file CSV được upload)
    # Ví dụ: /Volumes/olist_project/staging/landing_zone/orders
    landing_volume_path = f"/Volumes/{CATALOG}/{STAGING_SCHEMA}/{LANDING_VOLUME}/{source_dir_name}"
    
    # Tên bảng đích (tên 3 cấp của Unity Catalog)
    # Ví dụ: olist_project.bronze.orders_raw
    bronze_table_full_name = f"{CATALOG}.{BRONZE_SCHEMA}.{target_table_name}"
    
    # Đường dẫn Checkpoint (để Auto Loader theo dõi file đã đọc)
    # Ví dụ: /Volumes/olist_project/staging/checkpoints/orders_raw
    checkpoint_path = f"/Volumes/{CATALOG}/{STAGING_SCHEMA}/checkpoints/{target_table_name}"
    
    # 5. In ra để kiểm tra (log của Job)
    print("--- Cấu hình Task ---")
    print(f"  Source Dir (Param):     {source_dir_name}")
    print(f"  Target Table (Param):   {target_table_name}")
    print(f"  Landing Volume Path:  {landing_volume_path}")
    print(f"  Bronze Table Full Name: {bronze_table_full_name}")
    print(f"  Checkpoint Path:      {checkpoint_path}")
    print("-------------------------")
    
    # 6. Gọi hàm logic chính
    load_bronze(spark, landing_volume_path, bronze_table_full_name, checkpoint_path)