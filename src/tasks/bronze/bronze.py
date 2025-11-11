import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name

def get_dbutils(spark: SparkSession):
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        return dbutils
    except ImportError:
        print("Không thể khởi tạo dbutils. Script này có thể không chạy trên Databricks Job.")
        return None

def load_bronze(spark: SparkSession, landing_volume_path: str, bronze_table_name: str, checkpoint_path: str):
    print(f"Bắt đầu Auto Loader (Chế độ Batch):")
    print(f"  ĐỌC TỪ: {landing_volume_path}")
    print(f"  GHI VÀO: {bronze_table_name}")
    
    (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", checkpoint_path)
        .load(landing_volume_path)
        
        .withColumn("_source_file", input_file_name())
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

if __name__ == "__main__":
    
    spark = SparkSession.builder.getOrCreate()
    dbutils = get_dbutils(spark)
    
    if not dbutils:
        print("Lỗi: Không thể khởi tạo dbutils. Script này phải chạy như một Databricks Job.")
        exit()
        
    source_dir_name = dbutils.widgets.get("source_dir_name")
    
    target_table_name = dbutils.widgets.get("target_table_name")
    
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
    
    load_bronze(spark, landing_volume_path, bronze_table_full_name, checkpoint_path)