# Databricks notebook source
# ⬆️ DÒNG NÀY RẤT QUAN TRỌNG, HÃY GIỮ NÓ

# Tên file: src/tasks/silver/silver_order_reviews.py
# --------------------
# ⚠️ CHẠY FILE NÀY TRÊN DATABRICKS JOB
# --------------------
# (Phiên bản đã SỬA LỖI: Dùng 'try_to_timestamp' để xử lý data "bẩn")

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, try_to_timestamp # ⭐️ THAY ĐỔI: Dùng 'try_to_timestamp'
from delta.tables import DeltaTable
# (Không cần import DBUtils)

# --- HÀM LOGIC CHÍNH (Giữ nguyên) ---
def process_silver_order_reviews(spark: SparkSession, bronze_table_name: str, silver_table_name: str, checkpoint_path: str):

    print(f"Bắt đầu xử lý Silver (Batch mode):")
    print(f"  ĐỌC TỪ: {bronze_table_name}")
    print(f"  GHI VÀO: {silver_table_name}")

    bronze_df = spark.readStream.table(bronze_table_name)
    
    # Định dạng (format)

    # 2. Logic "Làm sạch" (Transform)
    silver_df = (bronze_df
        .select(
            col("review_id").cast("string"),
            col("order_id").cast("string"),
            col("review_score").cast("integer"),
            col("review_comment_title").cast("string"),
            col("review_comment_message").cast("string"),
            
            # ⭐️ SỬA LỖI: Dùng 'try_to_timestamp' (an toàn)
            # Nếu gặp text bẩn, nó sẽ trả về NULL thay vì crash
            try_to_timestamp(col("review_creation_date"), "yyyy-MM-dd HH:mm:ss").alias("review_creation_date"),
            try_to_timestamp(col("review_answer_timestamp"), "yyyy-MM-dd HH:mm:ss").alias("review_answer_timestamp")
        )
        .where("review_id IS NOT NULL AND order_id IS NOT NULL")
    )

    # 3. Hàm để chạy logic MERGE (Upsert) - (Giữ nguyên)
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
                "s.review_id = b.review_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    # 4. Bắt đầu stream (Giữ nguyên)
    (silver_df.writeStream
        .foreachBatch(upsert_to_silver)
        .outputMode("update")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )
    print(f"Hoàn thành xử lý Bảng Silver: {silver_table_name}")

# --- ĐIỂM BẮT ĐẦU CHẠY (ENTRYPOINT) ---
if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    # (Không cần khởi tạo DBUtils)
    
    bronze_table_input = dbutils.widgets.get("bronze_table_input")
    silver_table_output = dbutils.widgets.get("silver_table_output")
    
    CATALOG = "olist_project"
    BRONZE_SCHEMA = "bronze"
    SILVER_SCHEMA = "silver"
    STAGING_SCHEMA = "staging"

    bronze_table_full_name = f"{CATALOG}.{BRONZE_SCHEMA}.{bronze_table_input}"
    silver_table_full_name = f"{CATALOG}.{SILVER_SCHEMA}.{silver_table_output}"
    checkpoint_path = f"/Volumes/{CATALOG}/{STAGING_SCHEMA}/checkpoints/{silver_table_output}"

    process_silver_order_reviews(spark, bronze_table_full_name, silver_table_full_name, checkpoint_path)