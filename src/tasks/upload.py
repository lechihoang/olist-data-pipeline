import os
from databricks.sdk import WorkspaceClient

LOCAL_DATA_FOLDER = "data"

CATALOG = "olist_project"
STAGING_SCHEMA = "staging"
LANDING_VOLUME = "landing_zone"

FILE_TO_DIR_MAPPING = {
    "olist_orders_dataset.csv": "orders",
    "olist_customers_dataset.csv": "customers",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "olist_geolocation_dataset.csv": "geolocation",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "product_category_name_translation.csv": "product_category_name_translation"
}

def upload_file_to_volume(w: WorkspaceClient, local_path: str, volume_path: str):
    filename = os.path.basename(local_path)
    full_volume_path = f"{volume_path}/{filename}"

    print(f"  Uploading '{filename}' to '{full_volume_path}'...")
    
    try:
        with open(local_path, "rb") as f:
            w.dbfs.upload(
                path=full_volume_path,
                src=f,
                overwrite=True
            )
        print(f"  ✅ Upload thành công!")
        
    except Exception as e:
        print(f"  ❌ Lỗi upload file: {e}")

if __name__ == "__main__":
    
    print(f"Đang kết nối với Databricks workspace...")
    
    w = WorkspaceClient()
    current_user = w.current_user.me().user_name
    print(f"Kết nối thành công với tư cách: {current_user}\n")

    print(f"Bắt đầu quét và upload file từ '{LOCAL_DATA_FOLDER}'...")
    
    file_count = 0
    for filename, target_dir in FILE_TO_DIR_MAPPING.items():
        
        local_file_path = os.path.join(LOCAL_DATA_FOLDER, filename)
        
        if os.path.exists(local_file_path):
            file_count += 1
            
            target_volume_path = f"/Volumes/{CATALOG}/{STAGING_SCHEMA}/{LANDING_VOLUME}/{target_dir}"
            
            upload_file_to_volume(w, local_file_path, target_volume_path)

    print(f"\nHoàn tất! Đã upload {file_count} file.")