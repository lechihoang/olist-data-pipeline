"""
Script upload file CSV lên Databricks Unity Catalog Volume.

Usage:
    python script/upload.py
"""

import os
from databricks.sdk import WorkspaceClient

# Configuration
LOCAL_DATA_FOLDER = "data"

CATALOG = "olist_project"
STAGING_SCHEMA = "staging"
LANDING_VOLUME = "landing_zone"

# File mapping: local filename -> target subfolder trong Volume
FILE_MAPPING = {
    # Dynamic data
    "orders.csv": "orders",
    "customers.csv": "customers",
    "order_items.csv": "order_items",
    "order_payments.csv": "order_payments",
    "order_reviews.csv": "order_reviews",
    "products.csv": "products",
    "sellers.csv": "sellers",
    # Static data
    "geolocation.csv": "static",
    "product_category_name_translation.csv": "static"
}


def upload_file_to_volume(w: WorkspaceClient, local_path: str, volume_path: str):
    """Upload một file lên Databricks Volume"""
    filename = os.path.basename(local_path)
    full_volume_path = f"{volume_path}/{filename}"

    print(f"  Uploading '{filename}' to '{full_volume_path}'...")
    
    try:
        with open(local_path, "rb") as f:
            w.files.upload(
                file_path=full_volume_path,
                contents=f,
                overwrite=True
            )
        print(f"  ✅ Upload thành công!")
        return True
        
    except Exception as e:
        print(f"  ❌ Lỗi upload file: {e}")
        return False


def main():
    print("="*50)
    print("  UPLOADING DATA FILES TO DATABRICKS")
    print("="*50)
    print()
    
    print("Đang kết nối với Databricks workspace...")
    w = WorkspaceClient()
    current_user = w.current_user.me().user_name
    print(f"Kết nối thành công với tư cách: {current_user}\n")
    
    file_count = 0
    for filename, target_dir in FILE_MAPPING.items():
        local_file_path = os.path.join(LOCAL_DATA_FOLDER, filename)
        
        if os.path.exists(local_file_path):
            target_volume_path = f"/Volumes/{CATALOG}/{STAGING_SCHEMA}/{LANDING_VOLUME}/{target_dir}"
            if upload_file_to_volume(w, local_file_path, target_volume_path):
                file_count += 1
        else:
            print(f"  ⚠️ File không tồn tại: {local_file_path}")
    
    print()
    print("="*50)
    print(f"  HOÀN TẤT! Đã upload {file_count}/{len(FILE_MAPPING)} file")
    print("="*50)


if __name__ == "__main__":
    main()
