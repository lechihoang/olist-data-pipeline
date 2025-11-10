# Tên file: upload.py
# Mục đích: Tự động upload TẤT CẢ file .csv từ thư mục local 
#          lên Databricks Volume.

import os
from databricks.sdk import WorkspaceClient

LOCAL_DATA_FOLDER = "data"

CATALOG = "olist_project"
SCHEMA = "staging"
VOLUME = "landing_zone"

def upload_file_to_volume(w: WorkspaceClient, local_path: str, volume_path: str):
    """
    Sử dụng Databricks SDK để upload file lên Volume.
    """
    print(f"  Uploading '{os.path.basename(local_path)}' to '{volume_path}'...")
    
    try:
        with open(local_path, "rb") as f:
            w.dbfs.upload(
                path=volume_path,
                src=f,
                overwrite=True  # Ghi đè nếu file đã tồn tại
            )
        print(f"  ✅ Upload thành công!")
        
    except Exception as e:
        print(f"  ❌ Lỗi upload file: {e}")

if __name__ == "__main__":
    print(f"Đang kết nối với Databricks workspace...")
    w = WorkspaceClient()
    current_user = w.current_user.me().user_name
    print(f"Kết nối thành công với tư cách: {current_user}\n") 

    print(f"Bắt đầu quét thư mục '{LOCAL_DATA_FOLDER}' để tìm file .csv...")
    
    file_count = 0
    for filename in os.listdir(LOCAL_DATA_FOLDER):

        if filename.endswith(".csv"):
            file_count += 1
            
            local_file_path = os.path.join(LOCAL_DATA_FOLDER, filename)
            
            volume_file_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{filename}"

            upload_file_to_volume(w, local_file_path, volume_file_path)
        print(f"\nHoàn tất! Đã upload {file_count} file.")