import os
import logging
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

LOCAL_DATA_FOLDER = os.getenv("DATA_FOLDER", "data")

CATALOG = os.getenv("CATALOG", "olist_project")
STAGING_SCHEMA = os.getenv("STAGING_SCHEMA", "staging")
LANDING_VOLUME = os.getenv("LANDING_VOLUME", "landing_zone")

FILE_MAPPING = {
    "order.csv": "order",
    "customer.csv": "customer",
    "order_item.csv": "order_item",
    "order_payment.csv": "order_payment",
    "order_review.csv": "order_review",
    "product.csv": "product",
    "seller.csv": "seller",
    "geolocation.csv": "static",
    "product_category_name_translation.csv": "static"
}


def upload_file_to_volume(w: WorkspaceClient, local_path: str, volume_path: str):
    filename = os.path.basename(local_path)
    full_volume_path = f"{volume_path}/{filename}"

    logger.info(f"  Uploading '{filename}' to '{full_volume_path}'...")
    
    try:
        with open(local_path, "rb") as f:
            w.files.upload(
                file_path=full_volume_path,
                contents=f,
                overwrite=True
            )
        logger.info(f"  Upload successful!")
        return True
        
    except Exception as e:
        logger.error(f"  Upload failed: {e}")
        return False


def main():
    logger.info("=" * 50)
    logger.info("  UPLOADING DATA FILES TO DATABRICKS")
    logger.info("=" * 50)
    
    logger.info("Connecting to Databricks workspace...")
    w = WorkspaceClient()
    current_user = w.current_user.me().user_name
    logger.info(f"Connected as: {current_user}")
    
    file_count = 0
    for filename, target_dir in FILE_MAPPING.items():
        local_file_path = os.path.join(LOCAL_DATA_FOLDER, filename)
        
        if os.path.exists(local_file_path):
            target_volume_path = f"/Volumes/{CATALOG}/{STAGING_SCHEMA}/{LANDING_VOLUME}/{target_dir}"
            if upload_file_to_volume(w, local_file_path, target_volume_path):
                file_count += 1
        else:
            logger.warning(f"  File not found: {local_file_path}")
    
    logger.info("=" * 50)
    logger.info(f"  COMPLETED! Uploaded {file_count}/{len(FILE_MAPPING)} files")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
