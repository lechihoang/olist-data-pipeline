CREATE CATALOG IF NOT EXISTS olist_project;

USE CATALOG olist_project;

CREATE SCHEMA IF NOT EXISTS bronze
    COMMENT 'Nơi chứa dữ liệu thô, chưa qua xử lý (raw)';

CREATE SCHEMA IF NOT EXISTS silver
    COMMENT 'Nơi chứa dữ liệu đã làm sạch, hợp nhất (cleansed)';

CREATE SCHEMA IF NOT EXISTS staging
    COMMENT 'Nơi chứa các đối tượng tạm như Volumes, checkpoints';

-- 4. TẠO "KHU VỰC UPLOAD" (VOLUME)
CREATE VOLUME IF NOT EXISTS staging.landing_zone
    COMMENT 'Nơi chứa file CSV thô từ Olist (vùng đệm an toàn)';

CREATE VOLUME IF NOT EXISTS olist_project.staging.checkpoints
COMMENT 'Nơi chứa các file checkpoint cho Auto Loader và Streams';

GRANT USE_CATALOG ON CATALOG olist_project TO `account users`;

GRANT USE_SCHEMA ON SCHEMA olist_project.staging TO `account users`;

GRANT READ VOLUME ON VOLUME olist_project.staging.landing_zone TO `account users`;
GRANT WRITE VOLUME ON VOLUME olist_project.staging.landing_zone TO `account users`;

GRANT USE_SCHEMA ON SCHEMA olist_project.bronze TO `account users`;
GRANT CREATE TABLE ON SCHEMA olist_project.bronze TO `account users`;

GRANT USE_SCHEMA ON SCHEMA olist_project.silver TO `account users`;
GRANT CREATE TABLE ON SCHEMA olist_project.silver TO `account users`;