-- Tên file: setup.sql
-- Mục đích: Khởi tạo toàn bộ hạ tầng (Catalog, Schema, Volume) 
--          VÀ cấp quyền (GRANT) cho pipeline.
-- -------------------------------------------------------------

-- 1. TẠO "KHO" (CATALOG) CHÍNH CHO DỰ ÁN
CREATE CATALOG IF NOT EXISTS olist_project;

-- 2. CHUYỂN SANG DÙNG "KHO" ĐÓ
USE CATALOG olist_project;

-- 3. TẠO CÁC "NGĂN" (SCHEMAS)
CREATE SCHEMA IF NOT EXISTS bronze
    COMMENT 'Nơi chứa dữ liệu thô, chưa qua xử lý (raw)';

CREATE SCHEMA IF NOT EXISTS silver
    COMMENT 'Nơi chứa dữ liệu đã làm sạch, hợp nhất (cleansed)';

CREATE SCHEMA IF NOT EXISTS staging
    COMMENT 'Nơi chứa các đối tượng tạm như Volumes, checkpoints';

-- 4. TẠO "KHU VỰC UPLOAD" (VOLUME)
CREATE VOLUME IF NOT EXISTS staging.landing_zone
    COMMENT 'Nơi chứa file CSV thô từ Olist (vùng đệm an toàn)';

-- -------------------------------------------------------------
-- PHẦN 5: CẤP QUYỀN (GRANT PERMISSIONS)
-- -------------------------------------------------------------
-- Chúng ta cấp quyền cho 'account users' (tất cả người dùng trong tài khoản,
-- bao gồm cả "Job" của bạn) để họ có thể chạy pipeline.

-- Quyền trên "Kho" (Catalog)
GRANT USE_CATALOG ON CATALOG olist_project TO `account users`;

-- Quyền trên "Ngăn" Staging
GRANT USE_SCHEMA ON SCHEMA olist_project.staging TO `account users`;

-- Quyền trên "Volume" (Rất quan trọng)
GRANT READ VOLUME ON VOLUME olist_project.staging.landing_zone TO `account users`;
GRANT WRITE VOLUME ON VOLUME olist_project.staging.landing_zone TO `account users`; -- Cần cho 'bronze.py' ghi checkpoint

-- Quyền trên "Ngăn" Bronze
GRANT USE_SCHEMA ON SCHEMA olist_project.bronze TO `account users`;
GRANT CREATE TABLE ON SCHEMA olist_project.bronze TO `account users`; -- Cho phép 'bronze.py' tạo bảng

-- Quyền trên "Ngăn" Silver
GRANT USE_SCHEMA ON SCHEMA olist_project.silver TO `account users`;
GRANT CREATE TABLE ON SCHEMA olist_project.silver TO `account users`; -- Cho phép 'silver.py' tạo bảng