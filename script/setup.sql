CREATE CATALOG IF NOT EXISTS olist_project;

USE CATALOG olist_project;

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE SCHEMA IF NOT EXISTS silver;

CREATE SCHEMA IF NOT EXISTS gold;

CREATE SCHEMA IF NOT EXISTS staging;

CREATE VOLUME IF NOT EXISTS staging.lakehouse;

CREATE VOLUME IF NOT EXISTS staging.checkpoints;

GRANT USE_CATALOG ON CATALOG olist_project TO `account users`;

GRANT USE_SCHEMA ON SCHEMA olist_project.staging TO `account users`;
GRANT READ VOLUME ON VOLUME olist_project.staging.lakehouse TO `account users`;
GRANT WRITE VOLUME ON VOLUME olist_project.staging.lakehouse TO `account users`;
GRANT READ VOLUME ON VOLUME olist_project.staging.checkpoints TO `account users`;
GRANT WRITE VOLUME ON VOLUME olist_project.staging.checkpoints TO `account users`;

GRANT USE_SCHEMA ON SCHEMA olist_project.bronze TO `account users`;
GRANT CREATE TABLE ON SCHEMA olist_project.bronze TO `account users`;

GRANT USE_SCHEMA ON SCHEMA olist_project.silver TO `account users`;
GRANT CREATE TABLE ON SCHEMA olist_project.silver TO `account users`;

GRANT USE_SCHEMA ON SCHEMA olist_project.gold TO `account users`;
GRANT CREATE TABLE ON SCHEMA olist_project.gold TO `account users`;
