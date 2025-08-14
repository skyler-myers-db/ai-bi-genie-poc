USE CATALOG genie_poc;
USE SCHEMA bronze;

-- Dims
CREATE TABLE IF NOT EXISTS dim_product_bronze
USING DELTA CLUSTER BY AUTO
COMMENT 'Bronze: dim_product'
AS SELECT * FROM parquet.`/Volumes/genie_poc/raw/seed/dim_product`;

CREATE TABLE IF NOT EXISTS dim_region_bronze
USING DELTA CLUSTER BY AUTO AS SELECT * FROM parquet.`/Volumes/genie_poc/raw/seed/dim_region`;

CREATE TABLE IF NOT EXISTS dim_channel_bronze
USING DELTA CLUSTER BY AUTO AS SELECT * FROM parquet.`/Volumes/genie_poc/raw/seed/dim_channel`;

CREATE TABLE IF NOT EXISTS dim_mill_bronze
USING DELTA CLUSTER BY AUTO AS SELECT * FROM parquet.`/Volumes/genie_poc/raw/seed/dim_mill`;

CREATE TABLE IF NOT EXISTS dim_customer_bronze
USING DELTA CLUSTER BY AUTO AS SELECT * FROM parquet.`/Volumes/genie_poc/raw/seed/dim_customer`;

CREATE TABLE IF NOT EXISTS dim_calendar_bronze
USING DELTA CLUSTER BY AUTO AS SELECT * FROM parquet.`/Volumes/genie_poc/raw/seed/dim_calendar`;

-- Facts
CREATE TABLE IF NOT EXISTS fact_sales_bronze
USING DELTA
CLUSTER BY (order_date_key)
AS SELECT * FROM parquet.`/Volumes/genie_poc/raw/seed/fact_sales`;

CREATE TABLE IF NOT EXISTS fact_inventory_bronze
USING DELTA CLUSTER BY AUTO AS SELECT * FROM parquet.`/Volumes/genie_poc/raw/seed/fact_inventory`;

CREATE TABLE IF NOT EXISTS fact_production_bronze
USING DELTA CLUSTER BY AUTO AS SELECT * FROM parquet.`/Volumes/genie_poc/raw/seed/fact_production`;

CREATE TABLE IF NOT EXISTS fact_logistics_bronze
USING DELTA CLUSTER BY AUTO AS SELECT * FROM parquet.`/Volumes/genie_poc/raw/seed/fact_logistics`;
