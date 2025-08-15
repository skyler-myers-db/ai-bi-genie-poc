-- ============================
-- BRONZE (Lakeflow or DLT SQL) with Auto Loader
-- Minimal: inference + schemaHints for the few tricky columns
-- ============================
-- DIMENSIONS
USE SCHEMA bronze;

CREATE OR REFRESH STREAMING TABLE dim_product_bz
  CLUSTER BY AUTO AS
SELECT
  *
FROM
  STREAM READ_FILES(
    '/Volumes/genie_poc/raw/seed/dim_product',
    format => 'parquet',
    recursiveFileLookup => TRUE,
    includeExistingFiles => TRUE
  );

CREATE OR REFRESH STREAMING TABLE dim_region_bz
  CLUSTER BY AUTO AS
SELECT
  *
FROM
  STREAM READ_FILES(
    '/Volumes/genie_poc/raw/seed/dim_region',
    format => 'parquet',
    recursiveFileLookup => TRUE,
    includeExistingFiles => TRUE
  );

CREATE OR REFRESH STREAMING TABLE dim_channel_bz
  CLUSTER BY AUTO AS
SELECT
  *
FROM
  STREAM READ_FILES(
    '/Volumes/genie_poc/raw/seed/dim_channel',
    format => 'parquet',
    recursiveFileLookup => TRUE,
    includeExistingFiles => TRUE
  );

CREATE OR REFRESH STREAMING TABLE dim_mill_bz
  CLUSTER BY AUTO AS
SELECT
  *
FROM
  STREAM READ_FILES(
    '/Volumes/genie_poc/raw/seed/dim_mill',
    format => 'parquet',
    recursiveFileLookup => TRUE,
    includeExistingFiles => TRUE
  );

CREATE OR REFRESH STREAMING TABLE dim_customer_bz
  CLUSTER BY AUTO AS
SELECT
  *
FROM
  STREAM READ_FILES(
    '/Volumes/genie_poc/raw/seed/dim_customer',
    format => 'parquet',
    recursiveFileLookup => TRUE,
    includeExistingFiles => TRUE
  );

-- 🔧 Fix: date is a Parquet DATE (INT32) => keep as DATE in bronze
CREATE OR REFRESH STREAMING TABLE dim_calendar_bz AS
SELECT
  *
FROM
  STREAM READ_FILES(
    '/Volumes/genie_poc/raw/seed/dim_calendar',
    format => 'parquet',
    recursiveFileLookup => TRUE,
    includeExistingFiles => TRUE,
    schemaHints => 'date DATE'
  );

-- FACTS
CREATE OR REFRESH STREAMING TABLE fact_sales_bz AS
SELECT
  *
FROM
  STREAM READ_FILES(
    '/Volumes/genie_poc/raw/seed/fact_sales',
    format => 'parquet',
    recursiveFileLookup => TRUE,
    includeExistingFiles => TRUE,
    -- keep numeric flag in bronze; cast to BOOLEAN later
    schemaHints =>
      'on_time_flag BIGINT'
  );

CREATE OR REFRESH STREAMING TABLE fact_inventory_bz
  CLUSTER BY AUTO AS
SELECT
  *
FROM
  STREAM READ_FILES(
    '/Volumes/genie_poc/raw/seed/fact_inventory',
    format => 'parquet',
    recursiveFileLookup => TRUE,
    includeExistingFiles => TRUE
  );

CREATE OR REFRESH STREAMING TABLE fact_production_bz
  CLUSTER BY AUTO AS
SELECT
  *
FROM
  STREAM READ_FILES(
    '/Volumes/genie_poc/raw/seed/fact_production',
    format => 'parquet',
    recursiveFileLookup => TRUE,
    includeExistingFiles => TRUE
  );

CREATE OR REFRESH STREAMING TABLE fact_logistics_bz
  CLUSTER BY AUTO AS
SELECT
  *
FROM
  STREAM READ_FILES(
    '/Volumes/genie_poc/raw/seed/fact_logistics',
    format => 'parquet',
    recursiveFileLookup => TRUE,
    includeExistingFiles => TRUE
  );