-- Lakeflow Declarative Pipelines (SQL) - curated dimensions & facts
-- This notebook/SQL is referenced by the pipeline in pipeline.dab.yml
-- Basic DQ checks with EXPECT and late-arriving join tolerance
USE SCHEMA silver;

CREATE OR REFRESH LIVE TABLE dim_product_silver
  CLUSTER BY AUTO
  COMMENT "Cleaned product dimension"
  TBLPROPERTIES ("quality" = "silver") AS
SELECT
  product_id,
  sku,
  family,
  grade,
  base_cost
FROM
  bronze.dim_product_bz
WHERE
  sku IS NOT NULL;

CREATE OR REFRESH LIVE TABLE dim_region_silver
  CLUSTER BY AUTO AS
SELECT
  *
FROM
  bronze.dim_region_bz;

CREATE OR REFRESH LIVE TABLE dim_channel_silver
  CLUSTER BY AUTO AS
SELECT
  *
FROM
  bronze.dim_channel_bz;

CREATE OR REFRESH LIVE TABLE dim_mill_silver
  CLUSTER BY AUTO AS
SELECT
  *
FROM
  bronze.dim_mill_bz;

CREATE OR REFRESH LIVE TABLE dim_customer_silver
  CLUSTER BY AUTO AS
SELECT
  customer_id,
  customer_name,
  segment
FROM
  bronze.dim_customer_bz;

CREATE OR REFRESH LIVE TABLE dim_calendar_silver
  CLUSTER BY AUTO AS
SELECT
  *
FROM
  bronze.dim_calendar_bz;

-- SALES SILVER with basic conformance & referential checks
CREATE
OR REFRESH LIVE TABLE fact_sales_silver(
  CONSTRAINT valid_values EXPECT(
    qty > 0
    AND net_price >= 0
    AND cogs >= 0
  ) ON VIOLATION DROP ROW
) CLUSTER BY AUTO
TBLPROPERTIES (
  "quality" = "silver",
  'pipelines.expect' = 'qty > 0 AND net_price >= 0 AND cogs >= 0',
  'pipelines.expect.on_violation' = 'DROP'
) AS
SELECT
  s.order_line_id,
  s.order_date_key,
  s.ship_date_key,
  s.product_id,
  s.region_id,
  s.channel_id,
  s.mill_id,
  s.customer_id,
  s.qty,
  s.list_price,
  s.discount_pct,
  s.net_price,
  s.cogs,
  s.on_time_flag
FROM
  bronze.fact_sales_bz s -- late arriving dim tolerance; left joins in gold
;