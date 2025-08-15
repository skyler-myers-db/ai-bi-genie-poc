USE CATALOG genie_poc;

USE SCHEMA gold;

-- Dimensions (Gold) from LIVE tables
CREATE OR REPLACE VIEW dim_product AS
SELECT
  *
FROM
  genie_poc.silver.dim_product_silver;

CREATE OR REPLACE VIEW dim_region AS
SELECT
  *
FROM
  genie_poc.silver.dim_region_silver;

CREATE OR REPLACE VIEW dim_channel AS
SELECT
  *
FROM
  genie_poc.silver.dim_channel_silver;

CREATE OR REPLACE VIEW dim_mill AS
SELECT
  *
FROM
  genie_poc.silver.dim_mill_silver;

CREATE OR REPLACE VIEW dim_customer AS
SELECT
  *
FROM
  genie_poc.silver.dim_customer_silver;

CREATE OR REPLACE VIEW dim_calendar AS
SELECT
  *
FROM
  genie_poc.silver.dim_calendar_silver;

COMMENT ON COLUMN fact_sales.net_sales IS 'net_price * qty in USD';

COMMENT ON COLUMN fact_sales.gross_margin_pct IS '(net_sales - cogs_total)/net_sales';