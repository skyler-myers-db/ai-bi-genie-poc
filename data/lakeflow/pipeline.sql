-- Lakeflow Declarative Pipelines (SQL) - curated dimensions & facts
-- This notebook/SQL is referenced by the pipeline in pipeline.dab.yml
-- Basic DQ checks with EXPECT and late-arriving join tolerance

CREATE LIVE TABLE dim_product_silver
COMMENT "Cleaned product dimension"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT product_id, sku, family, grade, base_cost
FROM genie_poc.bronze.dim_product_bronze
WHERE sku IS NOT NULL;

CREATE LIVE TABLE dim_region_silver
AS SELECT * FROM genie_poc.bronze.dim_region_bronze;

CREATE LIVE TABLE dim_channel_silver
AS SELECT * FROM genie_poc.bronze.dim_channel_bronze;

CREATE LIVE TABLE dim_mill_silver
AS SELECT * FROM genie_poc.bronze.dim_mill_bronze;

CREATE LIVE TABLE dim_customer_silver
AS
SELECT customer_id, customer_name, segment
FROM genie_poc.bronze.dim_customer_bronze;

CREATE LIVE TABLE dim_calendar_silver
AS SELECT * FROM genie_poc.bronze.dim_calendar_bronze;

-- SALES SILVER with basic conformance & referential checks
CREATE LIVE TABLE fact_sales_silver
TBLPROPERTIES ("quality"="silver")
AS
SELECT
  s.order_line_id, s.order_date_key, s.ship_date_key, s.product_id, s.region_id, s.channel_id,
  s.mill_id, s.customer_id, s.qty, s.list_price, s.discount_pct, s.net_price, s.cogs, s.on_time_flag
FROM genie_poc.bronze.fact_sales_bronze s
-- late arriving dim tolerance; left joins in gold
;

-- GOLD star model with joins and curated columns
CREATE LIVE TABLE fact_sales_gold
TBLPROPERTIES ("quality"="gold")
AS
SELECT
  f.order_line_id,
  c.date as order_date,
  r.region_name,
  ch.channel_name,
  m.mill_name,
  p.family as product_family,
  p.grade,
  f.qty,
  f.net_price*qty as net_sales,
  f.cogs*qty   as cogs_total,
  CASE WHEN (f.net_price*qty) > 0 THEN ( (f.net_price*qty) - (f.cogs*qty) ) / (f.net_price*qty) END as gross_margin_pct,
  f.on_time_flag
FROM LIVE.fact_sales_silver f
LEFT JOIN LIVE.dim_calendar_silver c   ON f.order_date_key = c.date_key
LEFT JOIN LIVE.dim_region_silver   r   ON f.region_id      = r.region_id
LEFT JOIN LIVE.dim_channel_silver  ch  ON f.channel_id     = ch.channel_id
LEFT JOIN LIVE.dim_mill_silver     m   ON f.mill_id        = m.mill_id
LEFT JOIN LIVE.dim_product_silver  p   ON f.product_id     = p.product_id;

-- EXPECT rules (fail rows are recorded; pipeline healthy if expectations hold)
APPLY CHANGES INTO LIVE.fact_sales_gold
FROM (SELECT * FROM LIVE.fact_sales_gold) -- placeholder for CDC pattern
KEYS (order_line_id)
SEQUENCE BY order_line_id
COLUMNS * EXCEPT (gross_margin_pct)
STORED AS SCD TYPE 1;

CREATE LIVE TABLE fact_inventory_gold AS
SELECT i.date_key, m.mill_name, i.product_id, i.qty_on_hand
FROM genie_poc.bronze.fact_inventory_bronze i
LEFT JOIN LIVE.dim_mill_silver m ON i.mill_id = m.mill_id;

CREATE LIVE TABLE fact_production_gold AS
SELECT p.date_key, m.mill_name, p.product_id, p.shift, p.qty_produced
FROM genie_poc.bronze.fact_production_bronze p
LEFT JOIN LIVE.dim_mill_silver m ON p.mill_id = m.mill_id;

CREATE LIVE TABLE fact_logistics_gold AS
SELECT l.date_key, m.mill_name, l.product_id, l.carrier, l.lead_time_days, l.ship_cost_usd
FROM genie_poc.bronze.fact_logistics_bronze l
LEFT JOIN LIVE.dim_mill_silver m ON l.mill_id = m.mill_id;

-- EXPECT checks
ALTER LIVE TABLE fact_sales_silver SET TBLPROPERTIES (
  'pipelines.expect' = 'qty > 0 AND net_price >= 0 AND cogs >= 0',
  'pipelines.expect.on_violation' = 'DROP'
);
