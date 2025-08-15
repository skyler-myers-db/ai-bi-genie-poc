-- GOLD star model with joins and curated columns
CREATE OR REFRESH LIVE TABLE fact_sales_gold
  CLUSTER BY AUTO
  TBLPROPERTIES ("quality" = "gold") AS
SELECT
  f.order_line_id,
  c.date AS order_date,
  r.region_name,
  ch.channel_name,
  m.mill_name,
  p.family AS product_family,
  p.grade,
  f.qty,
  f.net_price * qty AS net_sales,
  f.cogs * qty AS cogs_total,
  CASE
    WHEN (f.net_price * qty) > 0 THEN ((f.net_price * qty) - (f.cogs * qty)) / (f.net_price * qty)
  END AS gross_margin_pct,
  f.on_time_flag
FROM
  silver.fact_sales_silver f
    LEFT JOIN silver.dim_calendar_silver c
      ON f.order_date_key = c.date_key
    LEFT JOIN silver.dim_region_silver r
      ON f.region_id = r.region_id
    LEFT JOIN silver.dim_channel_silver ch
      ON f.channel_id = ch.channel_id
    LEFT JOIN silver.dim_mill_silver m
      ON f.mill_id = m.mill_id
    LEFT JOIN silver.dim_product_silver p
      ON f.product_id = p.product_id;

-- EXPECT rules (fail rows are recorded; pipeline healthy if expectations hold)
-- APPLY CHANGES INTO fact_sales_gold FROM (
--   SELECT
--     *
--   FROM
--     fact_sales_gold
-- ) KEYS (order_line_id) SEQUENCE BY order_line_id COLUMNS * EXCEPT (
--   -- placeholder for CDC pattern
--   gross_margin_pct
-- ) STORED AS SCD TYPE 1;
CREATE OR REFRESH LIVE TABLE fact_inventory_gold
  CLUSTER BY AUTO AS
SELECT
  i.date_key,
  m.mill_name,
  i.product_id,
  i.qty_on_hand
FROM
  bronze.fact_inventory_bz i
    LEFT JOIN silver.dim_mill_silver m
      ON i.mill_id = m.mill_id;

CREATE OR REFRESH LIVE TABLE fact_production_gold
  CLUSTER BY AUTO AS
SELECT
  p.date_key,
  m.mill_name,
  p.product_id,
  p.shift,
  p.qty_produced
FROM
  bronze.fact_production_bz p
    LEFT JOIN silver.dim_mill_silver m
      ON p.mill_id = m.mill_id;

CREATE OR REFRESH LIVE TABLE fact_logistics_gold
  CLUSTER BY AUTO AS
SELECT
  l.date_key,
  m.mill_name,
  l.product_id,
  l.carrier,
  l.lead_time_days,
  l.ship_cost_usd
FROM
  bronze.fact_logistics_bz l
    LEFT JOIN silver.dim_mill_silver m
      ON l.mill_id = m.mill_id;

CREATE OR REFRESH LIVE TABLE fact_sales
  CLUSTER BY AUTO
  COMMENT 'Order line level'
  TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true') AS
SELECT
  *
FROM
  gold.fact_sales_gold;