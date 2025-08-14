USE CATALOG genie_poc;
USE SCHEMA semantic;

CREATE OR REPLACE VIEW sales_mv
METRICS
LANGUAGE YAML
AS $$ 
-- paste YAML contents from sales_metrics.yaml here
version: 0.1
source:
  query: |
    SELECT
      fs.order_line_id,
      fs.order_date,
      fs.region_name,
      fs.channel_name,
      fs.mill_name,
      fs.product_family,
      fs.grade,
      fs.qty,
      fs.net_sales,
      fs.cogs_total,
      fs.gross_margin_pct,
      fs.on_time_flag
    FROM genie_poc.gold.fact_sales fs
joins: []
dimensions:
  - name: order_date
    expr: order_date
    type: time
    time_granularities: [day, month, quarter, year]
  - name: region
    expr: region_name
  - name: channel
    expr: channel_name
  - name: mill
    expr: mill_name
  - name: product_family
    expr: product_family
  - name: grade
    expr: grade
measures:
  - name: net_sales
    expr: SUM(net_sales)
  - name: units_sold
    expr: SUM(qty)
  - name: avg_selling_price
    expr: DIV0(SUM(net_sales), NULLIF(SUM(qty),0))
  - name: gross_margin_pct
    expr: DIV0(SUM(net_sales - cogs_total), NULLIF(SUM(net_sales),0))
  - name: on_time_delivery_pct
    expr: DIV0(SUM(CASE WHEN on_time_flag=1 THEN 1 ELSE 0 END), COUNT(*))
filters:
  - name: last_2_years
    expr: order_date >= DATEADD(year, -2, CURRENT_DATE())
metadata:
  description: "Sales KPIs with product/mill/region/channel grain"
  tags: ["governed","ai-bi","metric-view"]
$$;

-- Grants for governed self-service
GRANT USAGE ON SCHEMA genie_poc.semantic TO `Sales-Analytics`;
GRANT SELECT ON VIEW genie_poc.semantic.sales_mv TO `Sales-Analytics`;
