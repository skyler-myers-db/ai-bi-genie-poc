USE CATALOG genie_poc;

USE SCHEMA semantic;

CREATE OR REPLACE VIEW sales_mv WITH METRICS LANGUAGE YAML AS
$$
version: 0.1

source: |
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
    fs.on_time_flag
  FROM genie_poc.gold.fact_sales fs

joins: []

dimensions:
  - name: "Order Name"
    expr: order_date
  - name: "Order Month"
    expr: DATE_TRUNC('month', order_date)
  - name: "Order Quarter"
    expr: DATE_TRUNC('quarter', order_date)
  - name: "Order Year"
    expr: DATE_TRUNC('year', order_date)

  - name: "Region"
    expr: region_name
  - name: "Channel"
    expr: channel_name
  - name: "Mill"
    expr: mill_name
  - name: "Product Family"
    expr: product_family
  - name: "Grade"
    expr: grade

# ---- Atomic measures (true aggregates) ----
measures:
  - name: "Total Net Sales"
    expr: SUM(net_sales)

  - name: "Units Sold"
    expr: SUM(qty)

  - name: "Total Cogs"
    expr: SUM(cogs_total)

  - name: "On Time Count"
    expr: SUM(CASE WHEN on_time_flag = 1 THEN 1 ELSE 0 END)

  - name: "Order Lines"
    expr: COUNT(*)

# ---- Derived measures (reference prior measures with MEASURE()) ----
  - name: "Average Selling Price"
    expr: CASE
            WHEN MEASURE(`Units Sold`) = 0 THEN 0
            ELSE MEASURE(`Total Net Sales`) / MEASURE(`Units Sold`)
          END

  - name: "Gross Margin Percent"
    expr: CASE
            WHEN MEASURE(`Total Net Sales`) = 0 THEN 0
            ELSE (MEASURE(`Total Net Sales`) - MEASURE(`Total Cogs`)) / MEASURE(`Total Net Sales`)
          END

  - name: "On Time Delivery Percent"
    expr: CASE
            WHEN MEASURE(`Order Lines`) = 0 THEN 0
            ELSE MEASURE(`On Time Count`) / MEASURE(`Order Lines`)
          END

filter: order_date >= DATEADD(year, -2, CURRENT_DATE())
$$;

COMMENT ON VIEW sales_mv IS 'Sales KPIs with product/mill/region/channel grain';

ALTER VIEW
  sales_mv
SET TBLPROPERTIES
  ('tag.governed' = 'true', 'tag.ai-bi' = 'true', 'tag.metric-view' = 'true');

-- Grants for governed self-service
GRANT USAGE ON SCHEMA genie_poc.semantic TO `Sales-Analytics`;

GRANT SELECT ON VIEW genie_poc.semantic.sales_mv TO `Sales-Analytics`;