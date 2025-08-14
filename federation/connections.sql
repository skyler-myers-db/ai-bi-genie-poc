-- run in a SQL editor as metastore admin or user with CREATE CONNECTION
USE CATALOG royomartin;

CREATE CONNECTION IF NOT EXISTS erp_pg_conn TYPE postgresql
OPTIONS (
  host 'erp-postgres.example.com',
  port '5432',
  user secret('erp_scope','erp_user'),
  password secret('erp_scope','erp_pwd')
);

CREATE FOREIGN CATALOG IF NOT EXISTS erp_price_fc
USING CONNECTION erp_pg_conn
OPTIONS (database 'pricing');

SELECT s.region_name, s.product_family,
       SUM(s.net_sales) AS realized_sales,
       AVG(p.list_price_usd) AS avg_list_price,
       DIV0(AVG(p.list_price_usd) - DIV0(SUM(s.net_sales), NULLIF(SUM(s.qty),0)), AVG(p.list_price_usd)) AS avg_discount_pct
FROM genie_poc.gold.fact_sales s
LEFT JOIN erp_price_fc.public.price_list p
  ON p.sku = CONCAT(s.product_family,'-', LPAD(CAST(s.product_id AS STRING),4,'0'))
GROUP BY 1,2
ORDER BY 1,2;
