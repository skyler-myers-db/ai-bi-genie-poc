### Open AI/BI → Dashboards → New. Add datasets via SQL:

* `SELECT * FROM royomartin.semantic.sales_mv`
* Add time filter: `order_date` (last 365 days by default).

### Tiles:

* KPI tiles: net_sales, units_sold, avg_selling_price, gross_margin_pct, on_time_delivery_pct (choose “Measure” and select aggregation from the metric view measures).
* Time series: `net_sales` by month.
* Top/Bottom customers: create dataset `SELECT customer_id, SUM(net_sales) net_sales FROM genie_poc.gold.fact_sales GROUP BY 1 ORDER BY net_sales DESC LIMIT 20`.
* Product mix: `net_sales` by product_family donut.
* Margin bridge: dataset with price_mix_cost_yield factors (can derive from decomposition SQL below).
* “Service level by region”: map or bar on `on_time_delivery_pct` by region.
* Logistics lead-time box plot: from `genie_poc.gold.fact_logistics_gold`.

1. Filters: date (order_date), region, mill, product_family, channel.
2. Narrative panels: paste KPI definitions from `/semantic/metric_views/sales_metrics.yaml`.
3. Publish dashboard; Enable Genie from dashboard (creates/links a space so viewers can click Ask Genie).  ￼
