# AI/BI Genie PoC — Manufacturing Sales

This repo delivers a **1-click-ish** Databricks PoC that demonstrates governed, self-service analytics with **AI/BI Genie** over realistic manufacturing sales data (OSB/plywood/timber across multiple mills & regions).

---

## What you get

- Synthetic but realistic **Lakehouse** (Bronze/Silver/Gold) for:
  - Sales orders (10–50M order lines, scalable)
  - Inventory (by mill/SKU/day)
  - Production (by mill/SKU/shift)
  - Logistics (carrier, lead time, cost)
- **Lakeflow Declarative Pipelines**: incremental loads, expectations (data quality), late-arrival tolerance, CDC-ready patterns.
- **Unity Catalog Metric Views**: governed metrics (Net Sales, Units, ASP, GM%, OTD%, Inventory Turns, Mill Yield%, Region Fill Rate) with YAML definitions.
- **AI/BI Dashboard** + linked **Genie Space** (Ask Genie button) for natural-language analytics.
- **Lakehouse Federation** to ERP (PostgreSQL price list) + federated joins.
- **Mosaic AI Model Serving**: tiny demand-forecast endpoint surfaced in dashboard & Genie prompts.
- **Automation**: smoke tests, acceptance criteria, demo script. 
- **Security/Governance**: CMK-backed Genie, lineage, audit queries.

---

## Prerequisites

> Workspace & account requirements (validated against Databricks docs).

- **Edition/Plan**: Premium or Enterprise; **Unity Catalog enabled** and set as default catalog. 
- **Serverless SQL Warehouse** available in your region; **Photon** enabled.
- **Entitlements & Permissions**
  - User running setup: `ACCOUNT ADMIN` (to confirm serverless enablement if needed) or `WORKSPACE ADMIN`, plus **SQL access** and permission to create catalogs/schemas, warehouses, pipelines, and Genie spaces.
  - Group **Sales-Analytics** exists (or adjust names) and can **USE CATALOG/SCHEMA**, **SELECT** on metric views, and **VIEW** dashboard & Genie space.
- **CMK for Managed Services (optional but recommended)** to encrypt Genie/dashboard metadata (Enterprise). **Genie spaces created after Apr 10, 2025** & **dashboards created after Nov 1, 2024** are CMK-compatible.
- **Databricks CLI (v0.216+)** configured (`databricks auth login`). **Asset Bundles** enabled (we use them to deploy Lakeflow).

> Default names used throughout (feel free to change, but be consistent):

- **Catalog**: `genie_poc`
- **Schemas**: `raw`, `bronze`, `silver`, `gold`, `ml`, `external`, `semantic`, `ops`
- **Warehouse**: `genie_serverless_wh` (Pro/Serverless, Medium)
- **UC Volume** (seed files): `genie_poc.raw.seed_vol`
- **Genie Space**: `Sales Analytics`
- **Dashboard**: `Executive Sales (AI/BI)`
- **Model**: `genie_poc.ml.units_forecast`
- **Federation**: connection `erp_pg_conn`, foreign catalog `erp_pg`

---

## Quickstart (“near one-click”) — from a clean workspace

> The following creates a Serverless SQL Warehouse (Photon), UC objects, seeds data, deploys Lakeflow, creates metric views, dashboard, Genie space, federation, and the model endpoint.

### 0) Create a Serverless SQL Warehouse (Photon)

```bash
# Create a serverless PRO warehouse with Photon (Medium; 15 min auto-stop)
databricks warehouses create \
  --name genie_serverless_wh \
  --cluster-size "2X-Small" \
  --auto-stop-mins 15 \
  --enable-serverless-compute \
  --enable-photon \
  --warehouse-type PRO \
  --max-num-clusters 1 \
  --spot-instance-policy COST_OPTIMIZED
```

Then capture its ID for later:

```bash
WH_ID=$(databricks warehouses list | awk -F' ' '/rm_serverless_wh/{print $1; exit}')
echo $WH_ID
```

0) Demo Setup

Link your Git credentials in you Databricks workspace (Settings dropdown in the top right corner under your user -> Linked accounts -> Git integration)

Add a new Git repo in the Workspace under your user directory root and clone this repo `https://github.com/skyler-myers-db/ai-bi-genie-poc.git`

1) Bootstrap Unity Catalog namespaces & seed volume

Create the user group `Sales-Analytics`

Open a SQL editor attached to `genie_serverless_wh` and run the code in `infra/create_catalog_schemas.sql`

UC provides centralized governance for all these objects.  ￼

2) Generate synthetic data (Parquet) into the UC Volume

In a Notebook on Serverless Notebook compute (or all-Python job), run:

```python
%pip install faker numpy pandas pyarrow
```

Then run the data generator script at `ai-bi-genie-poc/data/seed/generate_synthetic_data.py`

After completion, you should see:

> Synthetic data files written to /Volumes/genie_poc/raw/seed

Printed to the console

3) Deploy Pipeline

* Go to the `/ai-bi-genie-poc/data/lakeflow/databricks.yml` file
* Open the Workspace sidebar on the left (you can click the `genie_poc_sales_pipeline` banner above the file code if it's not open) and clikc the 'rocket' icon
* click the "Deploy" button then click "Deploy" again once the bundle is validated

4) Run the Job

* Once the deployement completes, go to `Jobs & Pipelines` on the left side banner (after you see the `Deployment complete!` message in the console)
* Find your new job, `wf_genie_poc_sales`
* Click `Run now`
* Once the job completes, run the script to generate the views at: `ai-bi-genie-poc/infra/create_gold_views.sql`

5) Semantic Layer (if you want to skip, import the dashboard file here: `/ai-bi-genie-poc/dashboards/genie_poc_sales_db.lvdash.json`

* Create the metric view `ai-bi-genie-poc/semantic/metric_views/create_metric_views.sql` by running this file in the editor
* On the left side menu, click "New" -> "Dashboard"
* Go to "Data" -> "+ Add data source" -> "Metric views" -> "sales_mv"
* Go back to the "Untitled page", click the filter icon on the bottom, place it, select the "Order Month" field, and transform by "MONTHLY"
* Select "Add a visualization" at the bottom and place boxes for "Total Net Sales", "Units Sold", etc. for all measures (you can also add cell titles and format the numbers, + add conditional formatting colors based on the values)
* Add a line chart with Total Net Sales and order month on X-axis Total net sales on Y-axis
* Go back to the "Data" tab, select "Create from SQL" under "Create another dataset", and use this SQL: `SELECT mill_name, SUM(net_sales) net_sales FROM genie_poc.gold.fact_sales GROUP BY 1 ORDER BY mill_name DESC LIMIT 20;` to find the top seeling mills
* Create a pi chart with this -- SUM(net_sales) as angle with each mill its own color
* Do the same with net sales by Product Family and create a bar chart for on time delivery percent by region
* Add the `fact_logistics_gold` table to the datasets
* Add more filters at the top for all of the dimensions like mill, channel, etc.
* Add a text box at the top and paste the content of the file: `ai-bi-genie-poc/semantic/glossary/terms.sql`

6) Create Genie Space

* At the top right of the dashboard page, click "Publish". Embed your credentials and click "Enable Genie" at the bottom with an auto-generated space
* Now, on the left menu again, let's click "Genie" to create our own Genie space
* Click "New" at the top right
* For connecting your data, add the logistics, inventory, and production gold tables as well as all the dim views except customer
* Click "Instructions" on the top right, and copy and paste the content of `ai-bi-genie-poc/genie/instructions.md`
* Click "Save" in the bottom right, then click on "Settings" at the top right. Add the sample questions from the `ai-bi-genie-poc/genie/sample_questions.md` file
* Ask away! You can always add even more things to help curate the space, such as sample joins and queries

7) Extras

* For detailed, automatic lineage in Unity Catalog click on Catalog on the left menu -> `genie_poc` -> `gold` -> `fact_sales` -> `Lineage` -> `See lineage graph`
* click the + signs to expand the lineage either way
* To audit the Genie space usage, use the query in `/ai-bi-genie-poc/genie/audit.sql` -- just be sure to enable the `audit` system table first

8) ML

* Run the file: `/ai-bi-genie-poc/models/serving/train_and_register_forecaster.py` to train and register a new ML forecasting model on the data
* Once the model is finished training, register the endpoint by running: `/ai-bi-genie-poc/models/serving/sdk_deployment.py`
* Get the ID (run in terminal with Databricks CLI: `EP_ID=$(databricks serving-endpoints get units-forecaster -o json | jq -r '.id')`
* Give access to the endpoint:

```bash
databricks serving-endpoints set-permissions "$EP_ID" --json '{
  "access_control_list":[
    {"group_name":"Sales-Analytics","permission_level":"CAN_QUERY"}
  ]
}'
```

* Make a prediction:

```bash
databricks serving-endpoints query units-forecaster --json '{
  "dataframe_split": {
    "index": [0],
    "columns": ["product_id","region_id","ds","horizon"],
    "data": [[101, 3, "2025-08-01", 1]]
  }
}'
```
