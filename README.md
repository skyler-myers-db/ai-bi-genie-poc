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

1) Bootstrap Unity Catalog namespaces & seed volume

Open a SQL editor attached to `genie_serverless_wh` and run:

```sql
-- 1. Create catalog & schemas (idempotent)
CREATE CATALOG IF NOT EXISTS genie_poc;
USE CATALOG genie_poc;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS ml;
CREATE SCHEMA IF NOT EXISTS external;
CREATE SCHEMA IF NOT EXISTS semantic;
CREATE SCHEMA IF NOT EXISTS ops;

-- 2. Create a UC Volume for seeds
CREATE VOLUME IF NOT EXISTS raw.seed_vol COMMENT 'Seed files for PoC';
```

UC provides centralized governance for all these objects.  ￼

2) Generate synthetic data (Parquet) into the UC Volume

In a Notebook on Serverless Notebook compute (or all-Python job), run:

```python
%pip install faker numpy pandas pyarrow
```

Then run the generator:

