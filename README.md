# Databricks AI/BI Genie PoC for Manufacturing Sales

## Prerequisites (why this matters)

### Genie, AI/BI dashboards, metric views, Lakehouse Federation, Lakeflow Declarative Pipelines, and Serverless SQL warehouses are the backbone for governed NL analytics. Confirm entitlement & feature availability first to avoid dead-ends at demo time.

#### Workspace edition & governance
* Unity Catalog enabled; you have Metastore Admin or equivalent to create catalogs/schemas, connections, and metric views.
* AI/BI (dashboards + Genie) enabled. Genie spaces are configured from Catalog Explorer/Genie UI.  ￼

#### Compute
* Serverless SQL Warehouse available; we’ll create one named aibi-genie-wh (cluster size: Small, auto-stop 10 min). Photon is default/automatic on SQL warehouses.  ￼ ￼

#### Security (recommended)
* Customer-managed key (CMK) for managed services so Genie spaces and AI/BI dashboards are encrypted at rest (new dashboards after 2024-11-01; Genie spaces after 2025-04-10).  ￼ 
￼
#### CLI
* Databricks CLI v0.218+ (for bundles & pipelines); authenticated (databricks auth login).  ￼

## What this creates

* UC catalog royomartin with schemas: raw, bronze, silver, gold, semantic, dashboards, ml.
* A UC Volume for seed files and Genie knowledge store.
* Lakeflow Declarative Pipeline (serverless) to land → transform → curate.
* Metric Views in UC to centralize KPIs; used by both the dashboard and Genie.  ￼
* AI/BI dashboard + published Genie space linked to it (Ask Genie from dashboard).  ￼
* Lakehouse Federation connection to Postgres ERP price list + federated join.  ￼
* Mosaic AI Model Serving endpoint for SKU/region demand forecast.  ￼
* Auditability & lineage: AI/BI dashboard & Genie events in audit logs; lineage in Catalog Explorer.  ￼
