-- Catalog & schemas
CREATE CATALOG IF NOT EXISTS genie_poc
-- Optional managed location if metastore storage is not set up
MANAGED LOCATION 's3://databricks-cloud-storage-bucket-deployment-2/unity-catalog/842446108592895/genie_poc';

USE CATALOG genie_poc;

CREATE SCHEMA IF NOT EXISTS raw COMMENT 'Seed files for Auto Loader';

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE SCHEMA IF NOT EXISTS silver;

CREATE SCHEMA IF NOT EXISTS gold;

CREATE SCHEMA IF NOT EXISTS semantic;

CREATE SCHEMA IF NOT EXISTS dashboards COMMENT 'Curated docs for Genie knowledge';

CREATE SCHEMA IF NOT EXISTS ml;

-- Volumes: one for seed files, one for Genie knowledge store
CREATE VOLUME IF NOT EXISTS raw.seed;

CREATE VOLUME IF NOT EXISTS dashboards.knowledge_store;

CREATE VOLUME IF NOT EXISTS genie_poc.ml.forecast_artifacts COMMENT 'ML artifacts for seasonal forecaster (governed by Unity Catalog)';

-- Privileges: allow analytics group to use this catalog & read semantic layer
GRANT USAGE ON CATALOG genie_poc TO `Sales-Analytics`;

GRANT USAGE ON SCHEMA genie_poc.semantic TO `Sales-Analytics`;

GRANT READ FILES, WRITE FILES ON VOLUME genie_poc.ml.forecast_artifacts TO `Sales-Analytics`;