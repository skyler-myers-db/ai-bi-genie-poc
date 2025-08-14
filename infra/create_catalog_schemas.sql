-- Catalog & schemas
CREATE CATALOG IF NOT EXISTS genie_poc;
USE CATALOG genie_poc;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS semantic;
CREATE SCHEMA IF NOT EXISTS dashboards;
CREATE SCHEMA IF NOT EXISTS ml;

-- Volumes: one for seed files, one for Genie knowledge store
CREATE VOLUME IF NOT EXISTS raw.seed COMMENT 'Seed files for Auto Loader' IN SCHEMA genie_poc.raw;
CREATE VOLUME IF NOT EXISTS dashboards.knowledge_store COMMENT 'Curated docs for Genie knowledge' IN SCHEMA genie_poc.dashboards;

-- Analytics group (if SCIM not provisioning one yet)
CREATE GROUP IF NOT EXISTS `Sales-Analytics`;

-- Privileges: allow analytics group to use this catalog & read semantic layer
GRANT USAGE ON CATALOG genie_poc TO `Sales-Analytics`;
GRANT USAGE ON SCHEMA genie_poc.semantic TO `Sales-Analytics`;
