-- Catalog & schemas
CREATE CATALOG IF NOT EXISTS royomartin;
USE CATALOG royomartin;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS semantic;
CREATE SCHEMA IF NOT EXISTS dashboards;
CREATE SCHEMA IF NOT EXISTS ml;

-- Volumes: one for seed files, one for Genie knowledge store
CREATE VOLUME IF NOT EXISTS raw.seed COMMENT 'Seed files for Auto Loader' IN SCHEMA royomartin.raw;
CREATE VOLUME IF NOT EXISTS dashboards.knowledge_store COMMENT 'Curated docs for Genie knowledge' IN SCHEMA royomartin.dashboards;

-- Analytics group (if SCIM not provisioning one yet)
CREATE GROUP IF NOT EXISTS `Sales-Analytics`;

-- Privileges: allow analytics group to use this catalog & read semantic layer
GRANT USAGE ON CATALOG royomartin TO `Sales-Analytics`;
GRANT USAGE ON SCHEMA royomartin.semantic TO `Sales-Analytics`;
