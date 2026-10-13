-- Enable PostGIS Extension for Spatial Analytics
CREATE EXTENSION IF NOT EXISTS postgis;

-- 1. Staging Schema (Internal Data Store for raw ingest and cleansing)
CREATE SCHEMA IF NOT EXISTS stage;

-- 2. Normalized Data Store (NDS - Internal 3NF Master Store)
CREATE SCHEMA IF NOT EXISTS nds;

-- 3. Dimensional Data Store (DDS - User-facing Star Schema for OLAP)
CREATE SCHEMA IF NOT EXISTS dds;

-- 4. Metadata & Audit Repository (Control system and event logging)
CREATE SCHEMA IF NOT EXISTS meta;

-- Grant Schema Permissions
GRANT ALL ON SCHEMA stage TO dwh_admin;
GRANT ALL ON SCHEMA nds TO dwh_admin;
GRANT ALL ON SCHEMA dds TO dwh_admin;
GRANT ALL ON SCHEMA meta TO dwh_admin;

COMMENT ON SCHEMA stage IS 'Internal staging area for raw extraction and cleansing';
COMMENT ON SCHEMA nds IS '3NF Normalized Data Store preserving complete transaction history';
COMMENT ON SCHEMA dds IS 'Dimensional Data Store (Star Schema) for spatial OLAP queries';
COMMENT ON SCHEMA meta IS 'Central metadata repository, ETL flow control, and audit logs';
