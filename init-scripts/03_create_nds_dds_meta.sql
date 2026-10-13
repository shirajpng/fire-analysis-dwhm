-- ============================================================================
-- 1. METADATA & AUDIT CONTROL SCHEMA ("meta")
-- ============================================================================

CREATE TABLE IF NOT EXISTS "meta" . "ds_table" (
    "table_key" SERIAL PRIMARY KEY,
    "data_store" VARCHAR(50) NOT NULL, -- 'STAGE', 'NDS', 'DDS'
    "table_name" VARCHAR(100) NOT NULL,
    "table_type" VARCHAR(50) NOT NULL,
    "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS "meta" . "ds_column" (
    "column_key" SERIAL PRIMARY KEY,
    "table_key" INT REFERENCES "meta" . "ds_table"("table_key"),
    "column_name" VARCHAR(100) NOT NULL,
    "data_type" VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS "meta" . "data_mapping" (
    "mapping_key" SERIAL PRIMARY KEY,
    "target_table" VARCHAR(100) NOT NULL,
    "target_column" VARCHAR(100) NOT NULL,
    "source_table" VARCHAR(100) NOT NULL,
    "source_column" VARCHAR(100) NOT NULL,
    "transformation_rule" TEXT,
    "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS "meta" . "etl_data_flow" (
    "flow_id" SERIAL PRIMARY KEY,
    "job_name" VARCHAR(100) NOT NULL,
    "source_table" VARCHAR(100) NOT NULL,
    "target_table" VARCHAR(100) NOT NULL,
    "lset" TIMESTAMP NOT NULL, -- Last Successful Extraction Time
    "cet" TIMESTAMP NOT NULL,  -- Current Extraction Time
    "status" VARCHAR(20) DEFAULT 'RUNNING'
);

CREATE TABLE IF NOT EXISTS "meta" . "event_log" (
    "log_key" SERIAL PRIMARY KEY,
    "event_type" VARCHAR(100) NOT NULL,
    "event_category" VARCHAR(100) NOT NULL,
    "execution_timestamp" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "rows_processed" INT DEFAULT 0,
    "status" VARCHAR(20) NOT NULL,
    "note" TEXT
);

-- ============================================================================
-- 2. NORMALIZED DATA STORE ("nds" - 3NF Master Store)
-- ============================================================================

CREATE TABLE IF NOT EXISTS "nds" . "nds_satellite" (
    "satellite_id" SERIAL PRIMARY KEY,
    "satellite_name" VARCHAR(50) UNIQUE NOT NULL,
    "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS "nds" . "nds_instrument" (
    "instrument_id" SERIAL PRIMARY KEY,
    "instrument_name" VARCHAR(50) NOT NULL,
    "sensor_type" VARCHAR(20) NOT NULL,
    "version" VARCHAR(10),
    CONSTRAINT "uq_instrument_type" UNIQUE ("instrument_name", "sensor_type", "version")
);

CREATE TABLE IF NOT EXISTS "nds" . "nds_location" (
    "location_id" SERIAL PRIMARY KEY,
    "latitude" NUMERIC(9,6) NOT NULL,
    "longitude" NUMERIC(9,6) NOT NULL,
    "geom" GEOMETRY(Point, 4326) NOT NULL,
    CONSTRAINT "uq_lat_lon" UNIQUE ("latitude", "longitude")
);

CREATE TABLE IF NOT EXISTS "nds" . "nds_protected_area" (
    "pa_id" SERIAL PRIMARY KEY,
    "site_id" INT NOT NULL,
    "name_eng" VARCHAR(255),
    "desig_eng" VARCHAR(150),
    "iucn_cat" VARCHAR(50),
    "rep_area" NUMERIC(12,4),
    "gov_type" VARCHAR(150),
    "iso3" VARCHAR(10),
    "geom" GEOMETRY(MultiPolygon, 4326),
    "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS "nds" . "nds_fire_event" (
    "event_id" BIGSERIAL PRIMARY KEY,
    "location_id" INT REFERENCES "nds" . "nds_location"("location_id"),
    "satellite_id" INT REFERENCES "nds" . "nds_satellite"("satellite_id"),
    "instrument_id" INT REFERENCES "nds" . "nds_instrument"("instrument_id"),
    "acq_timestamp" TIMESTAMP NOT NULL,
    "brightness_kelvin" NUMERIC(6,2),
    "channel_temp_kelvin" NUMERIC(6,2),
    "frp" NUMERIC(8,2),
    "scan" NUMERIC(4,2),
    "track" NUMERIC(4,2),
    "confidence_pct" INT,
    "daynight" VARCHAR(2),
    "fire_type" INT,
    "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- 3. DIMENSIONAL DATA STORE ("dds" - Star Schema for OLAP)
-- ============================================================================

-- Date Dimension
CREATE TABLE IF NOT EXISTS "dds" . "dim_date" (
    "date_key" INT PRIMARY KEY, -- YYYYMMDD
    "full_date" DATE NOT NULL,
    "day_of_week" INT NOT NULL,
    "day_name" VARCHAR(15) NOT NULL,
    "day_of_month" INT NOT NULL,
    "month_number" INT NOT NULL,
    "month_name" VARCHAR(15) NOT NULL,
    "quarter_number" INT NOT NULL,
    "year_number" INT NOT NULL
);

-- Spatial Location Dimension
CREATE TABLE IF NOT EXISTS "dds" . "dim_location_spatial" (
    "location_key" SERIAL PRIMARY KEY,
    "latitude" NUMERIC(9,6) NOT NULL,
    "longitude" NUMERIC(9,6) NOT NULL,
    "geom" GEOMETRY(Point, 4326) NOT NULL,
    "province_name" VARCHAR(100) DEFAULT 'Unknown',
    "district_name" VARCHAR(100) DEFAULT 'Unknown'
);

-- Satellite & Sensor Dimension
CREATE TABLE IF NOT EXISTS "dds" . "dim_satellite_instrument" (
    "satellite_key" SERIAL PRIMARY KEY,
    "satellite_name" VARCHAR(50) NOT NULL,
    "instrument_name" VARCHAR(50) NOT NULL,
    "sensor_type" VARCHAR(20) NOT NULL,
    "version" VARCHAR(10)
);

-- Protected Area Dimension (SCD Type 2)
CREATE TABLE IF NOT EXISTS "dds" . "dim_protected_area" (
    "protected_area_key" SERIAL PRIMARY KEY,
    "site_id" INT NOT NULL,
    "name_eng" VARCHAR(255),
    "desig_eng" VARCHAR(150),
    "iucn_cat" VARCHAR(50),
    "rep_area" NUMERIC(12,4),
    "gov_type" VARCHAR(150),
    "effective_timestamp" TIMESTAMP NOT NULL,
    "expiry_timestamp" TIMESTAMP,
    "is_current" BOOLEAN DEFAULT TRUE,
    "geom" GEOMETRY(MultiPolygon, 4326)
);

-- Active Fire Fact Table
CREATE TABLE IF NOT EXISTS "dds" . "fact_active_fire" (
    "fire_key" BIGSERIAL PRIMARY KEY,
    "date_key" INT REFERENCES "dds" . "dim_date"("date_key"),
    "location_key" INT REFERENCES "dds" . "dim_location_spatial"("location_key"),
    "satellite_key" INT REFERENCES "dds" . "dim_satellite_instrument"("satellite_key"),
    "protected_area_key" INT REFERENCES "dds" . "dim_protected_area"("protected_area_key"),
    "acq_time" VARCHAR(10),
    "brightness_kelvin" NUMERIC(6,2),
    "channel_temp_kelvin" NUMERIC(6,2),
    "frp" NUMERIC(8,2),
    "scan" NUMERIC(4,2),
    "track" NUMERIC(4,2),
    "confidence_score" INT,
    "daynight" VARCHAR(2)
);

-- Spatial & Analytical Indexing
CREATE INDEX IF NOT EXISTS "idx_dds_loc_geom" ON "dds" . "dim_location_spatial" USING GIST ("geom");
CREATE INDEX IF NOT EXISTS "idx_dds_pa_geom" ON "dds" . "dim_protected_area" USING GIST ("geom");
CREATE INDEX IF NOT EXISTS "idx_dds_fact_date" ON "dds" . "fact_active_fire" ("date_key");
CREATE INDEX IF NOT EXISTS "idx_dds_fact_loc" ON "dds" . "fact_active_fire" ("location_key");
CREATE INDEX IF NOT EXISTS "idx_dds_fact_pa" ON "dds" . "fact_active_fire" ("protected_area_key");
