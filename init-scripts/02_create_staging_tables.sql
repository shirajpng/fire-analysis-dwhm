-- 1. Raw MODIS Staging Table
CREATE TABLE IF NOT EXISTS stage.stg_modis_fire (
    stg_id SERIAL PRIMARY KEY,
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    brightness NUMERIC(6,2),
    scan NUMERIC(4,2),
    track NUMERIC(4,2),
    acq_date DATE,
    acq_time VARCHAR(10),
    satellite VARCHAR(20),
    instrument VARCHAR(20),
    confidence INT,
    version VARCHAR(10),
    bright_t31 NUMERIC(6,2),
    frp NUMERIC(8,2),
    daynight VARCHAR(2),
    type INT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Raw VIIRS Staging Table
CREATE TABLE IF NOT EXISTS stage.stg_viirs_fire (
    stg_id SERIAL PRIMARY KEY,
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    bright_ti4 NUMERIC(6,2),
    scan NUMERIC(4,2),
    track NUMERIC(4,2),
    acq_date DATE,
    acq_time VARCHAR(10),
    satellite VARCHAR(20),
    instrument VARCHAR(20),
    confidence VARCHAR(10),
    version VARCHAR(10),
    bright_ti5 NUMERIC(6,2),
    frp NUMERIC(8,2),
    daynight VARCHAR(2),
    type INT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Raw WDPA Protected Areas Staging Table
CREATE TABLE IF NOT EXISTS stage.stg_wdpa_protected_area (
    stg_id SERIAL PRIMARY KEY,
    site_id INT,
    name_eng VARCHAR(255),
    name_local VARCHAR(255),
    desig_eng VARCHAR(150),
    iucn_cat VARCHAR(50),
    realm VARCHAR(50),
    rep_area NUMERIC(12,4),
    gis_area NUMERIC(12,4),
    status VARCHAR(50),
    status_yr INT,
    gov_type VARCHAR(150),
    iso3 VARCHAR(10),
    geom_type VARCHAR(20),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Unified & Cleansed Active Fire Staging Table
CREATE TABLE IF NOT EXISTS stage.stg_active_fire_unified (
    unified_id SERIAL PRIMARY KEY,
    source_sensor VARCHAR(20), -- 'MODIS' or 'VIIRS'
    latitude NUMERIC(9,6) NOT NULL,
    longitude NUMERIC(9,6) NOT NULL,
    acq_timestamp TIMESTAMP NOT NULL,
    brightness_kelvin NUMERIC(6,2),   -- Reconciled sensor temp
    channel_temp_kelvin NUMERIC(6,2), -- Reconciled background temp
    frp NUMERIC(8,2),
    scan NUMERIC(4,2),
    track NUMERIC(4,2),
    confidence_pct INT,
    satellite VARCHAR(20),
    daynight VARCHAR(2),
    fire_type INT,
    geom GEOMETRY(Point, 4326),
    dq_flag VARCHAR(100) DEFAULT 'PASS',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stg_fire_geom ON stage.stg_active_fire_unified USING GIST (geom);
