-- ============================================================================
-- 10_dds_star_schema.sql : final star schema in dds
-- ============================================================================
-- Loaded by controllers/load_dds.py. Docs: docs/dds_star_schema.md.
--
--   dim_date                  ALTERED (empty, from 03): fire year, Thai season, burning season
--   dim_time                  NEW: minute grain, Thai local time (1,440 rows)
--   dim_satellite_instrument  ALTERED (empty, from 03): product, version, resolution
--   dim_detection_flag        NEW junk dimension: daynight, confidence_class, dq_pass, fire_type
--   dim_location              from 08 (district grain)
--   dim_protected_area        from 09 (SCD2)
--   fact_active_fire          RECREATED, partitioned by year (03 version was empty, plain,
--                             keyed on fire_key and pointing at dim_location_spatial)
--   bridge_fire_protected_area NEW: every (detection, park version) membership
--
-- Dates and times on the fact are THAI LOCAL TIME (Asia/Bangkok, UTC+7);
-- acq_ts (UTC instant) and acq_date_utc (FIRMS acq_date) stay on the fact.
--
-- dds.ml_fire_anomalies (05): the FK to fact_active_fire.fire_key is dropped
-- (fire_key no longer exists; a FK into a partitioned table must include the
-- partition key). The link is now detection_id. fire_key is kept, unused.
--
-- Safe to re-run: IF NOT EXISTS everywhere; the old fact is dropped only while
-- it is still the plain (non-partitioned) table from 03.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- dim_date
-- ---------------------------------------------------------------------------
ALTER TABLE dds.dim_date
    ALTER COLUMN day_name   TYPE TEXT,
    ALTER COLUMN month_name TYPE TEXT,
    ADD COLUMN IF NOT EXISTS month_short      TEXT,
    ADD COLUMN IF NOT EXISTS year_month       TEXT,      -- '2020-03'
    ADD COLUMN IF NOT EXISTS day_of_year      INT,
    ADD COLUMN IF NOT EXISTS iso_week         INT,
    ADD COLUMN IF NOT EXISTS iso_year         INT,
    ADD COLUMN IF NOT EXISTS is_weekend       BOOLEAN,
    ADD COLUMN IF NOT EXISTS thai_year_be     INT,       -- Buddhist Era = year + 543
    ADD COLUMN IF NOT EXISTS fire_year_start  INT,       -- Aug-Jul fire year: Aug 2019..Jul 2020 -> 2019
    ADD COLUMN IF NOT EXISTS fire_year_label  TEXT,      -- '2019/20'
    ADD COLUMN IF NOT EXISTS thai_season      TEXT,      -- Hot / Rainy / Cool (mid-month boundaries)
    ADD COLUMN IF NOT EXISTS is_burning_season BOOLEAN;  -- 1 Jan .. 30 Apr

-- ---------------------------------------------------------------------------
-- dim_time: one row per minute of the day, Thai local time
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dds.dim_time (
    time_key        SMALLINT PRIMARY KEY,   -- HHMM local, 0..2359
    time_local      TIME     NOT NULL,
    hour_local      SMALLINT NOT NULL,
    minute          SMALLINT NOT NULL,
    time_utc        TIME     NOT NULL,      -- same minute in UTC (local - 7 h)
    hour_utc        SMALLINT NOT NULL,
    part_of_day     TEXT     NOT NULL,      -- Night 00-05 / Morning 06-11 / Afternoon 12-17 / Evening 18-23
    overpass_window TEXT     NOT NULL       -- satellite overpass window observed in the data
);

-- ---------------------------------------------------------------------------
-- dim_satellite_instrument: one row per satellite x instrument x product x version
-- ---------------------------------------------------------------------------
ALTER TABLE dds.dim_satellite_instrument
    ALTER COLUMN satellite_name  TYPE TEXT,
    ALTER COLUMN instrument_name TYPE TEXT,
    ALTER COLUMN sensor_type     TYPE TEXT,               -- sensor family: MODIS / VIIRS
    ALTER COLUMN version         TYPE TEXT,
    ALTER COLUMN version         SET NOT NULL,
    ADD COLUMN IF NOT EXISTS satellite_code TEXT,          -- FIRMS code: Aqua, Terra, N, N20, N21
    ADD COLUMN IF NOT EXISTS src_sensor     TEXT,          -- e.g. VIIRS_SNPP_SP
    ADD COLUMN IF NOT EXISTS product        TEXT,          -- SP / NRT
    ADD COLUMN IF NOT EXISTS product_name   TEXT,          -- Standard Processing / Near Real-Time
    ADD COLUMN IF NOT EXISTS resolution_m   INT,           -- nominal pixel size at nadir
    ADD COLUMN IF NOT EXISTS updated_at     TIMESTAMPTZ NOT NULL DEFAULT now();
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_dim_sat_natural') THEN
        ALTER TABLE dds.dim_satellite_instrument
            ADD CONSTRAINT uq_dim_sat_natural UNIQUE (satellite_code, product, version);
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- dim_detection_flag: junk dimension of low-cardinality detection flags
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dds.dim_detection_flag (
    detection_flag_key SERIAL   PRIMARY KEY,
    daynight           TEXT     NOT NULL,   -- D / N (FIRMS flag, from solar zenith angle)
    daynight_label     TEXT     NOT NULL,   -- Day / Night
    confidence_class   TEXT     NOT NULL,   -- low / nominal / high (conformed in item 2)
    dq_pass            BOOLEAN  NOT NULL,
    dq_flags           TEXT[]   NOT NULL,
    fire_type          SMALLINT,            -- FIRMS type; NULL for NRT (no type column)
    fire_type_label    TEXT     NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_dim_flag_natural UNIQUE NULLS NOT DISTINCT
        (daynight, confidence_class, dq_pass, dq_flags, fire_type)
);

-- ---------------------------------------------------------------------------
-- ml_fire_anomalies: unlink from fire_key, link by detection_id
-- ---------------------------------------------------------------------------
ALTER TABLE dds.ml_fire_anomalies DROP CONSTRAINT IF EXISTS ml_fire_anomalies_fire_key_fkey;
ALTER TABLE dds.ml_fire_anomalies ADD COLUMN IF NOT EXISTS detection_id TEXT;
CREATE INDEX IF NOT EXISTS idx_ml_detection_id ON dds.ml_fire_anomalies (detection_id);
COMMENT ON COLUMN dds.ml_fire_anomalies.fire_key IS
    'DEPRECATED: fact_active_fire has no fire_key since 10_dds_star_schema.sql; use detection_id';

-- ---------------------------------------------------------------------------
-- fact_active_fire: drop the empty plain table from 03, recreate partitioned
-- ---------------------------------------------------------------------------
DO $$ BEGIN
    IF (SELECT c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'dds' AND c.relname = 'fact_active_fire') = 'r' THEN
        IF EXISTS (SELECT 1 FROM dds.fact_active_fire) THEN
            RAISE EXCEPTION 'dds.fact_active_fire (03) is not empty; refusing to drop it';
        END IF;
        DROP TABLE dds.fact_active_fire;
    END IF;
END $$;

-- Grain: one clean detection (stage.fire_detection_clean).
CREATE TABLE IF NOT EXISTS dds.fact_active_fire (
    detection_id        TEXT        NOT NULL,   -- degenerate dimension (md5 detection key)
    date_key            INT         NOT NULL,   -- Thai local date, YYYYMMDD
    time_key            SMALLINT    NOT NULL,   -- Thai local time, HHMM
    location_key        INT         NOT NULL,
    satellite_key       INT         NOT NULL,
    protected_area_key  INT         NOT NULL,   -- primary park version; 0 outside PA; -1 not assessed
    detection_flag_key  INT         NOT NULL,
    acq_ts              TIMESTAMPTZ NOT NULL,   -- acquisition instant (FIRMS acq_date + acq_time, UTC)
    acq_date_utc        DATE        NOT NULL,   -- FIRMS acq_date as delivered (for NASA reconciliation)
    latitude            NUMERIC     NOT NULL,
    longitude           NUMERIC     NOT NULL,
    geom                GEOMETRY(Point, 4326) NOT NULL,
    frp_mw              NUMERIC,
    brightness_k        NUMERIC,
    background_k        NUMERIC,
    scan                NUMERIC,
    track               NUMERIC,
    confidence_pct      SMALLINT,               -- MODIS only; VIIRS uses dim_detection_flag.confidence_class
    run_id              INT         NOT NULL
) PARTITION BY RANGE (date_key);

DO $$
DECLARE y INT;
BEGIN
    FOR y IN 2012..2030 LOOP
        EXECUTE format('CREATE TABLE IF NOT EXISTS dds.fact_active_fire_%s PARTITION OF dds.fact_active_fire
                        FOR VALUES FROM (%s) TO (%s)', y, y * 10000 + 101, (y + 1) * 10000 + 101);
    END LOOP;
END $$;
-- Must stay empty (checked by load_dds.py): catches dates outside 2012..2030.
CREATE TABLE IF NOT EXISTS dds.fact_active_fire_default PARTITION OF dds.fact_active_fire DEFAULT;

-- The secondary indexes and all foreign keys (fact -> 6 dimensions, bridge ->
-- fact and dim_protected_area) are owned by load_dds.py: it drops them before
-- the bulk load and recreates them after (faster than per-row maintenance).
-- The primary key is created here and also dropped/re-added by the loader.
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fact_active_fire_pkey') THEN
        ALTER TABLE dds.fact_active_fire ADD CONSTRAINT fact_active_fire_pkey PRIMARY KEY (detection_id, date_key);
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- bridge_fire_protected_area: many-to-many fire <-> park version
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dds.bridge_fire_protected_area (
    detection_id        TEXT     NOT NULL,
    date_key            INT      NOT NULL,      -- with detection_id: FK to the fact
    protected_area_key  INT      NOT NULL,      -- park version valid on the detection date
    is_primary          BOOLEAN  NOT NULL,      -- = fact_active_fire.protected_area_key
    allocation_weight   NUMERIC  NOT NULL,      -- 1 / number of parks: weighted counts add up to fires
    run_id              INT      NOT NULL,
    PRIMARY KEY (detection_id, protected_area_key)
);

COMMENT ON TABLE dds.fact_active_fire IS 'One row per clean FIRMS detection; date/time keys in Thai local time; partitioned by year';
COMMENT ON TABLE dds.dim_time IS 'Minute of day, Thai local time (Asia/Bangkok)';
COMMENT ON TABLE dds.dim_detection_flag IS 'Junk dimension: daynight, confidence_class, dq_pass/dq_flags, fire_type';
COMMENT ON TABLE dds.bridge_fire_protected_area IS 'Every (detection, protected-area version) membership; allocation_weight = 1/n_parks';
