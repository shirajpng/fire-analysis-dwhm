-- ============================================================================
-- 07_clean_dedup.sql : typed, conformed, deduplicated fire detections
-- ============================================================================
-- Input : stage.raw_firms_modis, stage.raw_firms_viirs (all TEXT, 06_stage_v2.sql)
-- Output: stage.fire_detection_clean   one row per detection (detection_id)
--         stage.fire_detection_removed rows dropped by dedup rules A/B, with reason
--         meta.dq_metrics              before/after counts per run, rule, sensor
-- Rebuilt by controllers/clean_stage.py (TRUNCATE + INSERT in one transaction).
--
-- Not stg_active_fire_unified (02): it has no detection_id / product / lineage
-- columns, a single dq_flag, and confidence_pct INT would force invented VIIRS
-- percentages. It is still referenced by the old DAG and etl_dim_fact.py.
--
-- Additive and safe to re-run (IF NOT EXISTS / CREATE OR REPLACE).
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Safe-cast helpers: NULL instead of an error when the text is not valid.
-- pg_input_is_valid (Postgres 16+) tests a cast without raising.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION stage.try_numeric(v text) RETURNS numeric
LANGUAGE sql STABLE PARALLEL SAFE AS
$$ SELECT CASE WHEN pg_input_is_valid(v, 'numeric') THEN v::numeric END $$;

CREATE OR REPLACE FUNCTION stage.try_date(v text) RETURNS date
LANGUAGE sql STABLE PARALLEL SAFE AS
$$ SELECT CASE WHEN v ~ '^\d{4}-\d{2}-\d{2}$' AND pg_input_is_valid(v, 'date') THEN v::date END $$;

-- FIRMS satellite codes -> one conformed name used everywhere downstream.
CREATE OR REPLACE FUNCTION stage.conform_satellite(code text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT CASE code
            WHEN 'Aqua'  THEN 'Aqua'
            WHEN 'Terra' THEN 'Terra'
            WHEN 'N'     THEN 'Suomi-NPP'
            WHEN 'N20'   THEN 'NOAA-20'
            WHEN 'N21'   THEN 'NOAA-21'
          END $$;

-- ---------------------------------------------------------------------------
-- Clean detections. Measures are unconstrained NUMERIC on purpose: a fixed
-- precision such as NUMERIC(6,2) would turn an out-of-range value into a
-- crash instead of a flag.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stage.fire_detection_clean (
    detection_id      TEXT        NOT NULL,   -- md5(satellite|acq_ts UTC|lat 4dp|lon 4dp)
    sensor_family     TEXT        NOT NULL,   -- MODIS / VIIRS
    src_sensor        TEXT        NOT NULL,   -- e.g. VIIRS_SNPP_SP
    product           TEXT        NOT NULL,   -- SP / NRT
    satellite         TEXT,                   -- conformed: Aqua, Terra, Suomi-NPP, NOAA-20, NOAA-21
    satellite_code    TEXT,                   -- as delivered: Aqua, Terra, N, N20, N21
    instrument        TEXT,
    version           TEXT,
    acq_date          DATE,
    acq_time_hhmm     TEXT,                   -- zero-padded HHMM, e.g. '0347'
    acq_ts            TIMESTAMPTZ,            -- acq_date + acq_time, UTC
    latitude          NUMERIC,
    longitude         NUMERIC,
    geom              GEOMETRY(Point, 4326),  -- NULL when coordinates are impossible
    brightness_k      NUMERIC,                -- MODIS brightness  / VIIRS bright_ti4
    background_k      NUMERIC,                -- MODIS bright_t31  / VIIRS bright_ti5
    frp_mw            NUMERIC,
    scan              NUMERIC,
    track             NUMERIC,
    confidence_raw    TEXT,                   -- original value: 0-100 (MODIS) or l/n/h (VIIRS)
    confidence_pct    SMALLINT,               -- MODIS only; NULL for VIIRS (no real percentage)
    confidence_class  TEXT,                   -- low / nominal / high for both sensors
    daynight          TEXT,
    fire_type         SMALLINT,               -- 0 veg, 1 volcano, 2 static land, 3 offshore; NULL for NRT
    dq_flags          TEXT[]      NOT NULL DEFAULT '{}',
    dq_pass           BOOLEAN     NOT NULL,   -- true when dq_flags is empty
    source_file       TEXT        NOT NULL,
    run_id            INT         NOT NULL,   -- meta.event_log.log_key
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fire_detection_clean_pkey PRIMARY KEY (detection_id)
);
CREATE INDEX IF NOT EXISTS idx_fire_clean_geom     ON stage.fire_detection_clean USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_fire_clean_acq_date ON stage.fire_detection_clean (acq_date);

-- ---------------------------------------------------------------------------
-- Audit of rows removed by dedup rules (rebuilt with the clean table).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stage.fire_detection_removed (
    LIKE stage.fire_detection_clean INCLUDING DEFAULTS,  -- same columns, no primary key
    removal_rule      TEXT NOT NULL,          -- A_exact_duplicate / B_nrt_superseded
    kept_source_file  TEXT                    -- rule A: file of the row that was kept
);

-- ---------------------------------------------------------------------------
-- Per-run data quality metrics (appended, never truncated).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta.dq_metrics (
    metric_id      BIGSERIAL   PRIMARY KEY,
    run_id         INT         NOT NULL,      -- meta.event_log.log_key
    step_order     INT         NOT NULL,
    rule           TEXT        NOT NULL,
    src_sensor     TEXT        NOT NULL,      -- 'ALL' = total over sensors
    rows_before    BIGINT      NOT NULL,
    rows_affected  BIGINT      NOT NULL,      -- removed (dedup) or flagged (flags)
    rows_after     BIGINT      NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_dq_metrics_run ON meta.dq_metrics (run_id);

COMMENT ON TABLE stage.fire_detection_clean   IS 'Typed, conformed, deduplicated FIRMS detections; rebuilt by controllers/clean_stage.py';
COMMENT ON TABLE stage.fire_detection_removed IS 'Rows removed by dedup rule A (exact duplicate) or B (NRT superseded by SP)';
COMMENT ON TABLE meta.dq_metrics              IS 'Before/after row counts per cleaning run, rule and sensor';
