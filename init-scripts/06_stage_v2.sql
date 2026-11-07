-- ============================================================================
-- 06_stage_v2.sql : raw-shaped staging for FIRMS fire detections
-- ============================================================================
-- The raw_firms_* tables REPLACE the old typed stage.stg_modis_fire and
-- stage.stg_viirs_fire tables (02_create_staging_tables.sql). The old tables
-- are kept for now and will be dropped in a later cleanup.
--
-- Why all TEXT: staging holds data exactly as it arrived. A typed column would
-- make one bad value abort a whole COPY; casting and cleansing happen in the
-- stage -> NDS step, where bad values can be flagged instead.
--
-- Everything here is additive and safe to re-run (IF NOT EXISTS).
-- Loaded by controllers/load_stage.py.
-- ============================================================================

-- 1. MODIS detections (SP + NRT). NRT files have no `type` column -> NULL.
CREATE TABLE IF NOT EXISTS stage.raw_firms_modis (
    latitude          TEXT,
    longitude         TEXT,
    brightness        TEXT,
    scan              TEXT,
    track             TEXT,
    acq_date          TEXT,
    acq_time          TEXT,
    satellite         TEXT,
    instrument        TEXT,
    confidence        TEXT,      -- numeric 0-100 for MODIS
    version           TEXT,
    bright_t31        TEXT,
    frp               TEXT,
    daynight          TEXT,
    type              TEXT,      -- NULL for NRT rows
    src_sensor        TEXT,
    src_window_start  TEXT,
    src_ingested_at   TEXT,
    -- lineage
    source_file       TEXT NOT NULL,   -- path relative to the raw/ folder
    product           TEXT NOT NULL CHECK (product IN ('SP', 'NRT')),
    load_run_id       INT  NOT NULL,   -- meta.event_log.log_key of the run
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 2. VIIRS detections (SNPP, NOAA-20, NOAA-21; SP + NRT).
CREATE TABLE IF NOT EXISTS stage.raw_firms_viirs (
    latitude          TEXT,
    longitude         TEXT,
    bright_ti4        TEXT,
    scan              TEXT,
    track             TEXT,
    acq_date          TEXT,
    acq_time          TEXT,
    satellite         TEXT,
    instrument        TEXT,
    confidence        TEXT,      -- l / n / h for VIIRS
    version           TEXT,
    bright_ti5        TEXT,
    frp               TEXT,
    daynight          TEXT,
    type              TEXT,      -- NULL for NRT rows
    src_sensor        TEXT,
    src_window_start  TEXT,
    src_ingested_at   TEXT,
    source_file       TEXT NOT NULL,
    product           TEXT NOT NULL CHECK (product IN ('SP', 'NRT')),
    load_run_id       INT  NOT NULL,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The loader deletes a file's rows before reloading it; without these the
-- DELETE ... WHERE source_file = ? would scan the whole table every time.
CREATE INDEX IF NOT EXISTS idx_raw_firms_modis_source_file ON stage.raw_firms_modis (source_file);
CREATE INDEX IF NOT EXISTS idx_raw_firms_viirs_source_file ON stage.raw_firms_viirs (source_file);

-- 3. Manifest: one row per file successfully processed. A rerun skips files
--    whose size and mtime are unchanged -> reruns never duplicate rows.
CREATE TABLE IF NOT EXISTS stage.load_file_manifest (
    source_file    TEXT PRIMARY KEY,
    target_table   TEXT        NOT NULL,
    sensor         TEXT        NOT NULL,
    product        TEXT        NOT NULL,
    file_size      BIGINT      NOT NULL,
    file_mtime_ns  BIGINT      NOT NULL,
    rows_loaded    INT         NOT NULL,
    rows_rejected  INT         NOT NULL,
    load_run_id    INT         NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 4. Rejected lines, kept for inspection instead of silently dropped.
CREATE TABLE IF NOT EXISTS stage.raw_firms_reject (
    reject_id    BIGSERIAL PRIMARY KEY,
    source_file  TEXT        NOT NULL,
    line_no      INT,                  -- 1 = header line
    reason       TEXT        NOT NULL,
    raw_line     TEXT,
    load_run_id  INT         NOT NULL,
    rejected_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_raw_firms_reject_source_file ON stage.raw_firms_reject (source_file);

-- 5. Run-level audit: rows loaded go in the existing rows_processed column.
ALTER TABLE meta.event_log ADD COLUMN IF NOT EXISTS files_loaded  INT DEFAULT 0;
ALTER TABLE meta.event_log ADD COLUMN IF NOT EXISTS rows_rejected INT DEFAULT 0;

COMMENT ON TABLE stage.raw_firms_modis IS 'Raw FIRMS MODIS detections (SP+NRT), all TEXT; replaces stage.stg_modis_fire';
COMMENT ON TABLE stage.raw_firms_viirs IS 'Raw FIRMS VIIRS detections (SP+NRT), all TEXT; replaces stage.stg_viirs_fire';
COMMENT ON TABLE stage.load_file_manifest IS 'Files loaded by controllers/load_stage.py; drives idempotent reruns';
COMMENT ON TABLE stage.raw_firms_reject IS 'Lines rejected by controllers/load_stage.py, with reason';
