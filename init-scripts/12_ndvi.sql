-- ============================================================================
-- 12_ndvi.sql : NDVI (MOD13Q1, 16-day, 250 m) staging, cleaning and imputation
-- ============================================================================
-- Docs: docs/etl_ndvi.md.
--
--   stage.raw_ndvi            AppEEARS point samples as delivered   controllers/load_ndvi.py
--   stage.raw_ndvi_reject     lines the loader could not load       controllers/load_ndvi.py
--   stage.ndvi_point          one row per grid point                controllers/impute_ndvi.py
--   stage.ndvi_clean          point x composite, gaps imputed       controllers/impute_ndvi.py
--   meta.ndvi_imputation_eval error of each method on held-out data controllers/impute_ndvi.py
--
-- Loaded files are recorded in the existing stage.load_file_manifest (06) with
-- target_table = 'stage.raw_ndvi'. `load_stage.py --full-reload` truncates that
-- manifest; the next load_ndvi.py run then reloads every NDVI file, which gives
-- the same rows (a file's old rows are deleted before it is loaded).
--
-- Additive and safe to re-run (IF NOT EXISTS).
-- ============================================================================

-- ---------------------------------------------------------------------------
-- raw_ndvi: as delivered, all TEXT (same reason as raw_firms_*, see 06)
-- ---------------------------------------------------------------------------
-- An AppEEARS file has 33 columns. The 11 source columns are kept. The other 22
-- are the two quality columns again, decoded bit by bit into text
-- (..._VI_Quality_MODLAND_Description and so on); they add nothing and would
-- triple the table.
CREATE TABLE IF NOT EXISTS stage.raw_ndvi (
    category           TEXT,
    id                 TEXT,      -- grid point, e.g. p000123
    latitude           TEXT,
    longitude          TEXT,
    date               TEXT,      -- first day of the 16-day composite
    modis_tile         TEXT,
    line_y_250m        TEXT,
    sample_x_250m      TEXT,
    ndvi               TEXT,      -- already scaled by AppEEARS; -3000 = fill
    vi_quality         TEXT,      -- QA bit field
    pixel_reliability  TEXT,      -- -1 fill, 0 good, 1 marginal, 2 snow/ice, 3 cloudy
    -- lineage
    source_file        TEXT NOT NULL,   -- path relative to the raw/ folder
    load_run_id        INT  NOT NULL,   -- meta.event_log.log_key of the run
    loaded_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_raw_ndvi_source_file ON stage.raw_ndvi (source_file);

CREATE TABLE IF NOT EXISTS stage.raw_ndvi_reject (
    reject_id    BIGSERIAL PRIMARY KEY,
    source_file  TEXT        NOT NULL,
    line_no      INT,                  -- 1 = header line
    reason       TEXT        NOT NULL,
    raw_line     TEXT,
    load_run_id  INT         NOT NULL,
    rejected_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_raw_ndvi_reject_source_file ON stage.raw_ndvi_reject (source_file);

-- ---------------------------------------------------------------------------
-- ndvi_point: the 0.1 degree sampling grid
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stage.ndvi_point (
    point_id       TEXT    PRIMARY KEY,
    latitude       NUMERIC NOT NULL,
    longitude      NUMERIC NOT NULL,
    geom           GEOMETRY(Point, 4326) NOT NULL,
    modis_tile     TEXT,
    -- same rules and codes as stage.fire_detection_location -> dds.dim_location
    location_code  TEXT    NOT NULL,
    in_thailand    BOOLEAN NOT NULL,
    -- land:    imputed
    -- sparse:  too few valid composites to impute (water edge); kept, gaps stay NULL
    -- no_data: fill value in every composite (sea); has no rows in ndvi_clean
    point_class    TEXT    NOT NULL CHECK (point_class IN ('land', 'sparse', 'no_data')),
    n_composites   INT     NOT NULL,
    n_valid        INT     NOT NULL,   -- good + marginal
    n_cloud        INT     NOT NULL,
    n_fill         INT     NOT NULL,
    n_imputed      INT     NOT NULL,
    run_id         INT     NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ndvi_point_geom     ON stage.ndvi_point USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_ndvi_point_location ON stage.ndvi_point (location_code);

-- ---------------------------------------------------------------------------
-- ndvi_clean: one row per point and composite; `ndvi` is the value to analyse
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stage.ndvi_clean (
    point_id           TEXT     NOT NULL,
    composite_date     DATE     NOT NULL,
    ndvi_observed      NUMERIC(6,4),       -- as delivered; NULL for the fill value
    pixel_reliability  SMALLINT,
    vi_quality         INT,
    obs_class          TEXT     NOT NULL
        CHECK (obs_class IN ('good', 'marginal', 'snow_ice', 'cloud', 'fill', 'invalid')),
    is_valid           BOOLEAN  NOT NULL,  -- good or marginal: the observed value is used
    ndvi               NUMERIC(6,4),       -- observed if valid, else imputed; NULL only on sparse points
    is_imputed         BOOLEAN  NOT NULL,
    gap_length         SMALLINT,           -- composites in the run of missing values this row is in
    source_file        TEXT     NOT NULL,
    run_id             INT      NOT NULL
);
-- Primary key and index are dropped and re-created by impute_ndvi.py around its bulk insert.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ndvi_clean_pkey') THEN
        ALTER TABLE stage.ndvi_clean ADD CONSTRAINT ndvi_clean_pkey PRIMARY KEY (point_id, composite_date);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_ndvi_clean_date ON stage.ndvi_clean (composite_date);

-- ---------------------------------------------------------------------------
-- ndvi_imputation_eval: valid observations hidden, imputed, compared
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta.ndvi_imputation_eval (
    run_id      INT     NOT NULL,
    method      TEXT    NOT NULL,   -- seasonal_anomaly is the one used; the others are baselines
    slice       TEXT    NOT NULL,   -- all | all_real_gap_mix | gap <length> | season <Thai season>
    n_heldout   INT     NOT NULL,
    rmse        NUMERIC NOT NULL,
    mae         NUMERIC NOT NULL,
    bias        NUMERIC NOT NULL,   -- mean of (imputed - observed)
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, method, slice)
);

COMMENT ON TABLE stage.raw_ndvi   IS 'Raw AppEEARS MOD13Q1 NDVI point samples, all TEXT; loaded by controllers/load_ndvi.py';
COMMENT ON TABLE stage.ndvi_point IS 'NDVI sampling grid points with location and data class; rebuilt by controllers/impute_ndvi.py';
COMMENT ON TABLE stage.ndvi_clean IS 'NDVI per point and 16-day composite, deduplicated, gaps imputed; rebuilt by controllers/impute_ndvi.py';
COMMENT ON TABLE meta.ndvi_imputation_eval IS 'Hold-out error of the NDVI imputation methods, per run';
