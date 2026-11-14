-- ============================================================================
-- 11_ml_mining.sql : output tables of the two mining jobs
-- ============================================================================
-- Docs: docs/mining.md.
--
--   ml_fire_event              one row per fire event        controllers/mine_fire_events.py
--   ml_fire_event_detection    detection -> event (or noise) controllers/mine_fire_events.py
--   ml_fire_event_silhouette   sampled silhouette values     controllers/mine_fire_events.py
--   ml_district_month_anomaly  district x month, scored      controllers/mine_district_anomalies.py
--
-- All four are rebuilt in full by their job (truncate + insert, one transaction).
-- They supersede dds.ml_fire_anomalies (05), which stays, empty and unused.
--
-- No foreign keys to fact_active_fire: load_dds.py truncates and reloads the
-- fact every run, and detection_id is stable across reloads.
--
-- Safe to re-run: IF NOT EXISTS everywhere.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- ml_fire_event: one space-time cluster of detections
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dds.ml_fire_event (
    event_id            BIGINT      PRIMARY KEY,  -- fire_year_start * 1,000,000 + sequence in start order
    fire_year_start     INT         NOT NULL,     -- Aug-Jul fire year the event was clustered in
    start_ts            TIMESTAMPTZ NOT NULL,     -- first detection (UTC instant)
    end_ts              TIMESTAMPTZ NOT NULL,     -- last detection
    start_date_key      INT         NOT NULL,     -- Thai local date of the first detection -> dim_date
    end_date_key        INT         NOT NULL,
    duration_days       NUMERIC     NOT NULL,     -- end_ts - start_ts
    n_detections        INT         NOT NULL,
    n_core              INT         NOT NULL,     -- DBSCAN core points
    n_overpasses        INT         NOT NULL,     -- distinct (satellite, acquisition time)
    n_satellites        INT         NOT NULL,     -- distinct satellites that saw it
    frp_max_mw          NUMERIC,
    frp_mean_mw         NUMERIC,
    frp_sum_mw          NUMERIC,                  -- over all detections: one fire seen on n overpasses counts n times
    longitude           NUMERIC     NOT NULL,     -- mean of the detections
    latitude            NUMERIC     NOT NULL,
    geom                GEOMETRY(Point, 4326) NOT NULL,
    extent_km           NUMERIC     NOT NULL,     -- diagonal of the bounding box (UTM 47N)
    location_key        INT         NOT NULL,     -- district holding most of its detections
    n_districts         INT         NOT NULL,
    protected_area_key  INT         NOT NULL,     -- park version holding most of its in-park detections; 0 = none
    n_in_protected_area INT         NOT NULL,     -- detections with protected_area_key > 0
    run_id              INT         NOT NULL      -- meta.event_log.log_key
);
CREATE INDEX IF NOT EXISTS idx_ml_event_start_date ON dds.ml_fire_event (start_date_key);
CREATE INDEX IF NOT EXISTS idx_ml_event_location   ON dds.ml_fire_event (location_key);
CREATE INDEX IF NOT EXISTS idx_ml_event_geom       ON dds.ml_fire_event USING gist (geom);

-- ---------------------------------------------------------------------------
-- ml_fire_event_detection: every clustered detection (Thailand, dq_pass)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dds.ml_fire_event_detection (
    detection_id  TEXT    PRIMARY KEY,          -- = fact_active_fire.detection_id
    date_key      INT     NOT NULL,             -- with detection_id: joins the fact with partition pruning
    event_id      BIGINT,                       -- NULL for noise
    role          TEXT    NOT NULL CHECK (role IN ('core', 'border', 'noise')),
    run_id        INT     NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ml_event_det_event ON dds.ml_fire_event_detection (event_id);

-- ---------------------------------------------------------------------------
-- ml_fire_event_silhouette: silhouette of a random sample of clustered
-- detections per fire year; join to the fact and dimensions to slice it
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dds.ml_fire_event_silhouette (
    detection_id  TEXT     PRIMARY KEY,
    date_key      INT      NOT NULL,
    event_id      BIGINT   NOT NULL,
    silhouette    NUMERIC  NOT NULL,            -- -1 .. 1
    is_exact      BOOLEAN  NOT NULL,            -- false: nearest other event not proven (value is an upper bound)
    run_id        INT      NOT NULL
);

-- ---------------------------------------------------------------------------
-- ml_district_month_anomaly: Isolation Forest on district x month aggregates
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dds.ml_district_month_anomaly (
    location_key      INT      NOT NULL,        -- dim_location (district)
    year_month        INT      NOT NULL,        -- YYYYMM, Thai local time
    month_start       DATE     NOT NULL,
    fire_year_start   INT      NOT NULL,
    n_detections      INT      NOT NULL,        -- Suomi-NPP VIIRS only, dq_pass
    n_in_pa           INT      NOT NULL,        -- of those, in a Designated / Inscribed protected area
    pa_share          NUMERIC  NOT NULL,
    frp_sum_mw        NUMERIC  NOT NULL,
    frp_max_mw        NUMERIC  NOT NULL,
    night_share       NUMERIC  NOT NULL,
    n_prior_years     INT      NOT NULL,        -- earlier years with the same calendar month
    expected_log_n    NUMERIC  NOT NULL,        -- mean of log1p(n) over those years
    dev_log_n         NUMERIC  NOT NULL,        -- log1p(n) - expected_log_n
    z_n               NUMERIC  NOT NULL,        -- dev_log_n / max(std over those years, 0.5)
    anomaly_score     NUMERIC  NOT NULL,        -- Isolation Forest decision function; < 0 = anomaly
    is_anomaly        BOOLEAN  NOT NULL,
    anomaly_rank      INT      NOT NULL,        -- 1 = most anomalous
    direction         TEXT     NOT NULL CHECK (direction IN ('high', 'low')),  -- more or less burning than expected
    top_feature       TEXT     NOT NULL,        -- feature furthest from its training mean (in training std)
    split             TEXT     NOT NULL CHECK (split IN ('train', 'test')),
    run_id            INT      NOT NULL,
    PRIMARY KEY (location_key, year_month)
);
CREATE INDEX IF NOT EXISTS idx_ml_dma_anomaly ON dds.ml_district_month_anomaly (is_anomaly, anomaly_rank);

COMMENT ON TABLE dds.ml_fire_event IS 'Fire events: ST-DBSCAN clusters of Thai dq_pass detections (mine_fire_events.py)';
COMMENT ON TABLE dds.ml_fire_event_detection IS 'Event (or noise) of every Thai dq_pass detection';
COMMENT ON TABLE dds.ml_fire_event_silhouette IS 'Silhouette of sampled clustered detections, per fire year';
COMMENT ON TABLE dds.ml_district_month_anomaly IS 'District x month burning scored by Isolation Forest (mine_district_anomalies.py)';
COMMENT ON TABLE dds.ml_fire_anomalies IS 'SUPERSEDED by the ml_* tables of 11_ml_mining.sql; empty, unused';
