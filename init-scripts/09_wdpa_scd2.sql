-- ============================================================================
-- 09_wdpa_scd2.sql : WDPA protected areas, SCD Type 2 dimension, fire assignment
-- ============================================================================
-- Loaded by controllers/load_wdpa.py (polygons, points, monthly attribute
-- snapshots, dds.dim_protected_area) and controllers/locate_protected_area.py
-- (point-in-time spatial join).
--
-- dds.dim_protected_area (03) is ALTERED here, not replaced: it is empty and
-- dds.fact_active_fire already references it. Changes: effective_timestamp /
-- expiry_timestamp -> effective_from / effective_to (DATE, effective_to
-- exclusive), version_no and the other SCD2 columns, unique (site_id, version_no),
-- and the geometry column is DROPPED: WDPA geometry exists for the current
-- release only, so storing it on historical versions would suggest historical
-- boundaries we do not have. Polygons live in stage.wdpa_polygon.
--
-- Legacy init-scripts/04_spatial_join.sql references dim_protected_area.geom and
-- idx_dds_pa_geom; it is superseded and should be removed in the cleanup.
--
-- Safe to re-run: IF NOT EXISTS, and the ALTERs check the current state first.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Current WDPA release (Thailand). Keys are SITE_ID (= SITE_PID = old WDPAID).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stage.wdpa_polygon (
    site_id     BIGINT PRIMARY KEY,
    site_pid    TEXT NOT NULL,
    site_type   TEXT,                          -- PA / OECM
    name_eng    TEXT,
    name        TEXT,
    desig       TEXT,
    desig_eng   TEXT,
    desig_type  TEXT,                          -- National / International
    iucn_cat    TEXT,
    realm       TEXT,
    rep_area    NUMERIC,
    gis_area    NUMERIC,
    status      TEXT,
    status_yr   INT,
    gov_type    TEXT,
    own_type    TEXT,
    mang_auth   TEXT,
    verif       TEXT,
    release     TEXT NOT NULL,                 -- e.g. Sep2026
    geom        GEOMETRY(MultiPolygon, 4326) NOT NULL
);

-- Point-only records: no boundary, so they cannot be used in the spatial join.
CREATE TABLE IF NOT EXISTS stage.wdpa_point (
    LIKE stage.wdpa_polygon EXCLUDING ALL
);
ALTER TABLE stage.wdpa_point DROP COLUMN IF EXISTS gis_area;
DO $$ BEGIN
    IF (SELECT format_type(atttypid, atttypmod) FROM pg_attribute
         WHERE attrelid = 'stage.wdpa_point'::regclass AND attname = 'geom') <> 'geometry(MultiPoint,4326)' THEN
        ALTER TABLE stage.wdpa_point ALTER COLUMN geom TYPE GEOMETRY(MultiPoint, 4326);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'wdpa_point_pkey') THEN
        ALTER TABLE stage.wdpa_point ADD CONSTRAINT wdpa_point_pkey PRIMARY KEY (site_id);
    END IF;
END $$;

-- Subdivided polygons (<= 256 vertices) for the point-in-polygon join.
CREATE TABLE IF NOT EXISTS stage.wdpa_polygon_piece (
    piece_id    SERIAL PRIMARY KEY,
    site_id     BIGINT NOT NULL,
    geom        GEOMETRY(Polygon, 4326) NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_wdpa_polygon_piece_geom ON stage.wdpa_polygon_piece USING GIST (geom);

-- ---------------------------------------------------------------------------
-- Monthly attribute snapshots (Earth Engine WCMC/WDPA/<yyyymm>/polygons), as
-- delivered (TEXT), plus the recovered key and a completeness flag.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stage.wdpa_snapshot (
    snapshot_month     DATE    NOT NULL,       -- 1st of the snapshot month
    site_id            BIGINT  NOT NULL,
    site_id_source     TEXT    NOT NULL,       -- 'id' (WDPA_PID) / 'name_match' (NAME + DESIG_ENG)
    snapshot_complete  BOOLEAN NOT NULL,       -- false for partial exports (< 90% of previous)
    wdpa_pid TEXT, wdpaid TEXT, name TEXT, desig_eng TEXT, desig_type TEXT, iucn_cat TEXT,
    marine TEXT, rep_area TEXT, gis_area TEXT, status TEXT, status_yr TEXT, gov_type TEXT,
    own_type TEXT, mang_auth TEXT, verif TEXT, iso3 TEXT, parent_iso3 TEXT,
    source_file        TEXT    NOT NULL,
    PRIMARY KEY (snapshot_month, site_id)
);

-- ---------------------------------------------------------------------------
-- dds.dim_protected_area -> SCD Type 2 (alter the empty table from 03).
-- ---------------------------------------------------------------------------
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'dds'
                AND table_name = 'dim_protected_area' AND column_name = 'effective_timestamp') THEN
        ALTER TABLE dds.dim_protected_area RENAME COLUMN effective_timestamp TO effective_from;
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'dds'
                AND table_name = 'dim_protected_area' AND column_name = 'expiry_timestamp') THEN
        ALTER TABLE dds.dim_protected_area RENAME COLUMN expiry_timestamp TO effective_to;
    END IF;
END $$;

ALTER TABLE dds.dim_protected_area
    ALTER COLUMN effective_from TYPE DATE USING effective_from::date,
    ALTER COLUMN effective_to   TYPE DATE USING effective_to::date,
    ALTER COLUMN effective_to   SET NOT NULL,
    ALTER COLUMN is_current     SET NOT NULL,
    ALTER COLUMN site_id        TYPE BIGINT,
    ALTER COLUMN name_eng       TYPE TEXT,
    ALTER COLUMN desig_eng      TYPE TEXT,
    ALTER COLUMN iucn_cat       TYPE TEXT,
    ALTER COLUMN gov_type       TYPE TEXT,
    ALTER COLUMN rep_area       TYPE NUMERIC,
    DROP COLUMN IF EXISTS geom,
    ADD COLUMN IF NOT EXISTS version_no     INT,
    ADD COLUMN IF NOT EXISTS site_pid       TEXT,
    ADD COLUMN IF NOT EXISTS site_type      TEXT,
    ADD COLUMN IF NOT EXISTS name           TEXT,
    ADD COLUMN IF NOT EXISTS desig_type     TEXT,
    ADD COLUMN IF NOT EXISTS status         TEXT,
    ADD COLUMN IF NOT EXISTS status_yr      INT,
    ADD COLUMN IF NOT EXISTS realm          TEXT,
    ADD COLUMN IF NOT EXISTS gis_area       NUMERIC,
    ADD COLUMN IF NOT EXISTS mang_auth      TEXT,
    ADD COLUMN IF NOT EXISTS has_polygon    BOOLEAN,
    ADD COLUMN IF NOT EXISTS record_source  TEXT,     -- history / current_release_point / member
    ADD COLUMN IF NOT EXISTS tracked_hash   TEXT,     -- md5 of the SCD2-tracked attributes
    ADD COLUMN IF NOT EXISTS run_id         INT,
    ADD COLUMN IF NOT EXISTS updated_at     TIMESTAMPTZ DEFAULT now();

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_dim_pa_site_version') THEN
        ALTER TABLE dds.dim_protected_area
            ADD CONSTRAINT uq_dim_pa_site_version UNIQUE (site_id, version_no);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_dim_pa_site_dates ON dds.dim_protected_area (site_id, effective_from, effective_to);

-- ---------------------------------------------------------------------------
-- Fire -> protected area. Bridge: every membership; primary: one per detection.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stage.fire_detection_pa_bridge (
    detection_id        TEXT   NOT NULL,
    site_id             BIGINT NOT NULL,
    protected_area_key  INT    NOT NULL,       -- version valid on acq_date
    run_id              INT    NOT NULL,
    PRIMARY KEY (detection_id, protected_area_key)
);

CREATE TABLE IF NOT EXISTS stage.fire_detection_protected_area (
    detection_id        TEXT     PRIMARY KEY,  -- every clean detection
    protected_area_key  INT      NOT NULL,     -- primary park version; 0 outside PA; -1 not assessed
    n_parks             SMALLINT NOT NULL,     -- memberships in the bridge
    run_id              INT      NOT NULL
);

COMMENT ON TABLE stage.wdpa_snapshot IS 'Monthly WDPA attribute snapshots (Earth Engine), key recovered, completeness flagged';
COMMENT ON TABLE stage.fire_detection_pa_bridge IS 'Every (detection, protected-area version) membership, point in time';
COMMENT ON TABLE stage.fire_detection_protected_area IS 'Primary protected_area_key per clean detection (0 = outside PA, -1 = outside Thailand)';
COMMENT ON TABLE dds.dim_protected_area IS 'Protected areas, SCD Type 2 from monthly WDPA snapshots; geometry in stage.wdpa_polygon (current release only)';
