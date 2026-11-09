-- ============================================================================
-- 08_gadm_location.sql : GADM admin boundaries, detection -> location, dim_location
-- ============================================================================
-- Loaded by controllers/load_reference.py (GADM polygons, NASA country archive,
-- dds.dim_location) and controllers/locate_stage.py (spatial join, validation).
--
-- dds.dim_location REPLACES dds.dim_location_spatial (03): that table has one row
-- per point, which with 13.57M distinct fire locations would be as large as the
-- fact table. dim_location is at DISTRICT grain; the point itself (lat/lon/geom,
-- detection_id) stays on the fact row. dim_location_spatial is kept for now and
-- will be dropped in a later cleanup.
--
-- Additive and safe to re-run (IF NOT EXISTS).
-- ============================================================================

-- ---------------------------------------------------------------------------
-- GADM 4.1 polygons. Keys are GADM GIDs, never names: 11 Thai district names
-- occur in more than one province.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stage.gadm_country (            -- level 0: Thailand + 5 neighbours
    gid_0         TEXT PRIMARY KEY,                        -- THA, MMR, LAO, KHM, MYS, VNM
    country_name  TEXT NOT NULL,
    geom          GEOMETRY(MultiPolygon, 4326) NOT NULL
);

CREATE TABLE IF NOT EXISTS stage.gadm_province (           -- level 1, Thailand only (77)
    gid_1         TEXT PRIMARY KEY,
    gid_0         TEXT NOT NULL,
    name_1        TEXT NOT NULL,
    nl_name_1     TEXT,                                    -- Thai script
    type_1        TEXT,
    engtype_1     TEXT,
    geom          GEOMETRY(MultiPolygon, 4326) NOT NULL
);

CREATE TABLE IF NOT EXISTS stage.gadm_district (           -- level 2, Thailand only (928)
    gid_2         TEXT PRIMARY KEY,
    gid_1         TEXT NOT NULL,
    gid_0         TEXT NOT NULL,
    name_2        TEXT NOT NULL,
    nl_name_2     TEXT,
    type_2        TEXT,                                    -- Amphoe / King Amphoe / Water body
    engtype_2     TEXT,                                    -- District / Minor district / Water body
    geom          GEOMETRY(MultiPolygon, 4326) NOT NULL
);

-- Subdivided copies (ST_Subdivide, max 256 vertices per piece) used for the joins.
-- A GIST index only compares bounding boxes; small pieces have tight boxes and
-- cheap exact tests, where a whole district would be tested vertex by vertex.
CREATE TABLE IF NOT EXISTS stage.gadm_district_piece (
    piece_id      SERIAL PRIMARY KEY,
    gid_2         TEXT NOT NULL,
    geom          GEOMETRY(Polygon, 4326) NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_gadm_district_piece_geom ON stage.gadm_district_piece USING GIST (geom);

CREATE TABLE IF NOT EXISTS stage.gadm_country_piece (
    piece_id      SERIAL PRIMARY KEY,
    gid_0         TEXT NOT NULL,
    geom          GEOMETRY(Polygon, 4326) NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_gadm_country_piece_geom ON stage.gadm_country_piece USING GIST (geom);

-- ---------------------------------------------------------------------------
-- NASA FIRMS country archive (clipped to Thailand by NASA), as delivered.
-- Validation reference only; it is NOT loaded into the warehouse facts.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stage.ref_firms_country (
    latitude TEXT, longitude TEXT,
    brightness TEXT, bright_t31 TEXT,                      -- MODIS files
    bright_ti4 TEXT, bright_ti5 TEXT,                      -- VIIRS files
    scan TEXT, track TEXT, acq_date TEXT, acq_time TEXT, satellite TEXT, instrument TEXT,
    confidence TEXT, version TEXT, frp TEXT, daynight TEXT, type TEXT,
    src_sensor   TEXT NOT NULL,                            -- from the sensor=... folder
    source_file  TEXT NOT NULL
);

-- ---------------------------------------------------------------------------
-- One row per clean detection: where it is. Rebuilt by locate_stage.py.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stage.fire_detection_location (
    detection_id      TEXT PRIMARY KEY,                    -- = stage.fire_detection_clean
    location_code     TEXT NOT NULL,                       -- -> dds.dim_location.location_code
    district_gid      TEXT,                                -- NULL outside Thailand
    province_gid      TEXT,
    country_gid       TEXT,                                -- country polygon hit, NULL = none
    in_thailand       BOOLEAN NOT NULL,
    outside_class     TEXT,                                -- NULL when in Thailand
    clean_run_id      INT NOT NULL,                        -- clean table run this was built from
    run_id            INT NOT NULL
);

-- ---------------------------------------------------------------------------
-- Location dimension, district grain. Upserted on location_code so that
-- location_key stays stable across reruns (fact rows will reference it).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dds.dim_location (
    location_key       SERIAL PRIMARY KEY,
    location_code      TEXT UNIQUE NOT NULL,               -- GID_2, or OUT_<class> members
    district_gid       TEXT,
    district_name      TEXT,
    district_name_th   TEXT,
    district_type      TEXT,                               -- District / Minor district / Water body
    province_gid       TEXT,
    province_name      TEXT,
    province_name_th   TEXT,
    country_code       TEXT,                               -- THA, a neighbour, or NULL (sea / gap)
    country_name       TEXT,
    in_thailand        BOOLEAN NOT NULL,
    outside_class      TEXT,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Validation against the NASA country archive, per run, sensor and year.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta.location_validation (
    run_id                 INT  NOT NULL,
    src_sensor             TEXT NOT NULL,
    year                   INT  NOT NULL,
    nasa_rows              BIGINT,
    ours_in_thailand       BIGINT,                         -- clean rows, in_thailand
    ours_incl_rule_a_dups  BIGINT,                         -- + rule A duplicates in Thailand
    diff_incl_dups         BIGINT,                         -- ours_incl_rule_a_dups - nasa_rows
    matched_ids            BIGINT,                         -- in NASA and ours, ours in_thailand
    matched_ours_outside   BIGINT,                         -- in NASA and ours, ours NOT in_thailand (border disagreement)
    nasa_only_ids          BIGINT,                         -- in NASA, not in our clean data at all
    ours_only_ids          BIGINT,                         -- ours in_thailand, not in NASA
    PRIMARY KEY (run_id, src_sensor, year)
);

COMMENT ON TABLE dds.dim_location IS 'Location dimension at district grain (district -> province -> country); replaces dds.dim_location_spatial';
COMMENT ON TABLE stage.fire_detection_location IS 'Detection -> district/province/country, in_thailand and outside_class; rebuilt by locate_stage.py';
COMMENT ON TABLE stage.ref_firms_country IS 'NASA FIRMS Thailand country archive, validation reference only';
