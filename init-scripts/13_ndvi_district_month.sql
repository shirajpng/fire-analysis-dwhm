-- ============================================================================
-- 13_ndvi_district_month.sql : NDVI per Thai district and month, and the NDVI
--                              features of the district-month anomaly model
-- ============================================================================
-- Docs: docs/etl_ndvi.md (table), docs/mining.md (features).
--
--   dds.fact_ndvi_district_month   stage.ndvi_clean aggregated   controllers/load_ndvi_dds.py
--   dds.ml_district_month_anomaly  + ndvi_lag1, dev_ndvi_lag1    controllers/mine_district_anomalies.py
--
-- Additive and safe to re-run.
-- ============================================================================

-- One row per Thai location of dim_location (928 districts + THA_NO_DISTRICT)
-- and month. Conformed: location_key -> dim_location; year_month is the same
-- YYYYMM as in ml_district_month_anomaly and as date_key / 100 on the fact.
--
-- A composite counts for the month its 16-day window ENDS in, so a month's
-- value never uses a day of a later month.
CREATE TABLE IF NOT EXISTS dds.fact_ndvi_district_month (
    location_key    INT          NOT NULL REFERENCES dds.dim_location (location_key),
    year_month      INT          NOT NULL,     -- YYYYMM
    month_start     DATE         NOT NULL,
    ndvi_mean       NUMERIC(6,4) NOT NULL,     -- mean over the points and composites of the month
    imputed_share   NUMERIC(5,4) NOT NULL,     -- share of those values that were imputed
    n_points        INT          NOT NULL,     -- land grid points behind the mean
    n_values        INT          NOT NULL,     -- point x composite values behind the mean
    -- district: the district's own points; province / country: the district has
    -- no grid point, so the mean of its province (or of Thailand) is used
    source          TEXT         NOT NULL CHECK (source IN ('district', 'province', 'country')),
    run_id          INT          NOT NULL,
    PRIMARY KEY (location_key, year_month)
);

-- NDVI features of the anomaly model. Nullable: existing rows have none until
-- mine_district_anomalies.py is rerun.
ALTER TABLE dds.ml_district_month_anomaly ADD COLUMN IF NOT EXISTS ndvi_lag1     NUMERIC;  -- district NDVI of the month before
ALTER TABLE dds.ml_district_month_anomaly ADD COLUMN IF NOT EXISTS dev_ndvi_lag1 NUMERIC;  -- ndvi_lag1 - its mean over the earlier years, same calendar month

COMMENT ON TABLE dds.fact_ndvi_district_month IS 'Mean NDVI per Thai district and month from stage.ndvi_clean (load_ndvi_dds.py)';
