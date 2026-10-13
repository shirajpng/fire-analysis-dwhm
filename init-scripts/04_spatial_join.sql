-- ============================================================================
-- STEP 4: SPATIAL POINT-IN-POLYGON JOIN & INDEX OPTIMIZATION
-- ============================================================================

-- 1. Re-index PostGIS Spatial Indices for Maximum Query Throughput
REINDEX INDEX "dds" . "idx_dds_loc_geom";
REINDEX INDEX "dds" . "idx_dds_pa_geom";

-- 2. Perform Spatial Point-in-Polygon Join (ST_Within)
-- Maps each fire location point to its corresponding conservation polygon
UPDATE "dds" . "fact_active_fire" f
SET "protected_area_key" = COALESCE(pa."protected_area_key", 0)
FROM "dds" . "dim_location_spatial" loc
LEFT JOIN "dds" . "dim_protected_area" pa 
  ON ST_Within(loc."geom", pa."geom") AND pa."is_current" = TRUE
WHERE f."location_key" = loc."location_key";

-- 3. Log Audit Metadata Event
INSERT INTO "meta" . "event_log" ("event_type", "event_category", "rows_processed", "status", "note")
SELECT 
    'SPATIAL_JOIN_UPDATE', 
    'POSTGIS_ST_WITHIN', 
    COUNT(*), 
    'SUCCESS', 
    'Updated fact_active_fire protected_area_keys using ST_Within spatial intersection'
FROM "dds" . "fact_active_fire"
WHERE "protected_area_key" > 0;
