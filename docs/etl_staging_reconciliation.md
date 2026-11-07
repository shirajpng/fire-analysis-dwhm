# ETL Staging Load: Reconciliation

Staging load of NASA FIRMS active-fire detections (Thailand) from the raw landing
zone (`data-extraction-scripts/raw/firms/{sp,nrt}`) into
`stage.raw_firms_modis` and `stage.raw_firms_viirs`, by `controllers/load_stage.py`
(tables defined in `init-scripts/06_stage_v2.sql`). Loaded 2026-11-07.

## Row counts per sensor vs. inventory

| Sensor | Product | Years | Files | Inventory rows | Rows in staging | Rejected | Diff |
|---|---|---|---:|---:|---:|---:|---:|
| MODIS_NRT | NRT | 2026 | 16 | 522 | 522 | 0 | 0 |
| MODIS_SP | SP | 2012–2026 | 1,055 | 1,365,550 | 1,365,550 | 0 | 0 |
| VIIRS_NOAA20_NRT | NRT | 2026 | 16 | 3,072 | 3,072 | 0 | 0 |
| VIIRS_NOAA20_SP | SP | 2018–2026 | 603 | 3,870,295 | 3,870,295 | 0 | 0 |
| VIIRS_NOAA21_NRT | NRT | 2024–2026 | 195 | 1,132,547 | 1,132,547 | 0 | 0 |
| VIIRS_SNPP_NRT | NRT | 2026 | 16 | 2,861 | 2,861 | 0 | 0 |
| VIIRS_SNPP_SP | SP | 2012–2026 | 1,047 | 7,201,715 | 7,201,715 | 0 | 0 |
| **Total** | | | **2,948** | **13,576,562** | **13,576,562** | **0** | **0** |

`Diff = rows in staging + rejected − inventory rows`. Inventory rows come from
`data-extraction-scripts/inventory.py` (lines per CSV minus the header).

## Load runs

| Run | Scope | Files loaded | Rows loaded | Duration | Throughput |
|---|---|---:|---:|---:|---:|
| 2020 test | `--year 2020` | 220 | 1,389,748 | 11 s | ≈ 126,000 rows/s |
| Full load | all years (2020 and 2026 skipped, already loaded) | 2,525 | 10,857,965 | 168 s | ≈ 64,600 rows/s |

2026 (203 files, 1,328,849 rows) was loaded in a separate `--year 2026` run before
the full load. 2020 + 2026 + full load = 13,576,562 rows. Re-running the 2020 and
2026 loads skipped every file and added no rows, which confirms reruns are safe.

## Checks

- **Completeness:** the inventory, the rows in the staging tables and the per-file
  counts in `stage.load_file_manifest` all match for every sensor.
- **Rejects:** 0 rows in `stage.raw_firms_reject`.
- **Files:** 2,948 non-empty CSVs loaded; 56 empty (0-byte) files skipped.
- **Schema handling:** NRT files have no `type` column, so `type` is NULL for all
  1,139,002 NRT rows and filled in for all 12,437,560 SP rows.
- **Audit:** each run is recorded in `meta.event_log` (files loaded, rows loaded,
  rows rejected, status).

## Method

- **Bulk load:** one Postgres `COPY ... FROM STDIN` per file (psycopg2
  `copy_expert`), not row-by-row `INSERT`.
- **Raw-shaped staging:** all source columns are stored as `TEXT`, plus lineage
  columns `source_file`, `product` (`SP`/`NRT`) and `load_run_id`. Type casting
  and cleansing happen in the next step (stage → NDS).
- **Validation:** each file's header must match one of the four known schemas
  (MODIS or VIIRS, with or without `type`). A row with the wrong number of fields
  is written to `stage.raw_firms_reject` with its reason instead of being dropped.
- **Safe reruns:** each file is loaded in one transaction (delete that file's
  old rows → COPY → record it in the manifest). A file whose size and mtime are
  unchanged is skipped on the next run. `--full-reload` truncates the tables and
  reloads everything.

## Reconciliation query

```sql
WITH inventory(sensor, expected) AS (VALUES
  ('MODIS_NRT',522), ('MODIS_SP',1365550), ('VIIRS_NOAA20_NRT',3072), ('VIIRS_NOAA20_SP',3870295),
  ('VIIRS_NOAA21_NRT',1132547), ('VIIRS_SNPP_NRT',2861), ('VIIRS_SNPP_SP',7201715)),
loaded AS (
  SELECT sensor, count(*) AS files, sum(rows_rejected) AS rejected
  FROM stage.load_file_manifest GROUP BY sensor),
in_db AS (
  SELECT src_sensor AS sensor, count(*) AS in_db FROM (
    SELECT src_sensor FROM stage.raw_firms_modis
    UNION ALL SELECT src_sensor FROM stage.raw_firms_viirs) t GROUP BY 1)
SELECT i.sensor, i.expected, l.files, d.in_db, l.rejected,
       d.in_db + l.rejected - i.expected AS diff
FROM inventory i
LEFT JOIN in_db d USING (sensor)
LEFT JOIN loaded l USING (sensor)
ORDER BY i.sensor;
```
