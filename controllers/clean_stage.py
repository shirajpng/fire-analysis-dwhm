"""
Clean, conform and deduplicate staged FIRMS detections.

    stage.raw_firms_modis + stage.raw_firms_viirs  (all TEXT, from load_stage.py)
        -> stage.fire_detection_clean    typed, conformed, one row per detection_id
        -> stage.fire_detection_removed  rows dropped by rule A / B, with reason
        -> meta.dq_metrics               before/after counts per rule and sensor

Tables: init-scripts/07_clean_dedup.sql. All the work is SQL, run inside ONE
transaction: TRUNCATE the output tables, rebuild them with INSERT ... SELECT,
commit. A rerun therefore produces the same result and never duplicates rows, and
a failure halfway leaves the previous version untouched.

Rules (see docs/etl_cleaning.md):
  1  safe type casting      invalid values -> NULL + cast_fail_<column> flag
  2  conform MODIS/VIIRS    brightness_k, background_k, confidence_pct/_class, satellite
  3  detection_id           md5(satellite | acq_ts UTC | lat 4dp | lon 4dp)
  A  exact duplicates       keep one per detection_id (SP first, then confidence, FRP)
  B  NRT superseded by SP   drop NRT rows inside the SP date range of their satellite
  6  quality flags          type 1/2/3, impossible coords, negative FRP, missing fields
  7  metrics                meta.dq_metrics, one row per run x rule x sensor

Usage:
    python controllers/clean_stage.py
"""

from __future__ import annotations

import os
import sys
import time

import psycopg2

DB = dict(
    host=os.getenv("DWH_HOST", "localhost"),
    port=int(os.getenv("DWH_PORT", "5432")),
    dbname=os.getenv("DWH_DB", "fire_dwh"),
    user=os.getenv("DWH_USER", "dwh_admin"),
    password=os.getenv("DWH_PASSWORD", "admin123"),
)

# Columns of stage.fire_detection_clean filled by this job (created_at has a default).
OUT_COLS = """detection_id, sensor_family, src_sensor, product, satellite, satellite_code,
    instrument, version, acq_date, acq_time_hhmm, acq_ts, latitude, longitude, geom,
    brightness_k, background_k, frp_mw, scan, track, confidence_raw, confidence_pct,
    confidence_class, daynight, fire_type, dq_flags, dq_pass, source_file, run_id"""

# Raw columns, renamed so MODIS and VIIRS line up (rule 2 starts here).
RAW_UNION = """
    SELECT 'MODIS'::text AS sensor_family, src_sensor, product, satellite AS satellite_code,
           instrument, version, latitude AS latitude_raw, longitude AS longitude_raw,
           brightness AS brightness_raw, bright_t31 AS background_raw, frp AS frp_raw,
           scan AS scan_raw, track AS track_raw, acq_date AS acq_date_raw, acq_time AS acq_time_raw,
           confidence AS confidence_raw, daynight, type AS type_raw, source_file
      FROM stage.raw_firms_modis
    UNION ALL
    SELECT 'VIIRS', src_sensor, product, satellite, instrument, version, latitude, longitude,
           bright_ti4, bright_ti5, frp, scan, track, acq_date, acq_time,
           confidence, daynight, type, source_file
      FROM stage.raw_firms_viirs
"""


def steps(run_id: int) -> list[tuple[str, str]]:
    return [
        ("session settings", """
            SET LOCAL work_mem = '256MB';
            SET LOCAL maintenance_work_mem = '1GB';
        """),

        # Rule B input: the date range SP covers, per conformed satellite.
        ("rule B: SP coverage per satellite", f"""
            CREATE TEMP TABLE t_sp_coverage ON COMMIT DROP AS
            SELECT stage.conform_satellite(satellite_code) AS satellite,
                   min(stage.try_date(acq_date_raw)) AS sp_first,
                   max(stage.try_date(acq_date_raw)) AS sp_last
              FROM ({RAW_UNION}) r
             WHERE product = 'SP' AND stage.conform_satellite(satellite_code) IS NOT NULL
             GROUP BY 1;
        """),

        # Rules 1, 2, 3, 6 and the A/B decision, computed once per raw row.
        ("rules 1-3, 6, A, B: typed working set", f"""
            CREATE TEMP TABLE t_typed ON COMMIT DROP AS
            WITH raw AS ({RAW_UNION}),
            -- rule 1: safe casts (NULL instead of an error)
            c AS (
                SELECT r.*,
                       stage.conform_satellite(satellite_code)   AS satellite,
                       stage.try_date(acq_date_raw)               AS acq_date,
                       CASE WHEN acq_time_raw ~ '^\\d{{1,4}}$'
                            THEN lpad(acq_time_raw, 4, '0') END   AS hhmm4,
                       stage.try_numeric(latitude_raw)            AS latitude,
                       stage.try_numeric(longitude_raw)           AS longitude,
                       stage.try_numeric(brightness_raw)          AS brightness_k,
                       stage.try_numeric(background_raw)          AS background_k,
                       stage.try_numeric(frp_raw)                 AS frp_mw,
                       stage.try_numeric(scan_raw)                AS scan,
                       stage.try_numeric(track_raw)               AS track,
                       stage.try_numeric(confidence_raw)          AS conf_num,
                       stage.try_numeric(type_raw)                AS type_num
                  FROM raw r),
            -- acq_time is HHMM without leading zeros: 347 -> '0347' -> 03:47
            t AS (
                SELECT c.*,
                       CASE WHEN left(hhmm4, 2)::int < 24 AND right(hhmm4, 2)::int < 60
                            THEN hhmm4 END AS acq_time_hhmm
                  FROM c),
            u AS (
                SELECT t.*,
                       (acq_date + make_interval(hours => left(acq_time_hhmm, 2)::int,
                                                 mins  => right(acq_time_hhmm, 2)::int))
                           AT TIME ZONE 'UTC'                     AS acq_ts,
                       -- rule 2: MODIS confidence is a real 0-100 percentage
                       CASE WHEN sensor_family = 'MODIS' AND conf_num BETWEEN 0 AND 100
                                 AND conf_num = trunc(conf_num)
                            THEN conf_num::smallint END           AS confidence_pct,
                       CASE WHEN type_num IN (0, 1, 2, 3)
                            THEN type_num::smallint END           AS fire_type,
                       (latitude BETWEEN -90 AND 90
                        AND longitude BETWEEN -180 AND 180)       AS coord_ok
                  FROM t),
            -- rule 2: one confidence class for both sensors
            v AS (
                SELECT u.*,
                       CASE WHEN sensor_family = 'MODIS' THEN
                                CASE WHEN confidence_pct < 30 THEN 'low'
                                     WHEN confidence_pct < 80 THEN 'nominal'
                                     WHEN confidence_pct IS NOT NULL THEN 'high' END
                            ELSE CASE lower(confidence_raw)
                                     WHEN 'l' THEN 'low' WHEN 'n' THEN 'nominal'
                                     WHEN 'h' THEN 'high' END
                       END AS confidence_class
                  FROM u),
            -- rule 6: quality flags; rule 3: natural key
            w AS (
                SELECT v.*,
                       array_remove(ARRAY[
                           CASE WHEN latitude_raw   IS NOT NULL AND latitude      IS NULL THEN 'cast_fail_latitude' END,
                           CASE WHEN longitude_raw  IS NOT NULL AND longitude     IS NULL THEN 'cast_fail_longitude' END,
                           CASE WHEN brightness_raw IS NOT NULL AND brightness_k  IS NULL THEN 'cast_fail_brightness' END,
                           CASE WHEN background_raw IS NOT NULL AND background_k  IS NULL THEN 'cast_fail_background' END,
                           CASE WHEN frp_raw        IS NOT NULL AND frp_mw        IS NULL THEN 'cast_fail_frp' END,
                           CASE WHEN scan_raw       IS NOT NULL AND scan          IS NULL THEN 'cast_fail_scan' END,
                           CASE WHEN track_raw      IS NOT NULL AND track         IS NULL THEN 'cast_fail_track' END,
                           CASE WHEN acq_date_raw   IS NOT NULL AND acq_date      IS NULL THEN 'cast_fail_acq_date' END,
                           CASE WHEN acq_time_raw   IS NOT NULL AND acq_time_hhmm IS NULL THEN 'cast_fail_acq_time' END,
                           CASE WHEN type_raw       IS NOT NULL AND fire_type     IS NULL THEN 'cast_fail_type' END,
                           CASE WHEN confidence_raw IS NOT NULL AND confidence_class IS NULL THEN 'confidence_invalid' END,
                           CASE fire_type WHEN 1 THEN 'type_volcano'
                                          WHEN 2 THEN 'type_static_land'
                                          WHEN 3 THEN 'type_offshore' END,
                           CASE WHEN NOT coord_ok THEN 'coord_impossible' END,
                           CASE WHEN frp_mw < 0 THEN 'frp_negative' END,
                           CASE WHEN latitude IS NULL OR longitude IS NULL OR acq_ts IS NULL
                                     OR satellite IS NULL OR brightness_k IS NULL
                                THEN 'missing_critical' END
                       ]::text[], NULL) AS dq_flags,
                       CASE WHEN satellite IS NOT NULL AND acq_ts IS NOT NULL
                                 AND latitude IS NOT NULL AND longitude IS NOT NULL
                            THEN md5(concat_ws('|', satellite,
                                     to_char(acq_ts AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI"Z"'),
                                     round(latitude, 4)::text, round(longitude, 4)::text))
                            -- a row without its key parts gets a key from its raw text,
                            -- so it is kept (and flagged missing_critical), not merged
                            ELSE md5(concat_ws('|', 'unkeyed', src_sensor, source_file,
                                     latitude_raw, longitude_raw, acq_date_raw, acq_time_raw,
                                     brightness_raw, frp_raw))
                       END AS detection_id
                  FROM v),
            -- rule A: rank rows sharing a detection_id; rank 1 is kept
            x AS (
                SELECT w.*,
                       row_number()             OVER win AS dup_rank,
                       first_value(source_file) OVER win AS kept_source_file
                  FROM w
                WINDOW win AS (PARTITION BY detection_id
                               ORDER BY (product = 'SP') DESC,
                                        CASE confidence_class WHEN 'high' THEN 3 WHEN 'nominal' THEN 2
                                                              WHEN 'low'  THEN 1 ELSE 0 END DESC,
                                        frp_mw DESC NULLS LAST, source_file, latitude, longitude)
            )
            SELECT x.*,
                   CASE WHEN coord_ok THEN ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) END AS geom,
                   cardinality(dq_flags) = 0 AS dq_pass,
                   {run_id} AS run_id,
                   CASE WHEN dup_rank > 1 THEN 'A_exact_duplicate'
                        -- rule B: NRT inside the SP date range of the same satellite
                        WHEN product = 'NRT' AND acq_date BETWEEN cov.sp_first AND cov.sp_last
                             THEN 'B_nrt_superseded'
                   END AS removal_rule
              FROM x
              LEFT JOIN t_sp_coverage cov ON cov.satellite = x.satellite;
        """),

        # Rebuild: empty the outputs; drop indexes so the bulk insert is fast.
        ("truncate outputs, drop indexes", """
            TRUNCATE stage.fire_detection_clean, stage.fire_detection_removed;
            ALTER TABLE stage.fire_detection_clean DROP CONSTRAINT IF EXISTS fire_detection_clean_pkey;
            DROP INDEX IF EXISTS stage.idx_fire_clean_geom;
            DROP INDEX IF EXISTS stage.idx_fire_clean_acq_date;
        """),

        ("insert clean detections", f"""
            INSERT INTO stage.fire_detection_clean ({OUT_COLS})
            SELECT {OUT_COLS} FROM t_typed WHERE removal_rule IS NULL;
        """),

        ("insert removed rows (audit)", f"""
            INSERT INTO stage.fire_detection_removed ({OUT_COLS}, removal_rule, kept_source_file)
            SELECT {OUT_COLS}, removal_rule,
                   CASE WHEN removal_rule = 'A_exact_duplicate' THEN kept_source_file END
              FROM t_typed WHERE removal_rule IS NOT NULL;
        """),

        # Re-adding the primary key also proves rule A worked: it fails on any duplicate.
        ("rebuild primary key and indexes", """
            ALTER TABLE stage.fire_detection_clean
                ADD CONSTRAINT fire_detection_clean_pkey PRIMARY KEY (detection_id);
            CREATE INDEX idx_fire_clean_geom     ON stage.fire_detection_clean USING GIST (geom);
            CREATE INDEX idx_fire_clean_acq_date ON stage.fire_detection_clean (acq_date);
            ANALYZE stage.fire_detection_clean;
        """),

        # Rule 7: before/after counts per rule and sensor ('ALL' = total).
        ("rule 7: write meta.dq_metrics", f"""
            INSERT INTO meta.dq_metrics (run_id, step_order, rule, src_sensor,
                                         rows_before, rows_affected, rows_after)
            SELECT {run_id}, m.step_order, m.rule, coalesce(s.src_sensor, 'ALL'), m.b, m.a, m.af
              FROM (
                SELECT src_sensor,
                       count(*) AS n_in,
                       count(*) FILTER (WHERE EXISTS (SELECT 1 FROM unnest(dq_flags) f
                                                       WHERE starts_with(f, 'cast_fail'))) AS n_cast,
                       count(*) FILTER (WHERE removal_rule = 'A_exact_duplicate') AS n_a,
                       count(*) FILTER (WHERE removal_rule = 'B_nrt_superseded')  AS n_b,
                       count(*) FILTER (WHERE removal_rule IS NULL)               AS n_out,
                       count(*) FILTER (WHERE removal_rule IS NULL AND 'type_volcano'       = ANY(dq_flags)) AS f_volc,
                       count(*) FILTER (WHERE removal_rule IS NULL AND 'type_static_land'   = ANY(dq_flags)) AS f_static,
                       count(*) FILTER (WHERE removal_rule IS NULL AND 'type_offshore'      = ANY(dq_flags)) AS f_off,
                       count(*) FILTER (WHERE removal_rule IS NULL AND 'coord_impossible'   = ANY(dq_flags)) AS f_coord,
                       count(*) FILTER (WHERE removal_rule IS NULL AND 'frp_negative'       = ANY(dq_flags)) AS f_frp,
                       count(*) FILTER (WHERE removal_rule IS NULL AND 'missing_critical'   = ANY(dq_flags)) AS f_miss,
                       count(*) FILTER (WHERE removal_rule IS NULL AND 'confidence_invalid' = ANY(dq_flags)) AS f_conf,
                       count(*) FILTER (WHERE removal_rule IS NULL AND dq_pass)  AS n_pass
                  FROM t_typed
                 GROUP BY GROUPING SETS ((src_sensor), ())
              ) s
             CROSS JOIN LATERAL (VALUES
                ( 1, 'input',                   n_in,        0::bigint,      n_in),
                ( 2, 'flag_cast_fail',          n_in,        n_cast,         n_in),
                ( 3, 'A_exact_duplicate',       n_in,        n_a,            n_in - n_a),
                ( 4, 'B_nrt_superseded',        n_in - n_a,  n_b,            n_in - n_a - n_b),
                ( 5, 'flag_type_volcano',       n_out,       f_volc,         n_out),
                ( 6, 'flag_type_static_land',   n_out,       f_static,       n_out),
                ( 7, 'flag_type_offshore',      n_out,       f_off,          n_out),
                ( 8, 'flag_coord_impossible',   n_out,       f_coord,        n_out),
                ( 9, 'flag_frp_negative',       n_out,       f_frp,          n_out),
                (10, 'flag_missing_critical',   n_out,       f_miss,         n_out),
                (11, 'flag_confidence_invalid', n_out,       f_conf,         n_out),
                (12, 'dq_pass',                 n_out,       n_out - n_pass, n_pass),
                (13, 'output',                  n_out,       0::bigint,      n_out)
             ) AS m(step_order, rule, b, a, af);
        """),
    ]


def main() -> int:
    conn = psycopg2.connect(**DB)
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO meta.event_log (event_type, event_category, status, note)
                       VALUES ('STAGE_CLEAN_DEDUP', 'STAGE', 'RUNNING', 'rebuild fire_detection_clean')
                       RETURNING log_key""")
        run_id = cur.fetchone()[0]
    conn.commit()
    print(f"run {run_id}: rebuilding stage.fire_detection_clean")
    t0 = time.time()

    try:
        with conn.cursor() as cur:
            for name, sql in steps(run_id):
                t = time.time()
                cur.execute(sql)
                print(f"  {name:<40} {time.time() - t:6.1f}s")
            cur.execute("""SELECT rows_before, rows_after FROM meta.dq_metrics
                            WHERE run_id = %s AND rule = 'output' AND src_sensor = 'ALL'""", (run_id,))
            n_in, n_out = cur.fetchone()
            cur.execute("SELECT count(*) FROM stage.raw_firms_modis")
            n_raw = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM stage.raw_firms_viirs")
            n_raw += cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM stage.fire_detection_removed")
            n_removed = cur.fetchone()[0]
            # Every input row must end up either clean or removed.
            if n_out + n_removed != n_raw:
                raise RuntimeError(f"row accounting failed: {n_out} clean + {n_removed} removed != {n_raw} raw")
            cur.execute("""UPDATE meta.event_log
                              SET status = 'SUCCESS', rows_processed = %s, rows_rejected = %s,
                                  note = %s
                            WHERE log_key = %s""",
                        (n_out, n_removed,
                         f"input={n_raw} clean={n_out} removed={n_removed} "
                         f"({time.time() - t0:.0f}s)", run_id))
        conn.commit()   # clean table, removed table, metrics and event_log become visible together
    except Exception as exc:
        conn.rollback()  # previous clean table stays as it was
        with conn.cursor() as cur:
            cur.execute("UPDATE meta.event_log SET status = 'FAILED', note = %s WHERE log_key = %s",
                        (f"{type(exc).__name__}: {exc}", run_id))
        conn.commit()
        conn.close()
        raise

    conn.close()
    print(f"run {run_id} SUCCESS: {n_raw:,} raw -> {n_out:,} clean, {n_removed:,} removed "
          f"in {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
