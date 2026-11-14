"""
Load the star schema in dds (init-scripts/10_dds_star_schema.sql).

    stage.fire_detection_clean            -> dds.fact_active_fire (one row per clean detection)
    stage.fire_detection_location         -> location_key        (dds.dim_location, item 3)
    stage.fire_detection_protected_area   -> protected_area_key  (dds.dim_protected_area, item 4)
    stage.fire_detection_pa_bridge        -> dds.bridge_fire_protected_area
    generated                             -> dds.dim_date, dds.dim_time
    distinct values of the clean table    -> dds.dim_satellite_instrument, dds.dim_detection_flag

date_key / time_key are THAI LOCAL TIME (Asia/Bangkok, UTC+7): 23.5% of the
detections (the ~01:30 night overpasses) fall on the previous day in UTC.

Everything runs in ONE transaction:
  1. upsert the four generated/small dimensions (keys stable across reruns)
  2. drop the fact/bridge FKs and indexes, TRUNCATE fact and bridge
  3. build the fact rows with all surrogate keys in a parallel CREATE TABLE AS,
     INSERT them into the partitions in date order, rebuild PK and indexes,
     load the bridge
  4. recreate the FKs (validates every key), run the checks, write metrics
Any failed check raises, and the whole load is rolled back.

Rerun order after an upstream change: clean_stage -> locate_stage ->
load_wdpa -> locate_protected_area -> load_dds.

Usage:
    python controllers/load_dds.py
"""

from __future__ import annotations

import os
import sys
import time

import psycopg2

import db_limits

DB = dict(
    host=os.getenv("DWH_HOST", "localhost"),
    port=int(os.getenv("DWH_PORT", "5432")),
    dbname=os.getenv("DWH_DB", "fire_dwh"),
    user=os.getenv("DWH_USER", "dwh_admin"),
    password=os.getenv("DWH_PASSWORD", "admin123"),
)

LOCAL_TZ = "Asia/Bangkok"

# --------------------------------------------------------------------------
# Dimensions
# --------------------------------------------------------------------------
DIM_DATE_SQL = """
INSERT INTO dds.dim_date (date_key, full_date, day_of_week, day_name, day_of_month, month_number,
                          month_name, quarter_number, year_number, month_short, year_month, day_of_year,
                          iso_week, iso_year, is_weekend, thai_year_be, fire_year_start, fire_year_label,
                          thai_season, is_burning_season)
SELECT to_char(d, 'YYYYMMDD')::int, d,
       extract(isodow FROM d)::int,                    -- ISO: 1 = Monday .. 7 = Sunday
       to_char(d, 'FMDay'), extract(day FROM d)::int, extract(month FROM d)::int,
       to_char(d, 'FMMonth'), extract(quarter FROM d)::int, extract(year FROM d)::int,
       to_char(d, 'Mon'), to_char(d, 'YYYY-MM'), extract(doy FROM d)::int,
       extract(week FROM d)::int, extract(isoyear FROM d)::int, extract(isodow FROM d) >= 6,
       extract(year FROM d)::int + 543,
       fy, fy || '/' || lpad(((fy + 1) % 100)::text, 2, '0'),
       -- NSO Statistical Yearbook Thailand (data from the Meteorological Department):
       -- hot mid-Feb..mid-May, rainy mid-May..mid-Oct, cool mid-Oct..mid-Feb.
       CASE WHEN md BETWEEN 216 AND 515 THEN 'Hot'
            WHEN md BETWEEN 516 AND 1015 THEN 'Rainy'
            ELSE 'Cool' END,
       extract(month FROM d) <= 4                       -- burning season: 1 Jan .. 30 Apr
  FROM (SELECT g::date AS d,
               CASE WHEN extract(month FROM g) >= 8 THEN extract(year FROM g)::int
                    ELSE extract(year FROM g)::int - 1 END AS fy,
               (extract(month FROM g) * 100 + extract(day FROM g))::int AS md
          FROM generate_series('2012-01-01'::date, '2030-12-31'::date, interval '1 day') g) s
ON CONFLICT (date_key) DO UPDATE SET
    full_date = EXCLUDED.full_date, day_of_week = EXCLUDED.day_of_week, day_name = EXCLUDED.day_name,
    day_of_month = EXCLUDED.day_of_month, month_number = EXCLUDED.month_number,
    month_name = EXCLUDED.month_name, quarter_number = EXCLUDED.quarter_number,
    year_number = EXCLUDED.year_number, month_short = EXCLUDED.month_short,
    year_month = EXCLUDED.year_month, day_of_year = EXCLUDED.day_of_year,
    iso_week = EXCLUDED.iso_week, iso_year = EXCLUDED.iso_year, is_weekend = EXCLUDED.is_weekend,
    thai_year_be = EXCLUDED.thai_year_be, fire_year_start = EXCLUDED.fire_year_start,
    fire_year_label = EXCLUDED.fire_year_label, thai_season = EXCLUDED.thai_season,
    is_burning_season = EXCLUDED.is_burning_season;
"""

DIM_TIME_SQL = """
INSERT INTO dds.dim_time (time_key, time_local, hour_local, minute, time_utc, hour_utc,
                          part_of_day, overpass_window)
SELECT (h * 100 + mi)::smallint, make_time(h, mi, 0), h, mi,
       make_time(h, mi, 0) - interval '7 hours', ((h + 17) % 24),
       CASE WHEN h < 6 THEN 'Night' WHEN h < 12 THEN 'Morning'
            WHEN h < 18 THEN 'Afternoon' ELSE 'Evening' END,
       -- windows observed in the data (local hour of every detection)
       CASE WHEN h BETWEEN 0 AND 4   THEN 'Night ~01:30 (VIIRS, Aqua)'
            WHEN h BETWEEN 8 AND 11  THEN 'Morning ~10:30 (Terra)'
            WHEN h BETWEEN 12 AND 16 THEN 'Afternoon ~13:30 (VIIRS, Aqua)'
            WHEN h BETWEEN 20 AND 23 THEN 'Evening ~22:30 (Terra)'
            ELSE 'No overpass' END
  FROM generate_series(0, 23) h, generate_series(0, 59) mi
ON CONFLICT (time_key) DO UPDATE SET
    time_local = EXCLUDED.time_local, hour_local = EXCLUDED.hour_local, minute = EXCLUDED.minute,
    time_utc = EXCLUDED.time_utc, hour_utc = EXCLUDED.hour_utc,
    part_of_day = EXCLUDED.part_of_day, overpass_window = EXCLUDED.overpass_window;
"""

DIM_SATELLITE_SQL = """
INSERT INTO dds.dim_satellite_instrument (satellite_name, satellite_code, instrument_name, sensor_type,
                                          src_sensor, product, product_name, version, resolution_m)
SELECT DISTINCT satellite, satellite_code, instrument, sensor_family, src_sensor, product,
       CASE product WHEN 'SP' THEN 'Standard Processing' ELSE 'Near Real-Time' END,
       version,
       CASE sensor_family WHEN 'MODIS' THEN 1000 ELSE 375 END
  FROM stage.fire_detection_clean
ON CONFLICT (satellite_code, product, version) DO UPDATE SET
    satellite_name = EXCLUDED.satellite_name, instrument_name = EXCLUDED.instrument_name,
    sensor_type = EXCLUDED.sensor_type, src_sensor = EXCLUDED.src_sensor,
    product_name = EXCLUDED.product_name, resolution_m = EXCLUDED.resolution_m, updated_at = now();
"""

DIM_FLAG_SQL = """
INSERT INTO dds.dim_detection_flag (daynight, daynight_label, confidence_class, dq_pass, dq_flags,
                                    fire_type, fire_type_label)
SELECT DISTINCT daynight, CASE daynight WHEN 'D' THEN 'Day' ELSE 'Night' END,
       confidence_class, dq_pass, dq_flags, fire_type,
       CASE fire_type WHEN 0 THEN 'Presumed vegetation fire' WHEN 1 THEN 'Active volcano'
                      WHEN 2 THEN 'Other static land source' WHEN 3 THEN 'Offshore'
                      ELSE 'Unknown (NRT, no type)' END
  FROM stage.fire_detection_clean
ON CONFLICT ON CONSTRAINT uq_dim_flag_natural DO UPDATE SET
    daynight_label = EXCLUDED.daynight_label, fire_type_label = EXCLUDED.fire_type_label, updated_at = now();
"""

# --------------------------------------------------------------------------
# Fact and bridge
# --------------------------------------------------------------------------
FACT_FKS = [
    ("fk_fact_date",      "date_key",           "dds.dim_date (date_key)"),
    ("fk_fact_time",      "time_key",           "dds.dim_time (time_key)"),
    ("fk_fact_location",  "location_key",       "dds.dim_location (location_key)"),
    ("fk_fact_satellite", "satellite_key",      "dds.dim_satellite_instrument (satellite_key)"),
    ("fk_fact_pa",        "protected_area_key", "dds.dim_protected_area (protected_area_key)"),
    ("fk_fact_flag",      "detection_flag_key", "dds.dim_detection_flag (detection_flag_key)"),
]
BRIDGE_FKS = [
    ("fk_bridge_fact", "detection_id, date_key", "dds.fact_active_fire (detection_id, date_key)"),
    ("fk_bridge_pa",   "protected_area_key",     "dds.dim_protected_area (protected_area_key)"),
]
FACT_INDEXES = [
    ("idx_fact_date_brin",  "USING BRIN (date_key)"),
    ("idx_fact_location",   "(location_key)"),
    ("idx_fact_pa",         "(protected_area_key)"),
    ("idx_fact_satellite",  "(satellite_key)"),
    ("idx_fact_geom",       "USING GIST (geom)"),
]


def drop_constraints_sql() -> str:
    parts = [f"ALTER TABLE dds.bridge_fire_protected_area DROP CONSTRAINT IF EXISTS {n};" for n, _, _ in BRIDGE_FKS]
    parts += [f"ALTER TABLE dds.fact_active_fire DROP CONSTRAINT IF EXISTS {n};" for n, _, _ in FACT_FKS]
    parts += [f"DROP INDEX IF EXISTS dds.{n};" for n, _ in FACT_INDEXES]
    parts += ["ALTER TABLE dds.fact_active_fire DROP CONSTRAINT IF EXISTS fact_active_fire_pkey;",
              "TRUNCATE dds.bridge_fire_protected_area, dds.fact_active_fire;"]
    return "\n".join(parts)


FACT_COLS = ("detection_id, date_key, time_key, location_key, satellite_key, protected_area_key, "
             "detection_flag_key, acq_ts, acq_date_utc, latitude, longitude, geom, "
             "frp_mw, brightness_k, background_k, scan, track, confidence_pct, run_id")


def fact_build_sql(run_id: int) -> str:
    # Phase 1: resolve every surrogate key in a parallel CREATE TABLE AS (INSERT ... SELECT
    # never runs in parallel; done serially these joins took 740 s, in parallel ~30 s).
    return f"""
DROP TABLE IF EXISTS stage.tmp_fact_build;
CREATE UNLOGGED TABLE stage.tmp_fact_build AS
SELECT c.detection_id,
       to_char(c.acq_ts AT TIME ZONE '{LOCAL_TZ}', 'YYYYMMDD')::int     AS date_key,
       to_char(c.acq_ts AT TIME ZONE '{LOCAL_TZ}', 'HH24MI')::smallint  AS time_key,
       dl.location_key, s.satellite_key, pa.protected_area_key, f.detection_flag_key,
       c.acq_ts, c.acq_date AS acq_date_utc, c.latitude, c.longitude, c.geom,
       c.frp_mw, c.brightness_k, c.background_k, c.scan, c.track, c.confidence_pct,
       {run_id} AS run_id
  FROM stage.fire_detection_clean c
  JOIN stage.fire_detection_location l          USING (detection_id)
  JOIN stage.fire_detection_protected_area pa   USING (detection_id)
  JOIN dds.dim_location dl                      ON dl.location_code = l.location_code
  JOIN dds.dim_satellite_instrument s           ON s.satellite_code = c.satellite_code
                                               AND s.product = c.product AND s.version = c.version
  JOIN dds.dim_detection_flag f                 ON f.daynight = c.daynight
                                               AND f.confidence_class = c.confidence_class
                                               AND f.dq_pass = c.dq_pass AND f.dq_flags = c.dq_flags
                                               AND coalesce(f.fire_type, -1) = coalesce(c.fire_type, -1);
"""


def fact_insert_sql() -> str:
    # Phase 2: plain copy into the partitioned fact, in date order (keeps BRIN tight).
    return f"""
INSERT INTO dds.fact_active_fire ({FACT_COLS})
SELECT {FACT_COLS} FROM stage.tmp_fact_build ORDER BY date_key, time_key;
DROP TABLE stage.tmp_fact_build;
"""


def rebuild_sql() -> str:
    parts = ["ALTER TABLE dds.fact_active_fire ADD CONSTRAINT fact_active_fire_pkey PRIMARY KEY (detection_id, date_key);"]
    parts += [f"CREATE INDEX {n} ON dds.fact_active_fire {spec};" for n, spec in FACT_INDEXES]
    parts += ["ANALYZE dds.fact_active_fire;"]
    return "\n".join(parts)


def bridge_insert_sql(run_id: int) -> str:
    # date_key is derived from the clean table (same formula as the fact) instead of
    # joining the 13.5M-row partitioned fact; the FK to the fact then proves it matches.
    return f"""
INSERT INTO dds.bridge_fire_protected_area (detection_id, date_key, protected_area_key, is_primary,
                                            allocation_weight, run_id)
SELECT b.detection_id,
       to_char(c.acq_ts AT TIME ZONE '{LOCAL_TZ}', 'YYYYMMDD')::int,
       b.protected_area_key,
       b.protected_area_key = p.protected_area_key,
       round(1.0 / count(*) OVER (PARTITION BY b.detection_id), 10),
       {run_id}
  FROM stage.fire_detection_pa_bridge b
  JOIN stage.fire_detection_clean c USING (detection_id)
  JOIN stage.fire_detection_protected_area p USING (detection_id);
ANALYZE dds.bridge_fire_protected_area;
"""


def add_fks_sql() -> str:
    parts = [f"ALTER TABLE dds.fact_active_fire ADD CONSTRAINT {n} FOREIGN KEY ({c}) REFERENCES {r};"
             for n, c, r in FACT_FKS]
    parts += [f"ALTER TABLE dds.bridge_fire_protected_area ADD CONSTRAINT {n} FOREIGN KEY ({c}) REFERENCES {r};"
              for n, c, r in BRIDGE_FKS]
    return "\n".join(parts)


# --------------------------------------------------------------------------
# Checks: each query returns the number of offending rows (must be 0)
# --------------------------------------------------------------------------
MEASURES = ["frp_mw", "brightness_k", "background_k", "scan", "track", "confidence_pct"]
_agg = ", ".join(f"count({m}) AS n_{m}, sum({m}) AS s_{m}" for m in MEASURES)

CHECKS = [
    ("fact rows = clean rows",
     "SELECT abs((SELECT count(*) FROM dds.fact_active_fire) - (SELECT count(*) FROM stage.fire_detection_clean))"),
    ("default partition empty",
     "SELECT count(*) FROM dds.fact_active_fire_default"),
    ("orphan date_key",
     "SELECT count(*) FROM dds.fact_active_fire f WHERE NOT EXISTS (SELECT 1 FROM dds.dim_date d WHERE d.date_key = f.date_key)"),
    ("orphan time_key",
     "SELECT count(*) FROM dds.fact_active_fire f WHERE NOT EXISTS (SELECT 1 FROM dds.dim_time d WHERE d.time_key = f.time_key)"),
    ("orphan location_key",
     "SELECT count(*) FROM dds.fact_active_fire f WHERE NOT EXISTS (SELECT 1 FROM dds.dim_location d WHERE d.location_key = f.location_key)"),
    ("orphan satellite_key",
     "SELECT count(*) FROM dds.fact_active_fire f WHERE NOT EXISTS (SELECT 1 FROM dds.dim_satellite_instrument d WHERE d.satellite_key = f.satellite_key)"),
    ("orphan protected_area_key",
     "SELECT count(*) FROM dds.fact_active_fire f WHERE NOT EXISTS (SELECT 1 FROM dds.dim_protected_area d WHERE d.protected_area_key = f.protected_area_key)"),
    ("orphan detection_flag_key",
     "SELECT count(*) FROM dds.fact_active_fire f WHERE NOT EXISTS (SELECT 1 FROM dds.dim_detection_flag d WHERE d.detection_flag_key = f.detection_flag_key)"),
    ("orphan bridge rows",
     """SELECT count(*) FROM dds.bridge_fire_protected_area b
         WHERE NOT EXISTS (SELECT 1 FROM dds.fact_active_fire f WHERE f.detection_id = b.detection_id AND f.date_key = b.date_key)"""),
    ("measure totals per sensor = clean",
     f"""WITH a AS (SELECT src_sensor, count(*) AS n, {_agg} FROM stage.fire_detection_clean GROUP BY 1),
              b AS (SELECT s.src_sensor, count(*) AS n, {_agg} FROM dds.fact_active_fire
                      JOIN dds.dim_satellite_instrument s USING (satellite_key) GROUP BY 1)
         SELECT count(*) FROM ((SELECT * FROM a EXCEPT SELECT * FROM b) UNION ALL (SELECT * FROM b EXCEPT SELECT * FROM a)) x"""),
    ("rows per location = item 3",
     """WITH a AS (SELECT location_code, count(*) FROM stage.fire_detection_location GROUP BY 1),
             b AS (SELECT d.location_code, count(*) FROM dds.fact_active_fire JOIN dds.dim_location d USING (location_key) GROUP BY 1)
        SELECT count(*) FROM ((SELECT * FROM a EXCEPT SELECT * FROM b) UNION ALL (SELECT * FROM b EXCEPT SELECT * FROM a)) x"""),
    ("rows per protected_area_key = item 4",
     """WITH a AS (SELECT protected_area_key, count(*) FROM stage.fire_detection_protected_area GROUP BY 1),
             b AS (SELECT protected_area_key, count(*) FROM dds.fact_active_fire GROUP BY 1)
        SELECT count(*) FROM ((SELECT * FROM a EXCEPT SELECT * FROM b) UNION ALL (SELECT * FROM b EXCEPT SELECT * FROM a)) x"""),
    ("bridge rows = stage bridge",
     "SELECT abs((SELECT count(*) FROM dds.bridge_fire_protected_area) - (SELECT count(*) FROM stage.fire_detection_pa_bridge))"),
    ("bridge: one primary per fact row in a park",
     """SELECT abs((SELECT count(*) FROM dds.bridge_fire_protected_area WHERE is_primary)
                 - (SELECT count(*) FROM dds.fact_active_fire WHERE protected_area_key > 0))"""),
    ("bridge: weights add up to 1 per detection",
     """SELECT count(*) FROM (SELECT detection_id FROM dds.bridge_fire_protected_area
                                GROUP BY 1 HAVING abs(sum(allocation_weight) - 1) > 1e-6) x"""),
    ("date/time keys = acq_ts in Thai local time",
     f"""SELECT count(*) FROM dds.fact_active_fire f
           JOIN dds.dim_date d USING (date_key) JOIN dds.dim_time t USING (time_key)
          WHERE d.full_date + t.time_local <> date_trunc('minute', f.acq_ts AT TIME ZONE '{LOCAL_TZ}')"""),
    ("acq_date_utc = UTC date of acq_ts",
     "SELECT count(*) FROM dds.fact_active_fire WHERE acq_date_utc <> (acq_ts AT TIME ZONE 'UTC')::date"),
]

# Order-independent checksum of every fact row except run_id (sum of 64-bit row hashes).
FACT_CHECKSUM_SQL = """
SELECT sum(hashtextextended(concat_ws('|', detection_id, date_key, time_key, location_key, satellite_key,
           protected_area_key, detection_flag_key, acq_ts, acq_date_utc, latitude, longitude, geom::text,
           frp_mw, brightness_k, background_k, scan, track, confidence_pct), 0)::numeric)
  FROM dds.fact_active_fire
"""
BRIDGE_CHECKSUM_SQL = """
SELECT sum(hashtextextended(concat_ws('|', detection_id, date_key, protected_area_key, is_primary,
           allocation_weight), 0)::numeric)
  FROM dds.bridge_fire_protected_area
"""
DIM_CHECKSUM_SQL = """
SELECT md5(string_agg(x, '#' ORDER BY x)) FROM (
    SELECT 'sat|' || concat_ws('|', satellite_key, satellite_code, product, version, src_sensor) FROM dds.dim_satellite_instrument
    UNION ALL SELECT 'flag|' || concat_ws('|', detection_flag_key, daynight, confidence_class, dq_pass, dq_flags, fire_type) FROM dds.dim_detection_flag
    UNION ALL SELECT 'date|' || concat_ws('|', count(*), sum(date_key), string_agg(fire_year_label || thai_season || is_burning_season, '' ORDER BY date_key)) FROM dds.dim_date
    UNION ALL SELECT 'time|' || concat_ws('|', count(*), string_agg(time_key || part_of_day || overpass_window, '' ORDER BY time_key)) FROM dds.dim_time
) s(x)
"""

METRICS_SQL = """
INSERT INTO meta.dq_metrics (run_id, step_order, rule, src_sensor, rows_before, rows_affected, rows_after)
SELECT %(run)s, 401, 'dds_fact_rows', a.src_sensor, a.n, a.n - coalesce(b.n, 0), coalesce(b.n, 0)
  FROM (SELECT src_sensor, count(*) n FROM stage.fire_detection_clean GROUP BY 1) a
  LEFT JOIN (SELECT s.src_sensor, count(*) n FROM dds.fact_active_fire JOIN dds.dim_satellite_instrument s USING (satellite_key) GROUP BY 1) b
       USING (src_sensor)
UNION ALL
SELECT %(run)s, 402, 'dds_local_date_differs_from_utc', s.src_sensor, count(*),
       count(*) FILTER (WHERE f.date_key <> to_char(f.acq_date_utc, 'YYYYMMDD')::int), count(*)
  FROM dds.fact_active_fire f JOIN dds.dim_satellite_instrument s USING (satellite_key) GROUP BY 2, 4
UNION ALL
SELECT %(run)s, 403, 'dds_bridge_rows', 'ALL', (SELECT count(*) FROM stage.fire_detection_pa_bridge),
       0, (SELECT count(*) FROM dds.bridge_fire_protected_area);
"""


def main() -> int:
    conn = psycopg2.connect(**DB)
    with conn.cursor() as cur:
        cur.execute("""SELECT (SELECT count(*) FROM stage.fire_detection_clean),
                              (SELECT count(*) FROM stage.fire_detection_location),
                              (SELECT count(*) FROM stage.fire_detection_protected_area)""")
        n_clean, n_loc, n_pa = cur.fetchone()
        if n_loc != n_clean:
            raise SystemExit("fire_detection_location is stale: rerun controllers/locate_stage.py")
        if n_pa != n_clean:
            raise SystemExit("fire_detection_protected_area is stale: rerun controllers/locate_protected_area.py")
        cur.execute("""SELECT note FROM meta.event_log WHERE event_type = 'LOAD_DDS' AND status = 'SUCCESS'
                        ORDER BY log_key DESC LIMIT 1""")
        prev = cur.fetchone()
        cur.execute("""INSERT INTO meta.event_log (event_type, event_category, status, note)
                       VALUES ('LOAD_DDS', 'DDS', 'RUNNING', 'star schema load') RETURNING log_key""")
        run_id = cur.fetchone()[0]
    conn.commit()
    print(f"run {run_id}: loading star schema from {n_clean:,} clean detections ({db_limits.describe()})")

    t0 = time.time()
    timings: dict[str, float] = {}

    def step(cur, name: str, sql: str, params=None) -> None:
        t = time.time()
        cur.execute(sql, params)
        timings[name] = time.time() - t
        print(f"  {name:<46} {timings[name]:7.1f}s")

    try:
        with conn.cursor() as cur:
            # enable_parallel_hash = off: same /dev/shm (64 MB) workaround as item 4.
            cur.execute(db_limits.session_sql(parallel=True, maintenance=True) +
                        f"""SET LOCAL TimeZone = 'UTC';
                           SET LOCAL enable_parallel_hash = off;
                           SET LOCAL max_parallel_maintenance_workers = {min(4, db_limits.PARALLEL_WORKERS)};""")
            step(cur, "dim_date upsert", DIM_DATE_SQL)
            step(cur, "dim_time upsert", DIM_TIME_SQL)
            step(cur, "dim_satellite_instrument upsert", DIM_SATELLITE_SQL)
            step(cur, "dim_detection_flag upsert", DIM_FLAG_SQL)
            # Without fresh statistics the planner guessed a few hundred fact rows and
            # chose per-row nested loops (run 18: 740 s for the fact insert).
            step(cur, "analyze dimensions", "ANALYZE dds.dim_date; ANALYZE dds.dim_time; "
                 "ANALYZE dds.dim_satellite_instrument; ANALYZE dds.dim_detection_flag; ANALYZE dds.dim_location;")
            step(cur, "drop FKs/indexes, truncate fact+bridge", drop_constraints_sql())
            step(cur, "FACT build: key lookup (parallel CTAS)", fact_build_sql(run_id))
            step(cur, "FACT INSERT into partitions (date order)", fact_insert_sql())
            step(cur, "fact PK + indexes + analyze", rebuild_sql())
            step(cur, "bridge insert", bridge_insert_sql(run_id))
            step(cur, "foreign keys (validate)", add_fks_sql())

            print("  checks:")
            failed = []
            for name, sql in CHECKS:
                cur.execute(sql)
                bad = cur.fetchone()[0]
                print(f"    {'OK  ' if bad == 0 else 'FAIL'} {name:<46} {bad}")
                if bad != 0:
                    failed.append(name)
            if failed:
                raise RuntimeError(f"checks failed: {failed}")

            cur.execute(FACT_CHECKSUM_SQL)
            fact_sum = cur.fetchone()[0]
            cur.execute(BRIDGE_CHECKSUM_SQL)
            bridge_sum = cur.fetchone()[0]
            cur.execute(DIM_CHECKSUM_SQL)
            dim_md5 = cur.fetchone()[0]
            cur.execute(METRICS_SQL, {"run": run_id})
            cur.execute("SELECT count(*) FROM dds.fact_active_fire")
            n_fact = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM dds.bridge_fire_protected_area")
            n_bridge = cur.fetchone()[0]

            total = time.time() - t0
            note = (f"fact_rows={n_fact} bridge_rows={n_bridge} fact_checksum={fact_sum} "
                    f"bridge_checksum={bridge_sum} dim_md5={dim_md5} "
                    f"fact_build={timings['FACT build: key lookup (parallel CTAS)']:.1f}s "
                    f"fact_insert={timings['FACT INSERT into partitions (date order)']:.1f}s "
                    f"indexes={timings['fact PK + indexes + analyze']:.1f}s total={total:.0f}s")
            cur.execute("UPDATE meta.event_log SET status = 'SUCCESS', rows_processed = %s, note = %s WHERE log_key = %s",
                        (n_fact, note, run_id))
        conn.commit()
    except Exception as exc:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("UPDATE meta.event_log SET status = 'FAILED', note = %s WHERE log_key = %s",
                        (f"{type(exc).__name__}: {exc}", run_id))
            cur.execute("DROP TABLE IF EXISTS stage.tmp_fact_build")
        conn.commit()
        conn.close()
        raise
    conn.close()

    print(f"run {run_id} SUCCESS: {n_fact:,} fact rows, {n_bridge:,} bridge rows, total {total:.0f}s")
    print(f"  fact checksum {fact_sum}\n  bridge checksum {bridge_sum}\n  dimension md5 {dim_md5}")
    if prev:
        p = dict(kv.split("=", 1) for kv in prev[0].split() if "=" in kv)
        same = (p.get("fact_checksum") == str(fact_sum) and p.get("bridge_checksum") == str(bridge_sum)
                and p.get("dim_md5") == dim_md5)
        print(f"  rerun vs previous successful load: {'IDENTICAL' if same else 'DIFFERENT'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
