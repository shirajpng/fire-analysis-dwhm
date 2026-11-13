"""
Link every clean detection to the Thailand admin hierarchy (GADM 4.1).

    stage.fire_detection_clean  x  stage.gadm_district_piece / gadm_country_piece
        -> stage.fire_detection_location   detection -> district -> province -> country,
                                           in_thailand, outside_class, location_code
        -> meta.location_validation        our in_thailand counts vs NASA's Thailand archive
        -> meta.dq_metrics                 counts per location class and sensor

Tables: init-scripts/08_gadm_location.sql. Reference data: controllers/load_reference.py.
Rebuilt in ONE transaction (truncate + insert), so reruns never duplicate rows.
Rerun this after every rerun of clean_stage.py: the location table stores the
clean run it was built from (clean_run_id).

Location rules, in order (first match wins):
  1. point in a Thai district piece            -> that district (in_thailand)
     (ties on a shared border: lowest GID_2; the 2 'Water body' districts count as Thailand)
  2. in the Thailand country polygon only      -> THA_NO_DISTRICT (in_thailand)
  3. in a neighbour's country polygon          -> OUT_NEIGHBOUR_<ISO3>
  4. flagged type_offshore                     -> OUT_OFFSHORE_FLAGGED
  5. within 1 km of Thailand                   -> OUT_GAP_THAILAND   (coastline/border miss)
  6. within 1 km of a neighbour                -> OUT_GAP_NEIGHBOUR
  7. anything else                             -> OUT_OPEN_SEA
Outside points are kept and flagged, never deleted.

Usage:
    python controllers/locate_stage.py
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

# 1 km is the largest pixel size (MODIS). 0.01 degrees is >= 1 km at these
# latitudes, so it is a cheap index prefilter before the exact geography test.
GAP_M = 1000
GAP_DEG = 0.01


def steps(run_id: int, clean_run_id: int) -> list[tuple[str, str]]:
    return [
        ("session settings", db_limits.session_sql(parallel=True, maintenance=True) + """
            DROP TABLE IF EXISTS stage.tmp_loc_district, stage.tmp_loc_country;
        """),

        # CREATE TABLE AS can run in parallel; INSERT ... SELECT cannot.
        ("SPATIAL JOIN 1: point -> district", """
            CREATE UNLOGGED TABLE stage.tmp_loc_district AS
            SELECT c.detection_id, c.dq_flags, c.geom, d.gid_2
              FROM stage.fire_detection_clean c
              LEFT JOIN LATERAL (
                    SELECT p.gid_2 FROM stage.gadm_district_piece p
                     WHERE ST_Intersects(p.geom, c.geom)
                     ORDER BY p.gid_2 LIMIT 1) d ON true;
        """),

        ("SPATIAL JOIN 2: non-district -> country", """
            CREATE UNLOGGED TABLE stage.tmp_loc_country AS
            SELECT t.detection_id, k.gid_0
              FROM stage.tmp_loc_district t
              LEFT JOIN LATERAL (
                    SELECT p.gid_0 FROM stage.gadm_country_piece p
                     WHERE ST_Intersects(p.geom, t.geom)
                     ORDER BY (p.gid_0 <> 'THA'), p.gid_0 LIMIT 1) k ON true
             WHERE t.gid_2 IS NULL;
            ANALYZE stage.tmp_loc_country;
        """),

        ("truncate output, drop primary key", """
            TRUNCATE stage.fire_detection_location;
            ALTER TABLE stage.fire_detection_location
                DROP CONSTRAINT IF EXISTS fire_detection_location_pkey;
        """),

        ("classify + insert locations (1 km gap test)", f"""
            INSERT INTO stage.fire_detection_location
                   (detection_id, location_code, district_gid, province_gid, country_gid,
                    in_thailand, outside_class, clean_run_id, run_id)
            SELECT x.detection_id, x.location_code, x.gid_2, d.gid_1,
                   CASE WHEN x.gid_2 IS NOT NULL THEN 'THA' ELSE x.gid_0 END,
                   dl.in_thailand, dl.outside_class, {clean_run_id}, {run_id}
              FROM (
                SELECT j.detection_id, j.gid_2, k.gid_0,
                       CASE
                         WHEN j.gid_2 IS NOT NULL               THEN j.gid_2
                         WHEN k.gid_0 = 'THA'                   THEN 'THA_NO_DISTRICT'
                         WHEN k.gid_0 IS NOT NULL               THEN 'OUT_NEIGHBOUR_' || k.gid_0
                         WHEN 'type_offshore' = ANY(j.dq_flags) THEN 'OUT_OFFSHORE_FLAGGED'
                         WHEN EXISTS (SELECT 1 FROM stage.gadm_country_piece p
                                       WHERE p.gid_0 = 'THA'
                                         AND ST_DWithin(p.geom, j.geom, {GAP_DEG})
                                         AND ST_DWithin(p.geom::geography, j.geom::geography, {GAP_M}))
                                                                THEN 'OUT_GAP_THAILAND'
                         WHEN EXISTS (SELECT 1 FROM stage.gadm_country_piece p
                                       WHERE p.gid_0 <> 'THA'
                                         AND ST_DWithin(p.geom, j.geom, {GAP_DEG})
                                         AND ST_DWithin(p.geom::geography, j.geom::geography, {GAP_M}))
                                                                THEN 'OUT_GAP_NEIGHBOUR'
                         ELSE 'OUT_OPEN_SEA'
                       END AS location_code
                  FROM stage.tmp_loc_district j
                  LEFT JOIN stage.tmp_loc_country k USING (detection_id)
              ) x
              -- in_thailand / outside_class come from the dimension: one source of truth
              JOIN dds.dim_location dl ON dl.location_code = x.location_code
              LEFT JOIN stage.gadm_district d ON d.gid_2 = x.gid_2;
        """),

        ("rebuild primary key, drop work tables", """
            ALTER TABLE stage.fire_detection_location
                ADD CONSTRAINT fire_detection_location_pkey PRIMARY KEY (detection_id);
            ANALYZE stage.fire_detection_location;
            DROP TABLE stage.tmp_loc_district, stage.tmp_loc_country;
        """),

        # Like for like: SP only, from our first SP date per sensor to 2024-12-31,
        # flagged rows included. Rule A duplicates are added back as a second count.
        ("validation vs NASA country archive", f"""
            INSERT INTO meta.location_validation
            WITH cov AS (
                SELECT src_sensor, min(acq_date) AS first_day
                  FROM stage.fire_detection_clean WHERE product = 'SP' GROUP BY 1),
            ref AS (
                SELECT r.src_sensor, d AS acq_date,
                       md5(concat_ws('|', stage.conform_satellite(r.satellite),
                           to_char(d + make_interval(hours => left(lpad(r.acq_time, 4, '0'), 2)::int,
                                                     mins  => right(lpad(r.acq_time, 4, '0'), 2)::int),
                                   'YYYY-MM-DD"T"HH24:MI"Z"'),
                           round(stage.try_numeric(r.latitude), 4)::text,
                           round(stage.try_numeric(r.longitude), 4)::text)) AS detection_id
                  FROM stage.ref_firms_country r
                  CROSS JOIN LATERAL (SELECT stage.try_date(r.acq_date) AS d) dd
                  JOIN cov USING (src_sensor)
                 WHERE d BETWEEN cov.first_day AND DATE '2024-12-31'),
            ref_rows AS (
                SELECT src_sensor, extract(year FROM acq_date)::int AS year, count(*) AS n
                  FROM ref GROUP BY 1, 2),
            ref_ids AS (SELECT DISTINCT src_sensor, extract(year FROM acq_date)::int AS year, detection_id FROM ref),
            ours AS (
                SELECT c.src_sensor, extract(year FROM c.acq_date)::int AS year, c.detection_id, l.in_thailand
                  FROM stage.fire_detection_clean c
                  JOIN stage.fire_detection_location l USING (detection_id)
                 WHERE c.product = 'SP' AND c.acq_date <= DATE '2024-12-31'),
            dups AS (
                SELECT r.src_sensor, extract(year FROM r.acq_date)::int AS year, count(*) AS n
                  FROM stage.fire_detection_removed r
                  JOIN stage.fire_detection_location l USING (detection_id)
                 WHERE r.removal_rule = 'A_exact_duplicate' AND l.in_thailand
                   AND r.product = 'SP' AND r.acq_date <= DATE '2024-12-31'
                 GROUP BY 1, 2),
            m AS (
                SELECT coalesce(a.src_sensor, b.src_sensor) AS src_sensor,
                       coalesce(a.year, b.year) AS year,
                       count(*) FILTER (WHERE b.in_thailand)                                   AS ours_tha,
                       count(*) FILTER (WHERE a.detection_id IS NOT NULL AND b.in_thailand)    AS matched,
                       count(*) FILTER (WHERE a.detection_id IS NOT NULL AND NOT b.in_thailand) AS matched_outside,
                       count(*) FILTER (WHERE a.detection_id IS NOT NULL AND b.detection_id IS NULL) AS nasa_only,
                       count(*) FILTER (WHERE a.detection_id IS NULL AND b.in_thailand)        AS ours_only
                  FROM ref_ids a
                  FULL JOIN ours b ON b.detection_id = a.detection_id AND b.src_sensor = a.src_sensor
                 GROUP BY 1, 2)
            SELECT {run_id}, m.src_sensor, m.year, rr.n, m.ours_tha,
                   m.ours_tha + coalesce(d.n, 0),
                   m.ours_tha + coalesce(d.n, 0) - rr.n,
                   m.matched, m.matched_outside, m.nasa_only, m.ours_only
              FROM m
              JOIN ref_rows rr USING (src_sensor, year)
              LEFT JOIN dups d USING (src_sensor, year);
        """),

        ("write meta.dq_metrics", f"""
            INSERT INTO meta.dq_metrics (run_id, step_order, rule, src_sensor,
                                         rows_before, rows_affected, rows_after)
            WITH x AS (
                SELECT c.src_sensor,
                       CASE WHEN l.district_gid IS NOT NULL            THEN 'in_thailand_district'
                            WHEN l.in_thailand                         THEN 'in_thailand_no_district'
                            WHEN l.outside_class = 'neighbour_country' THEN 'neighbour_' || l.country_gid
                            ELSE l.outside_class END AS cls
                  FROM stage.fire_detection_location l
                  JOIN stage.fire_detection_clean c USING (detection_id)),
            tot AS (SELECT coalesce(src_sensor, 'ALL') AS s, count(*) AS n
                      FROM x GROUP BY GROUPING SETS ((src_sensor), ())),
            per AS (SELECT coalesce(src_sensor, 'ALL') AS s, cls, count(*) AS n
                      FROM x GROUP BY GROUPING SETS ((src_sensor, cls), (cls)))
            SELECT {run_id},
                   CASE per.cls WHEN 'in_thailand_district' THEN 101 WHEN 'in_thailand_no_district' THEN 102
                                WHEN 'offshore_flagged' THEN 110 WHEN 'coast_gap_thailand' THEN 111
                                WHEN 'coast_gap_neighbour' THEN 112 WHEN 'open_sea' THEN 113
                                ELSE 105 END,
                   'loc_' || per.cls, per.s, tot.n, per.n, tot.n
              FROM per JOIN tot USING (s);
        """),
    ]


def main() -> int:
    conn = psycopg2.connect(**DB)
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM stage.gadm_district_piece")
        if cur.fetchone()[0] == 0:
            raise SystemExit("GADM not loaded: run controllers/load_reference.py first")
        cur.execute("SELECT count(*), max(run_id), min(run_id) FROM stage.fire_detection_clean")
        n_clean, clean_run_id, min_run = cur.fetchone()
        if not n_clean or clean_run_id != min_run:
            raise SystemExit("stage.fire_detection_clean is empty or mixed: rerun clean_stage.py")
        cur.execute("""INSERT INTO meta.event_log (event_type, event_category, status, note)
                       VALUES ('STAGE_LOCATE_GADM', 'STAGE', 'RUNNING', %s) RETURNING log_key""",
                    (f"clean_run_id={clean_run_id}",))
        run_id = cur.fetchone()[0]
    conn.commit()
    print(f"run {run_id}: locating {n_clean:,} detections (clean run {clean_run_id}; {db_limits.describe()})")
    t0 = time.time()
    timings = {}

    try:
        with conn.cursor() as cur:
            for name, sql in steps(run_id, clean_run_id):
                t = time.time()
                cur.execute(sql)
                timings[name] = time.time() - t
                print(f"  {name:<45} {timings[name]:6.1f}s")
            cur.execute("SELECT count(*), count(*) FILTER (WHERE in_thailand) FROM stage.fire_detection_location")
            n_loc, n_tha = cur.fetchone()
            if n_loc != n_clean:   # every clean detection must get exactly one location
                raise RuntimeError(f"{n_loc} locations for {n_clean} clean detections")
            join_s = sum(v for k, v in timings.items() if k.startswith("SPATIAL JOIN"))
            classify_s = timings["classify + insert locations (1 km gap test)"]
            cur.execute("""UPDATE meta.event_log
                              SET status = 'SUCCESS', rows_processed = %s, note = %s
                            WHERE log_key = %s""",
                        (n_loc, f"clean_run_id={clean_run_id} in_thailand={n_tha} outside={n_loc - n_tha} "
                                f"spatial_join={join_s:.1f}s classify={classify_s:.1f}s "
                                f"total={time.time() - t0:.0f}s", run_id))
        conn.commit()
    except Exception as exc:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("UPDATE meta.event_log SET status = 'FAILED', note = %s WHERE log_key = %s",
                        (f"{type(exc).__name__}: {exc}", run_id))
            cur.execute("DROP TABLE IF EXISTS stage.tmp_loc_district, stage.tmp_loc_country")
        conn.commit()
        conn.close()
        raise

    conn.close()
    print(f"run {run_id} SUCCESS: {n_loc:,} located, {n_tha:,} in Thailand, "
          f"{n_loc - n_tha:,} outside; spatial join {join_s:.1f}s, total {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
