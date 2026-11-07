"""
Point-in-time assignment of fire detections to protected-area versions.

    in-Thailand detections (stage.fire_detection_clean + stage.fire_detection_location)
      x stage.wdpa_polygon_piece (current WDPA polygons, subdivided)
      x dds.dim_protected_area   (SCD2: version valid on acq_date)
        -> stage.fire_detection_pa_bridge       every (detection, park version) membership
        -> stage.fire_detection_protected_area  one primary key per clean detection:
               key of the chosen park version, 0 = in Thailand but outside every park,
              -1 = outside Thailand (WDPA covers Thailand only: not assessed)

Primary rule when a detection lies in more than one park (overlapping or nested):
  1. Designated / Inscribed before Proposed
  2. National designation before International (Ramsar, World Heritage, MAB sit on
     top of the national parks and sanctuaries that actually manage the land)
  3. smallest area (the most specific of nested parks)
  4. lowest site_id (deterministic)

Rebuilt in ONE transaction (truncate + insert); reruns never duplicate rows.
Rerun after load_wdpa.py, clean_stage.py or locate_stage.py.

Usage:
    python controllers/locate_protected_area.py
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


def steps(run_id: int) -> list[tuple[str, str]]:
    return [
        # enable_parallel_hash = off: a parallel hash join keeps one shared hash
        # table in /dev/shm, which is only 64 MB in the default Docker container
        # ("could not resize shared memory segment"). Workers still scan in parallel.
        ("session settings", """
            SET LOCAL max_parallel_workers_per_gather = 8;
            SET LOCAL enable_parallel_hash = off;
            SET LOCAL work_mem = '256MB';
            DROP TABLE IF EXISTS stage.tmp_pa_candidate;
        """),

        # Parallel CREATE TABLE AS. DISTINCT: a point on a cut line between two
        # pieces of the same park must count once.
        ("SPATIAL JOIN: Thai detections -> park polygons", """
            CREATE UNLOGGED TABLE stage.tmp_pa_candidate AS
            SELECT DISTINCT c.detection_id, c.acq_date, pp.site_id
              FROM stage.fire_detection_clean c
              JOIN stage.fire_detection_location l USING (detection_id)
              JOIN stage.wdpa_polygon_piece pp ON ST_Intersects(pp.geom, c.geom)
             WHERE l.in_thailand;
            ANALYZE stage.tmp_pa_candidate;
        """),

        # Point in time: keep only the park version valid on the detection date.
        # A fire before a park's designation year finds no version -> not in that park.
        ("bridge: version valid on acq_date", f"""
            TRUNCATE stage.fire_detection_pa_bridge;
            INSERT INTO stage.fire_detection_pa_bridge (detection_id, site_id, protected_area_key, run_id)
            SELECT t.detection_id, t.site_id, d.protected_area_key, {run_id}
              FROM stage.tmp_pa_candidate t
              JOIN dds.dim_protected_area d
                ON d.site_id = t.site_id
               AND t.acq_date >= d.effective_from AND t.acq_date < d.effective_to;
        """),

        ("primary key per detection", f"""
            TRUNCATE stage.fire_detection_protected_area;
            INSERT INTO stage.fire_detection_protected_area (detection_id, protected_area_key, n_parks, run_id)
            SELECT l.detection_id,
                   coalesce(p.protected_area_key, CASE WHEN l.in_thailand THEN 0 ELSE -1 END),
                   coalesce(p.n_parks, 0), {run_id}
              FROM stage.fire_detection_location l
              LEFT JOIN (
                    SELECT DISTINCT ON (b.detection_id)
                           b.detection_id, b.protected_area_key,
                           count(*) OVER (PARTITION BY b.detection_id) AS n_parks
                      FROM stage.fire_detection_pa_bridge b
                      JOIN dds.dim_protected_area d USING (protected_area_key)
                     ORDER BY b.detection_id,
                              (d.status IN ('Designated', 'Inscribed')) DESC,
                              (d.desig_type = 'National') DESC,
                              d.gis_area ASC NULLS LAST,
                              d.site_id
              ) p USING (detection_id);
            DROP TABLE stage.tmp_pa_candidate;
            ANALYZE stage.fire_detection_pa_bridge;
            ANALYZE stage.fire_detection_protected_area;
        """),

        ("write meta.dq_metrics", f"""
            INSERT INTO meta.dq_metrics (run_id, step_order, rule, src_sensor,
                                         rows_before, rows_affected, rows_after)
            WITH x AS (
                SELECT c.src_sensor, a.n_parks,
                       CASE WHEN a.protected_area_key = -1 THEN 'pa_not_assessed_outside_thailand'
                            WHEN a.protected_area_key = 0  THEN 'pa_outside_protected_area'
                            WHEN d.status = 'Proposed'     THEN 'pa_in_proposed_only'
                            ELSE 'pa_in_designated' END AS cls
                  FROM stage.fire_detection_protected_area a
                  JOIN stage.fire_detection_clean c USING (detection_id)
                  JOIN dds.dim_protected_area d USING (protected_area_key)),
            tot AS (SELECT coalesce(src_sensor, 'ALL') AS s, count(*) AS n
                      FROM x GROUP BY GROUPING SETS ((src_sensor), ())),
            per AS (SELECT coalesce(src_sensor, 'ALL') AS s, cls, count(*) AS n
                      FROM x GROUP BY GROUPING SETS ((src_sensor, cls), (cls))
                    UNION ALL
                    SELECT coalesce(src_sensor, 'ALL'), 'pa_in_2plus_parks', count(*) FILTER (WHERE n_parks >= 2)
                      FROM x GROUP BY GROUPING SETS ((src_sensor), ()))
            SELECT {run_id},
                   CASE per.cls WHEN 'pa_in_designated' THEN 301 WHEN 'pa_in_proposed_only' THEN 302
                                WHEN 'pa_outside_protected_area' THEN 303
                                WHEN 'pa_not_assessed_outside_thailand' THEN 304 ELSE 305 END,
                   per.cls, per.s, tot.n, per.n, tot.n
              FROM per JOIN tot USING (s);
        """),
    ]


def main() -> int:
    conn = psycopg2.connect(**DB)
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM stage.wdpa_polygon_piece")
        if cur.fetchone()[0] == 0:
            raise SystemExit("WDPA not loaded: run controllers/load_wdpa.py first")
        cur.execute("SELECT (SELECT count(*) FROM stage.fire_detection_location), (SELECT count(*) FROM stage.fire_detection_clean)")
        n_loc, n_clean = cur.fetchone()
        if n_loc != n_clean:
            raise SystemExit("fire_detection_location is stale: rerun controllers/locate_stage.py")
        cur.execute("""INSERT INTO meta.event_log (event_type, event_category, status, note)
                       VALUES ('STAGE_LOCATE_PA', 'STAGE', 'RUNNING', 'point-in-time protected-area join')
                       RETURNING log_key""")
        run_id = cur.fetchone()[0]
    conn.commit()
    print(f"run {run_id}: assigning {n_clean:,} detections to protected areas")
    t0 = time.time()
    timings = {}
    try:
        with conn.cursor() as cur:
            for name, sql in steps(run_id):
                t = time.time()
                cur.execute(sql)
                timings[name] = time.time() - t
                print(f"  {name:<50} {timings[name]:6.1f}s")
            cur.execute("""SELECT count(*), count(*) FILTER (WHERE protected_area_key > 0),
                                  count(*) FILTER (WHERE n_parks >= 2)
                             FROM stage.fire_detection_protected_area""")
            n, n_pa, n_multi = cur.fetchone()
            if n != n_clean:
                raise RuntimeError(f"{n} assignments for {n_clean} clean detections")
            cur.execute("SELECT count(*) FROM stage.fire_detection_pa_bridge")
            n_bridge = cur.fetchone()[0]
            join_s = timings["SPATIAL JOIN: Thai detections -> park polygons"]
            cur.execute("""UPDATE meta.event_log SET status = 'SUCCESS', rows_processed = %s, note = %s
                            WHERE log_key = %s""",
                        (n, f"in_protected_area={n_pa} multi_park={n_multi} bridge_rows={n_bridge} "
                            f"spatial_join={join_s:.1f}s total={time.time() - t0:.0f}s", run_id))
        conn.commit()
    except Exception as exc:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("UPDATE meta.event_log SET status = 'FAILED', note = %s WHERE log_key = %s",
                        (f"{type(exc).__name__}: {exc}", run_id))
            cur.execute("DROP TABLE IF EXISTS stage.tmp_pa_candidate")
        conn.commit()
        conn.close()
        raise
    conn.close()
    print(f"run {run_id} SUCCESS: {n:,} assigned, {n_pa:,} in a protected area, {n_multi:,} in 2+ parks, "
          f"{n_bridge:,} bridge rows; spatial join {join_s:.1f}s, total {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
