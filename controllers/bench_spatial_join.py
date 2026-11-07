"""
Benchmark: point-in-district join on whole GADM districts vs ST_Subdivide pieces.

Runs the same district lookup used by locate_stage.py on one year of clean
detections, once against stage.gadm_district (whole polygons, with a temporary
GIST index) and once against stage.gadm_district_piece (<=256 vertices).
Everything happens in a transaction that is rolled back, so nothing changes;
only a BENCH_SPATIAL_JOIN row is written to meta.event_log.

Usage:
    python controllers/bench_spatial_join.py --year 2020
"""

from __future__ import annotations

import argparse
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

JOIN = """
    SELECT count(*), count(g)
      FROM bench_pts p
      LEFT JOIN LATERAL (SELECT d.gid_2 AS g FROM {table} d
                          WHERE ST_Intersects(d.geom, p.geom)
                          ORDER BY d.gid_2 LIMIT 1) x ON true
"""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--year", type=int, default=2020)
    args = ap.parse_args()

    conn = psycopg2.connect(**DB)
    results = {}
    with conn.cursor() as cur:
        cur.execute("SET LOCAL max_parallel_workers_per_gather = 8")
        cur.execute("CREATE INDEX bench_district_geom ON stage.gadm_district USING GIST (geom)")
        cur.execute("""CREATE TEMP TABLE bench_pts AS
                       SELECT detection_id, geom FROM stage.fire_detection_clean
                        WHERE acq_date >= make_date(%s, 1, 1) AND acq_date < make_date(%s + 1, 1, 1)""",
                    (args.year, args.year))
        cur.execute("ANALYZE bench_pts")
        for label, table in [("whole districts", "stage.gadm_district"),
                             ("subdivided pieces", "stage.gadm_district_piece")]:
            t = time.time()
            cur.execute(JOIN.format(table=table))
            n, matched = cur.fetchone()
            results[label] = (time.time() - t, n, matched)
            print(f"  {label:<18} {results[label][0]:7.1f}s  points={n:,}  in a district={matched:,}")
    conn.rollback()   # drop the temporary index and table

    (t_whole, n, m1), (t_piece, _, m2) = results["whole districts"], results["subdivided pieces"]
    if m1 != m2:
        print(f"WARNING: methods disagree ({m1} vs {m2})")
    note = (f"year={args.year} points={n} whole={t_whole:.1f}s pieces={t_piece:.1f}s "
            f"speedup={t_whole / t_piece:.1f}x matched_whole={m1} matched_pieces={m2}")
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO meta.event_log (event_type, event_category, status, rows_processed, note)
                       VALUES ('BENCH_SPATIAL_JOIN', 'BENCHMARK', 'SUCCESS', %s, %s)""", (n, note))
    conn.commit()
    conn.close()
    print(f"speed-up: {t_whole / t_piece:.1f}x  ({note})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
