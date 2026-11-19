"""
NDVI per Thai district and month, for the star schema.

    stage.ndvi_clean + stage.ndvi_point  (from impute_ndvi.py)
        -> dds.fact_ndvi_district_month   one row per Thai location x month

Table: init-scripts/13_ndvi_district_month.sql. Docs: docs/etl_ndvi.md.
Rebuilt in ONE transaction (truncate + insert). Rerun after impute_ndvi.py.

Rules:
  * Only land points in Thailand count (stage.ndvi_point: in_thailand, point_class
    = 'land'); observed and imputed values alike.
  * A composite counts for the month its 16-day window ends in (start + 15 days,
    or 31 December for the last composite of a year). A month's value therefore
    never uses a day of a later month, which the anomaly model relies on when it
    reads "the month before".
  * Every Thai location of dds.dim_location gets a row for every month that has
    data. A district without a grid point gets the mean of its province, and a
    location without a province (THA_NO_DISTRICT) the mean of Thailand; `source`
    says which.

Usage:
    python controllers/load_ndvi_dds.py
"""

from __future__ import annotations

import json
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


def load_sql(run_id: int) -> str:
    return f"""
        -- one row per Thai land point and composite, with the month the composite ends in
        CREATE TEMP TABLE t_value ON COMMIT DROP AS
        SELECT l.location_key, l.province_gid, c.point_id, c.ndvi, c.is_imputed,
               to_char(least(c.composite_date + 15,
                             make_date(extract(year FROM c.composite_date)::int, 12, 31)), 'YYYYMM')::int AS year_month
          FROM stage.ndvi_clean c
          JOIN stage.ndvi_point p USING (point_id)
          JOIN dds.dim_location l USING (location_code)
         WHERE p.in_thailand AND p.point_class = 'land';

        CREATE TEMP TABLE t_agg ON COMMIT DROP AS
        SELECT location_key, province_gid, year_month,
               sum(ndvi) AS ndvi_sum, count(*) AS n_values, count(*) FILTER (WHERE is_imputed) AS n_imputed,
               count(DISTINCT point_id) AS n_points,
               grouping(location_key) AS no_district, grouping(province_gid) AS no_province
          FROM t_value
         GROUP BY GROUPING SETS ((location_key, province_gid, year_month), (province_gid, year_month), (year_month));
        CREATE INDEX ON t_agg (year_month);
        ANALYZE t_agg;

        TRUNCATE dds.fact_ndvi_district_month;
        INSERT INTO dds.fact_ndvi_district_month
               (location_key, year_month, month_start, ndvi_mean, imputed_share, n_points, n_values, source, run_id)
        SELECT l.location_key, m.year_month, to_date(m.year_month::text, 'YYYYMM'),
               round(a.ndvi_sum / a.n_values, 4), round(a.n_imputed::numeric / a.n_values, 4),
               a.n_points, a.n_values, a.source, {run_id}
          FROM dds.dim_location l
         CROSS JOIN (SELECT DISTINCT year_month FROM t_agg) m
         CROSS JOIN LATERAL (
                SELECT x.*, CASE WHEN no_district = 0 THEN 'district'
                                 WHEN no_province = 0 THEN 'province' ELSE 'country' END AS source
                  FROM t_agg x
                 WHERE x.year_month = m.year_month
                   AND (   (x.no_district = 0 AND x.location_key = l.location_key)
                        OR (x.no_district = 1 AND x.no_province = 0 AND x.province_gid = l.province_gid)
                        OR (x.no_district = 1 AND x.no_province = 1))
                 ORDER BY x.no_district, x.no_province       -- own points, else province, else Thailand
                 LIMIT 1) a
         WHERE l.in_thailand;
        ANALYZE dds.fact_ndvi_district_month;
    """


def main() -> int:
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM stage.ndvi_point WHERE in_thailand AND point_class = 'land'")
    if not cur.fetchone()[0]:
        raise SystemExit("no Thai land points in stage.ndvi_point: run controllers/impute_ndvi.py first")
    cur.execute("""INSERT INTO meta.event_log (event_type, event_category, status, note)
                   VALUES ('LOAD_NDVI_DISTRICT_MONTH', 'DDS', 'RUNNING', 'rebuild fact_ndvi_district_month')
                   RETURNING log_key""")
    run_id = cur.fetchone()[0]
    conn.commit()
    print(f"run {run_id}: rebuilding dds.fact_ndvi_district_month")
    t0 = time.time()

    try:
        cur.execute(load_sql(run_id))

        # ---- checks: any failure rolls the job back
        cur.execute("""SELECT count(*), count(DISTINCT year_month), min(year_month), max(year_month),
                              count(*) FILTER (WHERE ndvi_mean NOT BETWEEN -0.2 AND 1),
                              count(*) FILTER (WHERE source = 'district'),
                              count(*) FILTER (WHERE source = 'province'),
                              count(*) FILTER (WHERE source = 'country'),
                              coalesce(sum(n_values) FILTER (WHERE source = 'district'), 0)::bigint,
                              count(DISTINCT location_key) FILTER (WHERE source = 'district'),
                              count(DISTINCT location_key) FILTER (WHERE source <> 'district')
                         FROM dds.fact_ndvi_district_month""")
        (rows, months, first, last, n_range, n_district, n_province, n_country,
         values_district, loc_own, loc_fallback) = cur.fetchone()
        cur.execute("SELECT count(*) FROM dds.dim_location WHERE in_thailand")
        locations = cur.fetchone()[0]
        cur.execute("SELECT count(*), count(DISTINCT year_month) FROM t_value")
        values_in, months_in = cur.fetchone()
        checks = [
            ("rows = Thai locations x months with data", rows == locations * months and months == months_in,
             f"{rows:,} = {locations} x {months}"),
            ("every Thai land value is in exactly one district row", values_district == values_in,
             f"{values_district:,} / {values_in:,}"),
            ("all means in [-0.2, 1]", n_range == 0, f"{n_range} outside"),
        ]
        print()
        for name, ok, detail in checks:
            print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {detail}")
        if not all(ok for _, ok, _ in checks):
            raise RuntimeError("checks failed: " + "; ".join(n for n, ok, _ in checks if not ok))

        cur.execute("""INSERT INTO meta.dq_metrics (run_id, step_order, rule, src_sensor,
                                                    rows_before, rows_affected, rows_after)
                       VALUES (%s, 721, 'ndvi district-months (affected = from province or country mean)',
                               'MOD13Q1', %s, %s, %s)""", (run_id, rows, n_province + n_country, rows))
        note = dict(rows=rows, locations=locations, months=months, first_month=first, last_month=last,
                    values=values_in, rows_district=n_district, rows_province=n_province, rows_country=n_country,
                    locations_with_own_points=loc_own, locations_on_fallback=loc_fallback,
                    seconds=round(time.time() - t0))
        cur.execute("UPDATE meta.event_log SET status = 'SUCCESS', rows_processed = %s, note = %s WHERE log_key = %s",
                    (rows, json.dumps(note), run_id))
        conn.commit()
    except BaseException as exc:
        conn.rollback()
        cur.execute("UPDATE meta.event_log SET status = 'FAILED', note = %s WHERE log_key = %s",
                    (f"{type(exc).__name__}: {exc}"[:2000], run_id))
        conn.commit()
        raise
    finally:
        conn.close()

    print(f"\nrun {run_id} SUCCESS: {rows:,} district-months ({first}..{last}); {loc_own} locations use their own "
          f"points, {loc_fallback} a province or country mean; {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
