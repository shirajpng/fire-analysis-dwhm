"""
Load WDPA protected areas and build dds.dim_protected_area as SCD Type 2.

Inputs (data-extraction-scripts/raw/):
  wdpa/WDPA_WDOECM_<MonYYYY>_Public_THA.zip     current release, file geodatabase
                                               (polygon + point layers)
  wdpa_historical/wdpa_THA_YYYYMM.csv          monthly attribute snapshots (no geometry)

Steps, all in ONE transaction (truncate + reload; the dimension is MERGEd on
(site_id, version_no) so protected_area_key stays stable across reruns):
  1. current polygons -> stage.wdpa_polygon, cut into <=256-vertex pieces;
     point-only records -> stage.wdpa_point (no boundary, not joinable)
  2. snapshots -> stage.wdpa_snapshot with
       site_id         from WDPA_PID, or - for the snapshots exported after the WDPA
                       renamed WDPAID/WDPA_PID to SITE_ID/SITE_PID (empty IDs) -
                       recovered by matching NAME + DESIG_ENG to the current release
       snapshot_complete  false when a snapshot has < 90% of the previous complete
                       one's rows (partial export); those are ignored for SCD2
  3. SCD Type 2 versions (SQL): a new version when a tracked attribute changes
       tracked: STATUS, IUCN_CAT, DESIG_ENG (wording normalised), REP_AREA (0.01 km2),
                GOV_TYPE, DESIG_TYPE, STATUS_YR
       Type 1 (latest value): NAME, GIS_AREA, MANG_AUTH, REALM, ...
     effective_from = 1st of the snapshot month where the version is first seen;
                      version 1 is backdated to Jan 1 of STATUS_YR (designation year)
     effective_to   = next version's start, or the first complete snapshot without
                      the park (removed), or 9999-12-31 (current); exclusive
     plus key 0 'Outside protected area' and key -1 'Not assessed (outside Thailand)'

Usage:
    python controllers/load_wdpa.py
"""

from __future__ import annotations

import csv
import io
import os
import re
import sys
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import geopandas as gpd
import pandas as pd
import psycopg2
import pyogrio
import shapely

DB = dict(
    host=os.getenv("DWH_HOST", "localhost"),
    port=int(os.getenv("DWH_PORT", "5432")),
    dbname=os.getenv("DWH_DB", "fire_dwh"),
    user=os.getenv("DWH_USER", "dwh_admin"),
    password=os.getenv("DWH_PASSWORD", "admin123"),
)

EXTRACTION = Path(__file__).resolve().parents[1] / "data-extraction-scripts"
sys.path.insert(0, str(EXTRACTION))
import config  # noqa: E402

RAW = Path(os.getenv("FIRMS_RAW_DIR", config.RAW))
COMPLETE_SHARE = 0.90

SNAP_COLS = ["WDPA_PID", "WDPAID", "NAME", "DESIG_ENG", "DESIG_TYPE", "IUCN_CAT", "MARINE",
             "REP_AREA", "GIS_AREA", "STATUS", "STATUS_YR", "GOV_TYPE", "OWN_TYPE",
             "MANG_AUTH", "VERIF", "ISO3", "PARENT_ISO3"]
SITE_COLS = ["site_id", "site_pid", "site_type", "name_eng", "name", "desig", "desig_eng",
             "desig_type", "iucn_cat", "realm", "rep_area", "status", "status_yr", "gov_type",
             "own_type", "mang_auth", "verif", "release"]


# --- step 1: current release ------------------------------------------------

def current_release() -> tuple[str, str]:
    """Return (vsizip path of the .gdb, release label) for the newest THA release."""
    zips = []
    for z in (RAW / "wdpa").glob("WDPA_WDOECM_*_Public_THA.zip"):
        m = re.fullmatch(r"WDPA_WDOECM_([A-Z][a-z]{2}\d{4})_Public_THA\.zip", z.name)
        if m:
            zips.append((datetime.strptime(m.group(1), "%b%Y"), m.group(1), z))
    if not zips:
        raise SystemExit("no WDPA_WDOECM_<MonYYYY>_Public_THA.zip in raw/wdpa")
    _, release, z = max(zips)
    return f"/vsizip/{z}/{z.stem}.gdb", release


def read_layer(gdb: str, kind: str) -> gpd.GeoDataFrame:
    names = [n for n, _ in pyogrio.list_layers(gdb) if f"_{kind}_" in n]
    if len(names) != 1:
        raise SystemExit(f"expected one '{kind}' layer in {gdb}, found {names}")
    g = gpd.read_file(gdb, layer=names[0])
    if g.crs is None or g.crs.to_epsg() != 4326:
        raise SystemExit(f"{names[0]}: expected EPSG:4326, got {g.crs}")
    return g


def copy_rows(cur, table: str, cols: list[str], rows) -> int:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    n = 0
    for r in rows:
        w.writerow(["" if v is None or (isinstance(v, float) and pd.isna(v)) else v for v in r])
        n += 1
    buf.seek(0)
    cur.copy_expert(f"COPY {table} ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv)", buf)
    return n


def site_rows(g: gpd.GeoDataFrame, release: str, with_gis_area: bool, multi):
    geoms = [geom if geom.geom_type.startswith("Multi") else multi([geom]) for geom in g.geometry]
    ewkb = shapely.to_wkb(shapely.set_srid(geoms, 4326), hex=True, include_srid=True)
    for r, wkb in zip(g.itertuples(), ewkb):
        row = [int(r.SITE_ID), r.SITE_PID, r.SITE_TYPE, r.NAME_ENG, r.NAME, r.DESIG, r.DESIG_ENG,
               r.DESIG_TYPE, r.IUCN_CAT, r.REALM, r.REP_AREA, r.STATUS, int(r.STATUS_YR), r.GOV_TYPE,
               r.OWN_TYPE, r.MANG_AUTH, r.VERIF, release]
        if with_gis_area:
            row.append(r.GIS_AREA)
        yield row + [wkb]


def load_current(cur) -> tuple[dict, dict[tuple[str, str], int], str]:
    gdb, release = current_release()
    poly, pts = read_layer(gdb, "poly"), read_layer(gdb, "point")
    for g in (poly, pts):
        if not g.SITE_ID.is_unique:
            raise RuntimeError("duplicate SITE_ID in current release")
    cur.execute("TRUNCATE stage.wdpa_polygon, stage.wdpa_point, stage.wdpa_polygon_piece RESTART IDENTITY")
    n_poly = copy_rows(cur, "stage.wdpa_polygon", SITE_COLS + ["gis_area", "geom"],
                       site_rows(poly, release, True, shapely.MultiPolygon))
    n_pts = copy_rows(cur, "stage.wdpa_point", SITE_COLS + ["geom"],
                      site_rows(pts, release, False, shapely.MultiPoint))
    cur.execute("""INSERT INTO stage.wdpa_polygon_piece (site_id, geom)
                   SELECT site_id, ST_Subdivide(geom, 256) FROM stage.wdpa_polygon""")
    n_pieces = cur.rowcount
    cur.execute("ANALYZE stage.wdpa_polygon_piece")
    # Key recovery for ID-less snapshots: (NAME, DESIG_ENG) is unique in the release.
    if poly.duplicated(["NAME", "DESIG_ENG"]).any():
        raise RuntimeError("(NAME, DESIG_ENG) is not unique in the current release")
    by_name = {(r.NAME, r.DESIG_ENG): int(r.SITE_ID) for r in poly.itertuples()}
    return {"polygons": n_poly, "points": n_pts, "pieces": n_pieces}, by_name, release


# --- step 2: snapshots ------------------------------------------------------

def load_snapshots(cur, by_name: dict[tuple[str, str], int]) -> dict:
    files = sorted((RAW / "wdpa_historical").glob("wdpa_THA_*.csv"))
    if not files:
        raise SystemExit("no snapshots in raw/wdpa_historical")
    cur.execute("TRUNCATE stage.wdpa_snapshot")
    stats = {"files": len(files), "complete": 0, "partial": [], "rows": 0, "rows_name_match": 0}
    prev_complete = None
    out = []
    for f in files:
        month = datetime.strptime(re.search(r"(\d{6})", f.name).group(1), "%Y%m").date()
        df = pd.read_csv(f, dtype=str, keep_default_na=False)
        if list(df.columns) != SNAP_COLS:
            raise RuntimeError(f"{f.name}: unexpected columns {list(df.columns)}")
        complete = prev_complete is None or len(df) >= COMPLETE_SHARE * prev_complete
        if complete:
            prev_complete = len(df)
            stats["complete"] += 1
        else:
            stats["partial"].append(f"{f.stem[-6:]} ({len(df)} rows)")
        rel = f.relative_to(RAW).as_posix()
        for r in df.itertuples(index=False):
            if r.WDPA_PID:
                site_id, source = int(r.WDPA_PID), "id"
                if r.WDPAID and int(Decimal(r.WDPAID)) != site_id:   # 5.55512221E8 parses exactly
                    raise RuntimeError(f"{f.name}: WDPAID {r.WDPAID} != WDPA_PID {r.WDPA_PID}")
            else:
                site_id, source = by_name.get((r.NAME, r.DESIG_ENG)), "name_match"
                if site_id is None:
                    raise RuntimeError(f"{f.name}: no ID and no name match for {r.NAME!r} / {r.DESIG_ENG!r}")
                stats["rows_name_match"] += 1
            out.append([month, site_id, source, complete, *r, rel])
    stats["rows"] = copy_rows(cur, "stage.wdpa_snapshot",
                              ["snapshot_month", "site_id", "site_id_source", "snapshot_complete"]
                              + [c.lower() for c in SNAP_COLS] + ["source_file"], out)
    return stats


# --- step 3: SCD Type 2 -----------------------------------------------------

SCD2_SQL = """
CREATE TEMP TABLE t_versions ON COMMIT DROP AS
WITH s AS (
    SELECT site_id, snapshot_month,
           CASE desig_eng   -- wording changes only; normalise to the current wording
                WHEN 'Ramsar Site, Wetland of International Importance'
                     THEN 'Wetland of International Importance (Ramsar Site)'
                WHEN 'World Heritage Site' THEN 'World Heritage Site (natural or mixed)'
                ELSE nullif(desig_eng, '') END                AS desig_eng,
           nullif(desig_type, '') AS desig_type, nullif(iucn_cat, '') AS iucn_cat,
           stage.try_numeric(rep_area)                         AS rep_area,
           nullif(status, '') AS status, stage.try_numeric(status_yr)::int AS status_yr,
           nullif(gov_type, '') AS gov_type
      FROM stage.wdpa_snapshot WHERE snapshot_complete),
h AS (
    SELECT s.*, md5(concat_ws('|', coalesce(status, '-'), coalesce(iucn_cat, '-'),
                              coalesce(desig_eng, '-'), coalesce(round(rep_area, 2)::text, '-'),
                              coalesce(gov_type, '-'), coalesce(desig_type, '-'),
                              coalesce(status_yr::text, '-'))) AS tracked_hash
      FROM s),
cm AS (   -- complete snapshot months with their neighbours
    SELECT snapshot_month,
           lag(snapshot_month)  OVER (ORDER BY snapshot_month) AS prev_month,
           lead(snapshot_month) OVER (ORDER BY snapshot_month) AS next_month
      FROM (SELECT DISTINCT snapshot_month FROM stage.wdpa_snapshot WHERE snapshot_complete) m),
r AS (
    SELECT h.*, cm.prev_month, cm.next_month,
           lag(h.snapshot_month) OVER w AS prev_seen,
           lag(h.tracked_hash)   OVER w AS prev_hash
      FROM h JOIN cm USING (snapshot_month)
    WINDOW w AS (PARTITION BY site_id ORDER BY snapshot_month)),
f AS (   -- a park missing from a complete snapshot starts a new "island" of presence
    SELECT r.*,
           (prev_seen IS NULL OR prev_seen <> prev_month)                           AS new_island,
           (prev_seen IS NULL OR prev_seen <> prev_month OR tracked_hash <> prev_hash) AS new_version
      FROM r),
g AS (
    SELECT f.*, sum(new_island::int)  OVER w AS island,
                sum(new_version::int) OVER w AS version_no
      FROM f WINDOW w AS (PARTITION BY site_id ORDER BY snapshot_month)),
v AS (
    SELECT site_id, version_no, min(island) AS island,
           min(snapshot_month) AS seen_from,
           (array_agg(next_month ORDER BY snapshot_month DESC))[1] AS next_after_last,
           (array_agg(status     ORDER BY snapshot_month))[1] AS status,
           (array_agg(iucn_cat   ORDER BY snapshot_month))[1] AS iucn_cat,
           (array_agg(desig_eng  ORDER BY snapshot_month))[1] AS desig_eng,
           (array_agg(desig_type ORDER BY snapshot_month))[1] AS desig_type,
           (array_agg(rep_area   ORDER BY snapshot_month))[1] AS rep_area,
           (array_agg(gov_type   ORDER BY snapshot_month))[1] AS gov_type,
           (array_agg(status_yr  ORDER BY snapshot_month))[1] AS status_yr,
           (array_agg(tracked_hash ORDER BY snapshot_month))[1] AS tracked_hash
      FROM g GROUP BY site_id, version_no),
latest AS (   -- Type 1 attributes: latest snapshot value
    SELECT DISTINCT ON (site_id) site_id, nullif(name, '') AS name, nullif(mang_auth, '') AS mang_auth,
           stage.try_numeric(gis_area) AS gis_area,
           CASE marine WHEN '0' THEN 'Terrestrial' WHEN '1' THEN 'Coastal' WHEN '2' THEN 'Marine' END AS realm
      FROM stage.wdpa_snapshot WHERE snapshot_complete
     ORDER BY site_id, snapshot_month DESC)
SELECT v.site_id, v.version_no,
       CASE WHEN v.version_no = 1 AND v.status_yr > 0
            THEN LEAST(v.seen_from, make_date(v.status_yr, 1, 1)) ELSE v.seen_from END AS effective_from,
       CASE WHEN lead(v.island) OVER w = v.island THEN lead(v.seen_from) OVER w
            ELSE coalesce(v.next_after_last, DATE '9999-12-31') END                   AS effective_to,
       v.status, v.iucn_cat, v.desig_eng, v.desig_type, v.rep_area, v.gov_type, v.status_yr, v.tracked_hash,
       coalesce(p.site_pid, v.site_id::text) AS site_pid, coalesce(p.site_type, 'PA') AS site_type,
       coalesce(p.name_eng, l.name) AS name_eng, coalesce(p.name, l.name) AS name,
       coalesce(p.realm, l.realm) AS realm, coalesce(p.gis_area, l.gis_area) AS gis_area,
       coalesce(p.mang_auth, l.mang_auth) AS mang_auth,
       (p.site_id IS NOT NULL) AS has_polygon, 'history'::text AS record_source
  FROM v
  JOIN latest l USING (site_id)
  LEFT JOIN stage.wdpa_polygon p USING (site_id)
WINDOW w AS (PARTITION BY v.site_id ORDER BY v.version_no)
UNION ALL
-- point-only records: never in the (polygon) history, one current version
SELECT site_id, 1,
       CASE WHEN status_yr > 0 THEN make_date(status_yr, 1, 1)
            ELSE to_date(release, 'MonYYYY') END,
       DATE '9999-12-31', status, iucn_cat, desig_eng, desig_type, rep_area, gov_type, status_yr,
       md5(concat_ws('|', status, iucn_cat, desig_eng, round(rep_area, 2)::text, gov_type, desig_type, status_yr::text)),
       site_pid, site_type, name_eng, name, realm, NULL::numeric, mang_auth, FALSE, 'current_release_point'
  FROM stage.wdpa_point
 WHERE site_id NOT IN (SELECT site_id FROM stage.wdpa_snapshot);
"""

MERGE_SQL = """
MERGE INTO dds.dim_protected_area d
USING t_versions v ON d.site_id = v.site_id AND d.version_no = v.version_no
WHEN MATCHED THEN UPDATE SET
    effective_from = v.effective_from, effective_to = v.effective_to,
    is_current = (v.effective_to = DATE '9999-12-31'),
    status = v.status, iucn_cat = v.iucn_cat, desig_eng = v.desig_eng, desig_type = v.desig_type,
    rep_area = v.rep_area, gov_type = v.gov_type, status_yr = v.status_yr, tracked_hash = v.tracked_hash,
    site_pid = v.site_pid, site_type = v.site_type, name_eng = v.name_eng, name = v.name,
    realm = v.realm, gis_area = v.gis_area, mang_auth = v.mang_auth, has_polygon = v.has_polygon,
    record_source = v.record_source, run_id = %(run_id)s, updated_at = now()
WHEN NOT MATCHED THEN INSERT
    (site_id, version_no, effective_from, effective_to, is_current, status, iucn_cat, desig_eng,
     desig_type, rep_area, gov_type, status_yr, tracked_hash, site_pid, site_type, name_eng, name,
     realm, gis_area, mang_auth, has_polygon, record_source, run_id, updated_at)
VALUES (v.site_id, v.version_no, v.effective_from, v.effective_to, v.effective_to = DATE '9999-12-31',
        v.status, v.iucn_cat, v.desig_eng, v.desig_type, v.rep_area, v.gov_type, v.status_yr,
        v.tracked_hash, v.site_pid, v.site_type, v.name_eng, v.name, v.realm, v.gis_area, v.mang_auth,
        v.has_polygon, v.record_source, %(run_id)s, now());
"""

MEMBERS_SQL = """
INSERT INTO dds.dim_protected_area AS d
    (protected_area_key, site_id, version_no, name_eng, effective_from, effective_to, is_current,
     has_polygon, record_source, run_id, updated_at)
VALUES ( 0,  0, 1, 'Outside protected area',          DATE '1900-01-01', DATE '9999-12-31', TRUE, FALSE, 'member', %(run_id)s, now()),
       (-1, -1, 1, 'Not assessed (outside Thailand)', DATE '1900-01-01', DATE '9999-12-31', TRUE, FALSE, 'member', %(run_id)s, now())
ON CONFLICT (protected_area_key) DO UPDATE SET
    name_eng = EXCLUDED.name_eng, run_id = EXCLUDED.run_id, updated_at = now();
"""

CHECKS = {
    # versions of one park must not overlap, and must start before they end
    "overlapping_versions": """
        SELECT count(*) FROM dds.dim_protected_area a JOIN dds.dim_protected_area b
          ON a.site_id = b.site_id AND a.protected_area_key < b.protected_area_key
         AND a.effective_from < b.effective_to AND b.effective_from < a.effective_to
         WHERE a.site_id > 0""",
    "empty_or_negative_ranges": "SELECT count(*) FROM dds.dim_protected_area WHERE effective_from >= effective_to",
    "parks_with_two_current": """SELECT count(*) FROM (SELECT site_id FROM dds.dim_protected_area
                                  WHERE is_current GROUP BY site_id HAVING count(*) > 1) x""",
}


def build_dimension(cur, run_id: int) -> dict:
    cur.execute(SCD2_SQL)
    # Rows from an earlier run that no longer exist (e.g. a re-extracted history)
    cur.execute("""DELETE FROM dds.dim_protected_area d WHERE d.protected_area_key > 0
                     AND NOT EXISTS (SELECT 1 FROM t_versions v
                                      WHERE v.site_id = d.site_id AND v.version_no = d.version_no)""")
    deleted = cur.rowcount
    cur.execute(MERGE_SQL, {"run_id": run_id})
    cur.execute(MEMBERS_SQL, {"run_id": run_id})
    for name, sql in CHECKS.items():
        cur.execute(sql)
        if (n := cur.fetchone()[0]):
            raise RuntimeError(f"SCD2 check failed: {name} = {n}")
    cur.execute("""
        SELECT count(*) FILTER (WHERE site_id > 0),
               count(DISTINCT site_id) FILTER (WHERE site_id > 0),
               count(*) FILTER (WHERE site_id > 0 AND version_no > 1),
               count(*) FILTER (WHERE is_current AND site_id > 0),
               count(*) FILTER (WHERE record_source = 'current_release_point')
          FROM dds.dim_protected_area""")
    rows, sites, later, current, points = cur.fetchone()
    # removed parks: no current version any more
    cur.execute("""SELECT count(*) FROM (SELECT site_id FROM dds.dim_protected_area WHERE site_id > 0
                    GROUP BY site_id HAVING NOT bool_or(is_current)) x""")
    removed = cur.fetchone()[0]
    return {"versions": rows, "sites": sites, "versions_after_first": later,
            "current_versions": current, "removed_sites": removed, "point_sites": points,
            "deleted_stale": deleted}


def log_metrics(cur, run_id: int, cur_counts: dict, snap: dict, dim: dict) -> None:
    rows = [
        (201, "wdpa_snapshot_files", snap["files"], snap["files"]),
        (202, "wdpa_snapshots_partial_skipped", snap["files"], len(snap["partial"])),
        (203, "wdpa_snapshot_rows", snap["rows"], snap["rows"]),
        (204, "wdpa_site_id_recovered_by_name", snap["rows"], snap["rows_name_match"]),
        (205, "wdpa_current_polygons", cur_counts["polygons"], cur_counts["polygons"]),
        (206, "wdpa_current_points_no_polygon", cur_counts["points"], cur_counts["points"]),
        (210, "dim_pa_sites", dim["sites"], dim["sites"]),
        (211, "dim_pa_versions", dim["versions"], dim["versions"]),
        (212, "dim_pa_versions_after_first", dim["versions"], dim["versions_after_first"]),
        (213, "dim_pa_removed_sites", dim["sites"], dim["removed_sites"]),
    ]
    cur.executemany("""INSERT INTO meta.dq_metrics (run_id, step_order, rule, src_sensor,
                                                    rows_before, rows_affected, rows_after)
                       VALUES (%s, %s, %s, 'ALL', %s, %s, %s)""",
                    [(run_id, o, r, b, a, b) for o, r, b, a in rows])


def main() -> int:
    conn = psycopg2.connect(**DB)
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO meta.event_log (event_type, event_category, status, note)
                       VALUES ('LOAD_WDPA_SCD2', 'DDS', 'RUNNING', 'WDPA polygons, snapshots, dim_protected_area')
                       RETURNING log_key""")
        run_id = cur.fetchone()[0]
    conn.commit()
    t0 = time.time()
    try:
        with conn.cursor() as cur:
            t = time.time(); cur_counts, by_name, release = load_current(cur)
            print(f"  current release {release}: {cur_counts} in {time.time() - t:.1f}s")
            t = time.time(); snap = load_snapshots(cur, by_name)
            print(f"  snapshots: {snap['files']} files, {snap['complete']} complete, partial skipped: "
                  f"{snap['partial']}, {snap['rows']} rows ({snap['rows_name_match']} keys recovered by name) "
                  f"in {time.time() - t:.1f}s")
            t = time.time(); dim = build_dimension(cur, run_id)
            print(f"  dds.dim_protected_area: {dim} in {time.time() - t:.1f}s")
            log_metrics(cur, run_id, cur_counts, snap, dim)
            cur.execute("""UPDATE meta.event_log SET status = 'SUCCESS', rows_processed = %s, note = %s
                            WHERE log_key = %s""",
                        (dim["versions"], f"release={release} current={cur_counts} snapshots={snap['complete']}/"
                         f"{snap['files']} complete, name_match_rows={snap['rows_name_match']} dim={dim} "
                         f"({time.time() - t0:.0f}s)", run_id))
        conn.commit()
    except Exception as exc:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("UPDATE meta.event_log SET status = 'FAILED', note = %s WHERE log_key = %s",
                        (f"{type(exc).__name__}: {exc}", run_id))
        conn.commit()
        conn.close()
        raise
    conn.close()
    print(f"run {run_id} SUCCESS in {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
