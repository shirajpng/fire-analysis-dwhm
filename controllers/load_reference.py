"""
Load reference data for the location step (tables: init-scripts/08_gadm_location.sql).

  1. GADM 4.1 polygons from data-extraction-scripts/raw/boundaries/:
       Thailand levels 0-2 (country, 77 provinces, 928 districts) and level 0 of
       the neighbours in config.NEIGHBOUR_ISO3 (MMR, LAO, KHM, MYS, VNM).
     Read with geopandas, bulk-loaded with COPY as hex EWKB (no ogr2ogr needed),
     then cut into <=256-vertex pieces with ST_Subdivide for fast point-in-polygon.
  2. The NASA FIRMS Thailand country archive (raw/firms_country) -> stage.ref_firms_country,
     the validation reference.
  3. dds.dim_location (district grain), upserted so location_key stays stable.

Everything except dim_location is truncated and reloaded in ONE transaction, so a
rerun gives the same result and never duplicates rows.

Usage:
    python controllers/load_reference.py
"""

from __future__ import annotations

import csv
import io
import os
import sys
import time
from pathlib import Path

import geopandas as gpd
import psycopg2
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
import config  # noqa: E402  (Ben's extraction config: ISO3, NEIGHBOUR_ISO3, RAW paths)

RAW = Path(os.getenv("FIRMS_RAW_DIR", config.RAW))
BOUNDARIES = RAW / "boundaries"
COUNTRY_ARCHIVE = RAW / "firms_country"

REF_COLS = {"latitude", "longitude", "brightness", "bright_t31", "bright_ti4", "bright_ti5",
            "scan", "track", "acq_date", "acq_time", "satellite", "instrument",
            "confidence", "version", "frp", "daynight", "type"}

# Members of dim_location for detections without a Thai district.
OUTSIDE_MEMBERS = (
    # location_code, country_code, in_thailand, outside_class
    [("THA_NO_DISTRICT", "THA", True, None)]
    + [(f"OUT_NEIGHBOUR_{iso}", iso, False, "neighbour_country") for iso in config.NEIGHBOUR_ISO3]
    + [("OUT_OFFSHORE_FLAGGED", None, False, "offshore_flagged"),
       ("OUT_GAP_THAILAND",     None, False, "coast_gap_thailand"),
       ("OUT_GAP_NEIGHBOUR",    None, False, "coast_gap_neighbour"),
       ("OUT_OPEN_SEA",         None, False, "open_sea")]
)


def read_layer(iso3: str, level: int) -> gpd.GeoDataFrame:
    path = BOUNDARIES / f"gadm41_{iso3}.gpkg"
    if not path.exists():
        raise SystemExit(f"{path} missing: run data-extraction-scripts/extract_boundaries.py")
    gdf = gpd.read_file(path, layer=f"ADM_ADM_{level}")
    if gdf.crs is None or gdf.crs.to_epsg() != 4326:
        raise SystemExit(f"{path.name} level {level}: expected EPSG:4326, got {gdf.crs}")
    # Force MultiPolygon so every row fits GEOMETRY(MultiPolygon, 4326).
    geoms = [g if g.geom_type == "MultiPolygon" else shapely.MultiPolygon([g]) for g in gdf.geometry]
    gdf["ewkb"] = shapely.to_wkb(shapely.set_srid(geoms, 4326), hex=True, include_srid=True)
    return gdf


def copy_rows(cur, table: str, cols: list[str], rows) -> int:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    n = 0
    for r in rows:
        w.writerow(r)
        n += 1
    buf.seek(0)
    cur.copy_expert(f"COPY {table} ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv)", buf)
    return n


def load_gadm(cur) -> dict[str, int]:
    counts = {}
    cur.execute("TRUNCATE stage.gadm_country, stage.gadm_province, stage.gadm_district, "
                "stage.gadm_district_piece, stage.gadm_country_piece RESTART IDENTITY")

    countries = []
    for iso3 in [config.ISO3, *config.NEIGHBOUR_ISO3]:
        g = read_layer(iso3, 0)
        countries += [(r.GID_0, r.COUNTRY, r.ewkb) for r in g.itertuples()]
    counts["countries"] = copy_rows(cur, "stage.gadm_country", ["gid_0", "country_name", "geom"], countries)

    g1 = read_layer(config.ISO3, 1)
    counts["provinces"] = copy_rows(
        cur, "stage.gadm_province",
        ["gid_1", "gid_0", "name_1", "nl_name_1", "type_1", "engtype_1", "geom"],
        ((r.GID_1, r.GID_0, r.NAME_1, r.NL_NAME_1, r.TYPE_1, r.ENGTYPE_1, r.ewkb) for r in g1.itertuples()))

    g2 = read_layer(config.ISO3, 2)
    counts["districts"] = copy_rows(
        cur, "stage.gadm_district",
        ["gid_2", "gid_1", "gid_0", "name_2", "nl_name_2", "type_2", "engtype_2", "geom"],
        ((r.GID_2, r.GID_1, r.GID_0, r.NAME_2, r.NL_NAME_2, r.TYPE_2, r.ENGTYPE_2, r.ewkb)
         for r in g2.itertuples()))

    # Subdivide for the joins (see 08_gadm_location.sql for why).
    cur.execute("""INSERT INTO stage.gadm_district_piece (gid_2, geom)
                   SELECT gid_2, ST_Subdivide(geom, 256) FROM stage.gadm_district""")
    counts["district_pieces"] = cur.rowcount
    cur.execute("""INSERT INTO stage.gadm_country_piece (gid_0, geom)
                   SELECT gid_0, ST_Subdivide(geom, 256) FROM stage.gadm_country""")
    counts["country_pieces"] = cur.rowcount
    cur.execute("ANALYZE stage.gadm_district_piece; ANALYZE stage.gadm_country_piece")

    # Consistency: every district must belong to a loaded province.
    cur.execute("""SELECT count(*) FROM stage.gadm_district d
                   LEFT JOIN stage.gadm_province p USING (gid_1) WHERE p.gid_1 IS NULL""")
    if cur.fetchone()[0]:
        raise RuntimeError("districts without a province in GADM level 1")
    return counts


def load_country_archive(cur) -> int:
    cur.execute("TRUNCATE stage.ref_firms_country")
    total = 0
    for path in sorted(COUNTRY_ARCHIVE.glob("sensor=*/year=*/*.csv")):
        if path.stat().st_size == 0:
            continue
        sensor = path.parent.parent.name.split("=", 1)[1]
        rel = path.relative_to(RAW).as_posix()
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            header = next(reader)
            if not set(header) <= REF_COLS:
                raise RuntimeError(f"{rel}: unexpected header {header}")
            n_fields = len(header)
            bad = 0

            def rows():
                nonlocal bad
                for row in reader:
                    if len(row) != n_fields:
                        bad += 1
                        continue
                    yield row + [sensor, rel]

            total += copy_rows(cur, "stage.ref_firms_country", header + ["src_sensor", "source_file"], rows())
            if bad:
                raise RuntimeError(f"{rel}: {bad} malformed rows")
    return total


def upsert_dim_location(cur) -> int:
    # Districts (incl. the 2 'Water body' districts, which count as Thailand).
    cur.execute("""
        INSERT INTO dds.dim_location (location_code, district_gid, district_name, district_name_th,
               district_type, province_gid, province_name, province_name_th,
               country_code, country_name, in_thailand, outside_class, updated_at)
        SELECT d.gid_2, d.gid_2, d.name_2, d.nl_name_2, d.engtype_2,
               p.gid_1, p.name_1, p.nl_name_1, c.gid_0, c.country_name, TRUE, NULL, now()
          FROM stage.gadm_district d
          JOIN stage.gadm_province p USING (gid_1)
          JOIN stage.gadm_country  c ON c.gid_0 = d.gid_0
        ON CONFLICT (location_code) DO UPDATE SET
               district_gid = EXCLUDED.district_gid, district_name = EXCLUDED.district_name,
               district_name_th = EXCLUDED.district_name_th, district_type = EXCLUDED.district_type,
               province_gid = EXCLUDED.province_gid, province_name = EXCLUDED.province_name,
               province_name_th = EXCLUDED.province_name_th, country_code = EXCLUDED.country_code,
               country_name = EXCLUDED.country_name, in_thailand = EXCLUDED.in_thailand,
               outside_class = EXCLUDED.outside_class, updated_at = now()
    """)
    for code, country, in_tha, cls in OUTSIDE_MEMBERS:
        cur.execute("""
            INSERT INTO dds.dim_location (location_code, district_name, province_name,
                   country_code, country_name, in_thailand, outside_class, updated_at)
            SELECT %(code)s, %(dname)s, %(pname)s, %(cc)s,
                   (SELECT country_name FROM stage.gadm_country WHERE gid_0 = %(cc)s),
                   %(in_tha)s, %(cls)s, now()
            ON CONFLICT (location_code) DO UPDATE SET
                   district_name = EXCLUDED.district_name, province_name = EXCLUDED.province_name,
                   country_code = EXCLUDED.country_code, country_name = EXCLUDED.country_name,
                   in_thailand = EXCLUDED.in_thailand, outside_class = EXCLUDED.outside_class,
                   updated_at = now()
        """, dict(code=code, cc=country, in_tha=in_tha, cls=cls,
                  dname="Unknown district" if in_tha else f"Outside Thailand ({cls})",
                  pname="Unknown province" if in_tha else f"Outside Thailand ({cls})"))
    cur.execute("SELECT count(*) FROM dds.dim_location")
    return cur.fetchone()[0]


def main() -> int:
    conn = psycopg2.connect(**DB)
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO meta.event_log (event_type, event_category, status, note)
                       VALUES ('LOAD_REFERENCE_GADM', 'STAGE', 'RUNNING', 'GADM + country archive + dim_location')
                       RETURNING log_key""")
        run_id = cur.fetchone()[0]
    conn.commit()
    t0 = time.time()
    try:
        with conn.cursor() as cur:
            t = time.time(); gadm = load_gadm(cur)
            print(f"  GADM loaded {gadm} in {time.time() - t:.1f}s")
            t = time.time(); n_ref = load_country_archive(cur)
            print(f"  country archive: {n_ref:,} rows in {time.time() - t:.1f}s")
            t = time.time(); n_dim = upsert_dim_location(cur)
            print(f"  dds.dim_location: {n_dim} members in {time.time() - t:.1f}s")
            cur.execute("""UPDATE meta.event_log SET status = 'SUCCESS', rows_processed = %s, note = %s
                            WHERE log_key = %s""",
                        (n_ref, f"gadm={gadm} ref_firms_country={n_ref} dim_location={n_dim} "
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
