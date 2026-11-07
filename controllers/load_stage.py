"""
Staging loader: raw FIRMS CSVs -> stage.raw_firms_modis / stage.raw_firms_viirs.

Walks data-extraction-scripts/raw/firms/{sp,nrt}/sensor=*/year=*/DATE.csv and
bulk-loads each file with Postgres COPY. Table definitions: init-scripts/06_stage_v2.sql.

How it stays safe to rerun:
  * every loaded file is recorded in stage.load_file_manifest with its size and
    mtime; an unchanged file is skipped on the next run;
  * a new or changed file is loaded in ONE transaction that first deletes that
    file's old rows, then COPYs the new ones, then upserts the manifest. Either
    all three happen or none do, so a file can never be loaded twice.

Every run writes one row to meta.event_log (RUNNING at start, then SUCCESS,
PARTIAL if any rows were rejected, or FAILED with the error).

Usage:
    python controllers/load_stage.py --year 2020     # test on one year
    python controllers/load_stage.py                 # everything, skip already loaded
    python controllers/load_stage.py --full-reload   # truncate and reload everything
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import psycopg2

# --- configuration ----------------------------------------------------------

DB = dict(
    host=os.getenv("DWH_HOST", "localhost"),
    port=int(os.getenv("DWH_PORT", "5432")),
    dbname=os.getenv("DWH_DB", "fire_dwh"),
    user=os.getenv("DWH_USER", "dwh_admin"),
    password=os.getenv("DWH_PASSWORD", "admin123"),
)

RAW_DIR = Path(os.getenv(
    "FIRMS_RAW_DIR",
    Path(__file__).resolve().parents[1] / "data-extraction-scripts" / "raw",
))

# Full source column lists. NRT files are the same minus `type`.
MODIS_COLS = ["latitude", "longitude", "brightness", "scan", "track", "acq_date",
              "acq_time", "satellite", "instrument", "confidence", "version",
              "bright_t31", "frp", "daynight", "type",
              "src_sensor", "src_window_start", "src_ingested_at"]
VIIRS_COLS = ["latitude", "longitude", "bright_ti4", "scan", "track", "acq_date",
              "acq_time", "satellite", "instrument", "confidence", "version",
              "bright_ti5", "frp", "daynight", "type",
              "src_sensor", "src_window_start", "src_ingested_at"]

TABLES = {"MODIS": ("stage.raw_firms_modis", MODIS_COLS),
          "VIIRS": ("stage.raw_firms_viirs", VIIRS_COLS)}

PRODUCTS = {"sp": "SP", "nrt": "NRT"}


@dataclass
class SourceFile:
    path: Path
    rel: str          # path relative to RAW_DIR, stored as source_file
    sensor: str       # e.g. VIIRS_SNPP_SP
    product: str      # SP / NRT
    table: str
    expected_cols: list[str]


@dataclass
class RunStats:
    files_seen: int = 0
    files_loaded: int = 0
    files_skipped: int = 0
    files_empty: int = 0
    rows_loaded: int = 0
    rows_rejected: int = 0


# --- step 1: discover files -------------------------------------------------

def discover(year: int | None) -> list[SourceFile]:
    files: list[SourceFile] = []
    for folder, product in PRODUCTS.items():
        base = RAW_DIR / "firms" / folder
        for sensor_dir in sorted(base.glob("sensor=*")):
            sensor = sensor_dir.name.split("=", 1)[1]
            family = sensor.split("_", 1)[0]          # MODIS or VIIRS
            if family not in TABLES:
                raise SystemExit(f"Unknown sensor family in {sensor_dir}")
            table, cols = TABLES[family]
            year_glob = f"year={year}" if year else "year=*"
            for path in sorted(sensor_dir.glob(f"{year_glob}/*.csv")):
                files.append(SourceFile(path, path.relative_to(RAW_DIR).as_posix(),
                                        sensor, product, table, cols))
    return files


# --- step 2: parse one file into a COPY buffer ------------------------------

def parse(sf: SourceFile, run_id: int):
    """Return (column list, COPY buffer, rows_ok, rejects).

    Reads the file with the csv module (handles quoting correctly), checks the
    header against the two allowed shapes (with / without `type`), and checks
    every row has the same number of fields as the header. Lineage columns are
    appended to each good row so COPY can load them in the same pass.
    """
    rejects: list[tuple[int, str, str]] = []
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    rows_ok = 0

    with open(sf.path, newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        allowed = (sf.expected_cols, [c for c in sf.expected_cols if c != "type"])
        if header not in allowed:
            # Unknown schema: reject the whole file rather than guess a mapping.
            rejects.append((1, "unexpected header", ",".join(header)))
            for row in reader:
                rejects.append((reader.line_num, "file rejected: unexpected header", ",".join(row)))
            return header, buf, 0, rejects

        for row in reader:
            if not row:
                rejects.append((reader.line_num, "blank line", ""))
            elif len(row) != len(header):
                rejects.append((reader.line_num,
                                f"expected {len(header)} fields, got {len(row)}", ",".join(row)))
            else:
                writer.writerow(row + [sf.rel, sf.product, run_id])
                rows_ok += 1

    buf.seek(0)
    return header, buf, rows_ok, rejects


# --- step 3: load one file in one transaction -------------------------------

def load_file(conn, sf: SourceFile, run_id: int, force: bool, stats: RunStats) -> None:
    st = sf.path.stat()
    if st.st_size == 0:
        stats.files_empty += 1
        return

    with conn.cursor() as cur:
        if not force:
            cur.execute("SELECT file_size, file_mtime_ns FROM stage.load_file_manifest "
                        "WHERE source_file = %s", (sf.rel,))
            hit = cur.fetchone()
            if hit == (st.st_size, st.st_mtime_ns):
                stats.files_skipped += 1
                return

        header, buf, rows_ok, rejects = parse(sf, run_id)

        # Remove anything a previous run left for this file (changed file / forced reload).
        cur.execute(f"DELETE FROM {sf.table} WHERE source_file = %s", (sf.rel,))
        cur.execute("DELETE FROM stage.raw_firms_reject WHERE source_file = %s", (sf.rel,))

        if rows_ok:
            cols = ", ".join(f'"{c}"' for c in header + ["source_file", "product", "load_run_id"])
            cur.copy_expert(f"COPY {sf.table} ({cols}) FROM STDIN WITH (FORMAT csv)", buf)

        if rejects:
            cur.executemany(
                "INSERT INTO stage.raw_firms_reject (source_file, line_no, reason, raw_line, load_run_id) "
                "VALUES (%s, %s, %s, %s, %s)",
                [(sf.rel, ln, why, raw, run_id) for ln, why, raw in rejects])

        # Header-line rejects (line 1) are not data rows; don't count them.
        n_rej = sum(1 for ln, _, _ in rejects if ln != 1)
        cur.execute("""
            INSERT INTO stage.load_file_manifest
                (source_file, target_table, sensor, product, file_size, file_mtime_ns,
                 rows_loaded, rows_rejected, load_run_id, loaded_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (source_file) DO UPDATE SET
                target_table = EXCLUDED.target_table, sensor = EXCLUDED.sensor,
                product = EXCLUDED.product, file_size = EXCLUDED.file_size,
                file_mtime_ns = EXCLUDED.file_mtime_ns, rows_loaded = EXCLUDED.rows_loaded,
                rows_rejected = EXCLUDED.rows_rejected, load_run_id = EXCLUDED.load_run_id,
                loaded_at = now()
        """, (sf.rel, sf.table, sf.sensor, sf.product, st.st_size, st.st_mtime_ns,
              rows_ok, n_rej, run_id))

    conn.commit()   # the delete + COPY + manifest for this file become visible together
    stats.files_loaded += 1
    stats.rows_loaded += rows_ok
    stats.rows_rejected += n_rej


# --- step 4: run-level audit in meta.event_log ------------------------------

def start_run(conn, args) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO meta.event_log (event_type, event_category, status, note)
            VALUES ('STAGE_LOAD_FIRMS', 'STAGE', 'RUNNING', %s) RETURNING log_key
        """, (f"year={args.year or 'all'} full_reload={args.full_reload}",))
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def finish_run(conn, run_id: int, stats: RunStats, status: str, extra: str = "") -> None:
    note = (f"files_seen={stats.files_seen} loaded={stats.files_loaded} "
            f"skipped={stats.files_skipped} empty={stats.files_empty} "
            f"rows_loaded={stats.rows_loaded} rows_rejected={stats.rows_rejected}")
    if extra:
        note += f" | {extra}"
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE meta.event_log
               SET status = %s, rows_processed = %s, files_loaded = %s,
                   rows_rejected = %s, note = note || ' | ' || %s
             WHERE log_key = %s
        """, (status, stats.rows_loaded, stats.files_loaded, stats.rows_rejected, note, run_id))
    conn.commit()


# --- main -------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--year", type=int, help="only load year=YYYY partitions")
    ap.add_argument("--full-reload", action="store_true",
                    help="ignore the manifest and reload; without --year also truncates the tables first")
    args = ap.parse_args()

    files = discover(args.year)
    if not files:
        print(f"No CSV files found under {RAW_DIR / 'firms'} (year={args.year or 'all'})")
        return 1

    conn = psycopg2.connect(**DB)
    stats = RunStats(files_seen=len(files))
    run_id = start_run(conn, args)
    print(f"run {run_id}: {len(files)} files under {RAW_DIR / 'firms'}")
    t0 = time.time()

    try:
        if args.full_reload and not args.year:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE stage.raw_firms_modis, stage.raw_firms_viirs, "
                            "stage.raw_firms_reject, stage.load_file_manifest")
            conn.commit()

        for i, sf in enumerate(files, 1):
            load_file(conn, sf, run_id, args.full_reload, stats)
            if i % 250 == 0 or i == len(files):
                print(f"  {i}/{len(files)} files  rows={stats.rows_loaded:,}  "
                      f"rejected={stats.rows_rejected:,}  {time.time() - t0:.0f}s")
    except Exception as exc:
        conn.rollback()                      # drop the half-loaded file only
        finish_run(conn, run_id, stats, "FAILED", f"{type(exc).__name__}: {exc}")
        conn.close()
        raise                                # never swallow: let the caller see it

    status = "PARTIAL" if stats.rows_rejected else "SUCCESS"
    finish_run(conn, run_id, stats, status)
    conn.close()
    print(f"run {run_id} {status}: loaded {stats.files_loaded} files / {stats.rows_loaded:,} rows, "
          f"skipped {stats.files_skipped}, empty {stats.files_empty}, "
          f"rejected {stats.rows_rejected:,} rows in {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
