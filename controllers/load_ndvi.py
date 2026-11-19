"""
Staging loader: raw AppEEARS NDVI CSVs -> stage.raw_ndvi.

Reads data-extraction-scripts/raw/ndvi/*.csv (MOD13Q1 point samples, one file
per AppEEARS task) and bulk-loads each file with Postgres COPY. Table
definitions: init-scripts/12_ndvi.sql.

Of the 33 columns in a file, the 11 source columns are loaded. The other 22
are the two quality columns decoded into text and are not stored.

Safe to rerun, the same way as load_stage.py:
  * every loaded file is recorded in stage.load_file_manifest with its size and
    mtime; an unchanged file is skipped on the next run;
  * a new or changed file is loaded in ONE transaction that first deletes that
    file's old rows, then COPYs the new ones, then upserts the manifest.

A file whose header is not the expected one is rejected whole (logged in
stage.raw_ndvi_reject), never loaded with a guessed column mapping.

Every run writes one row to meta.event_log (STAGE_LOAD_NDVI).

Usage:
    python controllers/load_ndvi.py                 # skip files already loaded
    python controllers/load_ndvi.py --full-reload   # reload every file
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import sys
import time
from pathlib import Path

import psycopg2

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

TABLE = "stage.raw_ndvi"
PRODUCT = "MOD13Q1_061"
LAYER = f"{PRODUCT}__250m_16_days_"

# (column in the file, column in stage.raw_ndvi): the first 11 columns of every file.
SOURCE_COLS = [
    ("Category", "category"),
    ("ID", "id"),
    ("Latitude", "latitude"),
    ("Longitude", "longitude"),
    ("Date", "date"),
    ("MODIS_Tile", "modis_tile"),
    (f"{PRODUCT}_Line_Y_250m", "line_y_250m"),
    (f"{PRODUCT}_Sample_X_250m", "sample_x_250m"),
    (f"{LAYER}NDVI", "ndvi"),
    (f"{LAYER}VI_Quality", "vi_quality"),
    (f"{LAYER}pixel_reliability", "pixel_reliability"),
]
# Every further column must be a decoded part of one of the two quality columns.
DECODED_PREFIXES = (f"{LAYER}VI_Quality_", f"{LAYER}pixel_reliability_")


def header_problem(header: list[str]) -> str | None:
    expected = [src for src, _ in SOURCE_COLS]
    if header[:len(expected)] != expected:
        return "unexpected header: first 11 columns differ"
    extra = [c for c in header[len(expected):] if not c.startswith(DECODED_PREFIXES)]
    if extra:
        return f"unexpected header: unknown column {extra[0]}"
    return None


def parse(path: Path, rel: str, run_id: int):
    """Return (COPY buffer, rows_ok, rejects)."""
    rejects: list[tuple[int, str, str]] = []
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    keep = len(SOURCE_COLS)
    rows_ok = 0

    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        header = next(reader, [])
        problem = header_problem(header)
        if problem:
            rejects.append((1, problem, ",".join(header)[:2000]))
            return buf, 0, rejects
        for row in reader:
            if not row:
                rejects.append((reader.line_num, "blank line", ""))
            elif len(row) != len(header):
                rejects.append((reader.line_num,
                                f"expected {len(header)} fields, got {len(row)}", ",".join(row)[:2000]))
            else:
                writer.writerow(row[:keep] + [rel, run_id])
                rows_ok += 1

    buf.seek(0)
    return buf, rows_ok, rejects


def load_file(conn, path: Path, run_id: int, force: bool) -> tuple[str, int, int]:
    """Load one file in one transaction. Returns (outcome, rows loaded, rows rejected)."""
    rel = path.relative_to(RAW_DIR).as_posix()
    st = path.stat()
    if st.st_size == 0:
        return "empty", 0, 0

    with conn.cursor() as cur:
        if not force:
            cur.execute("SELECT file_size, file_mtime_ns FROM stage.load_file_manifest "
                        "WHERE source_file = %s", (rel,))
            if cur.fetchone() == (st.st_size, st.st_mtime_ns):
                return "skipped", 0, 0

        buf, rows_ok, rejects = parse(path, rel, run_id)

        cur.execute(f"DELETE FROM {TABLE} WHERE source_file = %s", (rel,))
        cur.execute("DELETE FROM stage.raw_ndvi_reject WHERE source_file = %s", (rel,))
        if rows_ok:
            cols = ", ".join([col for _, col in SOURCE_COLS] + ["source_file", "load_run_id"])
            cur.copy_expert(f"COPY {TABLE} ({cols}) FROM STDIN WITH (FORMAT csv)", buf)
        if rejects:
            cur.executemany(
                "INSERT INTO stage.raw_ndvi_reject (source_file, line_no, reason, raw_line, load_run_id) "
                "VALUES (%s, %s, %s, %s, %s)",
                [(rel, ln, why, raw, run_id) for ln, why, raw in rejects])

        # A rejected header (line 1) is not a data row, but the file still counts as rejected.
        n_rej = sum(1 for ln, _, _ in rejects if ln != 1)
        cur.execute("""
            INSERT INTO stage.load_file_manifest
                (source_file, target_table, sensor, product, file_size, file_mtime_ns,
                 rows_loaded, rows_rejected, load_run_id, loaded_at)
            VALUES (%s, %s, 'MOD13Q1', 'NDVI', %s, %s, %s, %s, %s, now())
            ON CONFLICT (source_file) DO UPDATE SET
                target_table = EXCLUDED.target_table, sensor = EXCLUDED.sensor,
                product = EXCLUDED.product, file_size = EXCLUDED.file_size,
                file_mtime_ns = EXCLUDED.file_mtime_ns, rows_loaded = EXCLUDED.rows_loaded,
                rows_rejected = EXCLUDED.rows_rejected, load_run_id = EXCLUDED.load_run_id,
                loaded_at = now()
        """, (rel, TABLE, st.st_size, st.st_mtime_ns, rows_ok, n_rej, run_id))

    conn.commit()
    outcome = "rejected" if rejects and not rows_ok else "loaded"
    return outcome, rows_ok, n_rej


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--full-reload", action="store_true", help="ignore the manifest and reload every file")
    args = ap.parse_args()

    files = sorted((RAW_DIR / "ndvi").glob("*.csv"))
    if not files:
        print(f"No CSV files found under {RAW_DIR / 'ndvi'}")
        return 1

    conn = psycopg2.connect(**DB)
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO meta.event_log (event_type, event_category, status, note)
                       VALUES ('STAGE_LOAD_NDVI', 'STAGE', 'RUNNING', %s) RETURNING log_key""",
                    (f"full_reload={args.full_reload}",))
        run_id = cur.fetchone()[0]
    conn.commit()
    print(f"run {run_id}: {len(files)} files under {RAW_DIR / 'ndvi'}")
    t0 = time.time()

    counts = dict(loaded=0, skipped=0, empty=0, rejected=0)
    rows_loaded = rows_rejected = 0
    status, error = "SUCCESS", ""
    try:
        if args.full_reload:
            # Files that are gone from raw/ must not leave rows behind.
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE {TABLE}, stage.raw_ndvi_reject")
                cur.execute("DELETE FROM stage.load_file_manifest WHERE target_table = %s", (TABLE,))
            conn.commit()
        for i, path in enumerate(files, 1):
            outcome, n_ok, n_rej = load_file(conn, path, run_id, args.full_reload)
            counts[outcome] += 1
            rows_loaded += n_ok
            rows_rejected += n_rej
            if i % 20 == 0 or i == len(files):
                print(f"  {i}/{len(files)} files  rows={rows_loaded:,}  rejected={rows_rejected:,}  "
                      f"{time.time() - t0:.0f}s")
        if counts["rejected"] or rows_rejected:
            status = "PARTIAL"
    except Exception as exc:
        conn.rollback()                      # drops the half-loaded file only
        status, error = "FAILED", f" | {type(exc).__name__}: {exc}"
        raise
    finally:
        note = (f"files_seen={len(files)} loaded={counts['loaded']} skipped={counts['skipped']} "
                f"empty={counts['empty']} files_rejected={counts['rejected']} "
                f"rows_loaded={rows_loaded} rows_rejected={rows_rejected}{error}")
        with conn.cursor() as cur:
            cur.execute("""UPDATE meta.event_log
                              SET status = %s, rows_processed = %s, files_loaded = %s,
                                  rows_rejected = %s, note = note || ' | ' || %s
                            WHERE log_key = %s""",
                        (status, rows_loaded, counts["loaded"], rows_rejected, note[:2000], run_id))
        conn.commit()
        conn.close()

    print(f"run {run_id} {status}: loaded {counts['loaded']} files / {rows_loaded:,} rows, "
          f"skipped {counts['skipped']}, empty {counts['empty']}, rejected {counts['rejected']} files / "
          f"{rows_rejected:,} rows in {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
