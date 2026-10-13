"""
Resumable extractor for the NASA FIRMS active fire archive.

Writes one CSV per (sensor, 10-day window) into a partitioned raw landing zone.
Safe to re-run: completed windows are skipped, so a failed run resumes where it
stopped. Empty windows get a zero-byte marker so they are not re-requested.

Usage:
    export FIRMS_MAP_KEY=your_key_here
    python extract_firms.py                 # science-quality archive
    python extract_firms.py --nrt           # near-real-time, last ~2 months
    python extract_firms.py --availability  # just print sensor date bounds

Get a MAP_KEY from https://firms.modaps.eosdis.nasa.gov/api/
"""

from __future__ import annotations

import argparse
import io
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests

import config
from common import get_logger, get_with_retry, record_manifest, session

log = get_logger("firms")


def map_key() -> str:
    key = os.environ.get("FIRMS_MAP_KEY")
    if not key:
        sys.exit("FIRMS_MAP_KEY is not set. Request one at "
                 "https://firms.modaps.eosdis.nasa.gov/api/")
    return key


def check_availability(key: str) -> None:
    """Print real date bounds per sensor, so config.py can be sanity-checked."""
    url = f"{config.FIRMS_BASE}/data_availability/csv/{key}/ALL"
    try:
        resp = get_with_retry(url, timeout=60)
        print(pd.read_csv(io.StringIO(resp.text)).to_string(index=False))
    except Exception as exc:
        log.warning("availability check failed: %s", exc)


def fetch_window(key: str, sensor: str, start: date, sess) -> pd.DataFrame | None:
    """
    Fetch one <=10-day window. Returns None if genuinely empty.

    Two failure modes to be aware of:

    1. The API often signals problems in the response *body* with HTTP 200,
       e.g. "Error in processing: No data." or "Invalid MAP_KEY".
    2. It sometimes uses a real 400 instead, again with the reason in the body.
       requests' default error message throws that body away, so we log it.
    """
    url = (f"{config.FIRMS_BASE}/area/csv/{key}/{sensor}/{config.BBOX}/"
           f"{config.FIRMS_DAY_RANGE}/{start.isoformat()}")

    try:
        resp = get_with_retry(url, sess, timeout=180)
    except requests.HTTPError as exc:
        body = (exc.response.text or "").strip()[:300] if exc.response is not None else ""
        code = exc.response.status_code if exc.response is not None else "?"
        log.error("%s %s -> HTTP %s: %s", sensor, start, code, body or "(empty body)")
        return None
    except RuntimeError:
        log.error("FAILED %s %s -- exhausted retries", sensor, start)
        return None

    text = resp.text.strip()
    if not text:
        return None

    low = text.lower()
    if low.startswith(("invalid", "error", "no data")):
        if "invalid" in low:
            sys.exit(f"API rejected the request: {text[:200]}")
        return None

    df = pd.read_csv(io.StringIO(text))
    return df if not df.empty else None


def fetch_availability(key: str) -> dict[str, tuple[date, date]]:
    """
    Discover the real date bounds per sensor from the API.

    Returns {sensor: (min_date, max_date)}, or {} if the response cannot be
    understood -- in which case callers fall back to the dates in config.py.

    The endpoint's CSV layout is not documented on the API page, and two shapes
    are in circulation:
        a) data_id,min_date,max_date   -- one row per sensor
        b) sensor,date,sp,nrt          -- one row per sensor per date
    Both are handled. Anything else logs the raw header and returns {} rather
    than guessing, because a misread here silently skips every sensor.
    """
    url = f"{config.FIRMS_BASE}/data_availability/csv/{key}/ALL"
    try:
        resp = get_with_retry(url, timeout=120)
        raw = resp.text
        df = pd.read_csv(io.StringIO(raw))
    except Exception as exc:
        log.warning("availability lookup failed (%s); using config dates", exc)
        return {}

    df.columns = [c.strip().lower() for c in df.columns]
    cols = set(df.columns)
    log.info("availability columns: %s", ", ".join(df.columns))

    bounds: dict[str, tuple[date, date]] = {}

    # Shape (a): one row per sensor with explicit min/max.
    name_col = next((c for c in ("data_id", "sensor", "source") if c in cols), None)
    if name_col and {"min_date", "max_date"} <= cols:
        for _, row in df.iterrows():
            try:
                lo = pd.to_datetime(row["min_date"]).date()
                hi = pd.to_datetime(row["max_date"]).date()
            except Exception:
                continue
            bounds[str(row[name_col]).strip()] = (lo, hi)

    # Shape (b): one row per sensor per date.
    elif name_col and "date" in cols:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])
        for sensor, grp in df.groupby(name_col):
            sub = grp
            if "sp" in cols and str(sensor).endswith("_SP"):
                flagged = grp[grp["sp"].astype(str).str.lower().isin(("true", "1", "yes"))]
                if not flagged.empty:
                    sub = flagged
            bounds[str(sensor).strip()] = (sub["date"].min().date(),
                                           sub["date"].max().date())

    if not bounds:
        log.warning("could not parse availability response; using config dates")
        log.warning("first 300 chars of response: %s", raw[:300].replace("\n", " | "))
        return {}

    for sensor, (lo, hi) in sorted(bounds.items()):
        log.info("available  %-20s %s .. %s", sensor, lo, hi)
    return bounds


def run(sensors: dict, out_dir: Path, start: date, end: date,
        use_availability: bool = True) -> int:
    key = map_key()
    sess = session()
    bounds = fetch_availability(key) if use_availability else {}
    total_rows = 0
    windows_attempted = 0

    for sensor, available_from in sensors.items():
        lo, hi = bounds.get(sensor, (available_from, end))

        cursor = max(start, lo) if lo else (max(start, available_from) if available_from else start)
        sensor_end = min(end, hi) if hi else end

        if cursor > sensor_end:
            log.warning("%s: nothing to fetch (%s .. %s outside available range)",
                        sensor, cursor, sensor_end)
            continue

        log.info("%s: fetching %s .. %s", sensor, cursor, sensor_end)
        sensor_rows = 0

        while cursor <= sensor_end:
            # Partition by sensor and year: browsable on disk, and a natural
            # partition key for Spark/dbt downstream.
            part = out_dir / f"sensor={sensor}" / f"year={cursor.year}"
            part.mkdir(parents=True, exist_ok=True)
            target = part / f"{cursor.isoformat()}.csv"

            if target.exists():
                cursor += timedelta(days=config.FIRMS_DAY_RANGE)
                continue

            df = fetch_window(key, sensor, cursor, sess)
            windows_attempted += 1

            if df is not None:
                # Provenance columns. You need these for dedup across sensors
                # and for the SP-supersedes-NRT lineage story in the writeup.
                df["src_sensor"] = sensor
                df["src_window_start"] = cursor.isoformat()
                df["src_ingested_at"] = pd.Timestamp.now(tz="UTC").isoformat()
                df.to_csv(target, index=False)

                sensor_rows += len(df)
                record_manifest(target, source_url=f"firms:{sensor}:{cursor}",
                                rows=len(df), extra={"sensor": sensor})
                log.info("%-18s %s -> %7d rows", sensor, cursor, len(df))
            else:
                target.touch()  # empty marker, prevents re-requesting
                log.info("%-18s %s -> empty", sensor, cursor)

            # Politeness pause. Dropped accidentally during an earlier refactor;
            # matters more now that the 5-day cap roughly doubles request count.
            # The MAP_KEY budget is 5000 transactions per 10 minutes, so this
            # keeps us comfortably inside it.
            time.sleep(config.PAUSE_SECONDS)
            cursor += timedelta(days=config.FIRMS_DAY_RANGE)

        log.info("== %s total: %d rows ==", sensor, sensor_rows)
        total_rows += sensor_rows

    log.info("FIRMS TOTAL: %d rows", total_rows)

    if windows_attempted == 0:
        log.error("=" * 68)
        log.error("NO WINDOWS WERE ATTEMPTED. Every sensor was skipped.")
        log.error("Requested range was %s .. %s", start, end)
        log.error("This usually means the availability bounds above do not")
        log.error("overlap START_DATE/END_DATE in config.py, or the sensor")
        log.error("names in config.py do not match those the API returned.")
        log.error("Re-run with --no-availability to ignore the API bounds.")
        log.error("=" * 68)
        raise SystemExit(2)

    return total_rows


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=config.RAW_FIRMS)
    ap.add_argument("--start", type=date.fromisoformat, default=config.START_DATE)
    ap.add_argument("--end", type=date.fromisoformat, default=config.END_DATE)
    ap.add_argument("--nrt", action="store_true",
                    help="fetch near-real-time sources instead of science quality")
    ap.add_argument("--availability", action="store_true",
                    help="print sensor date bounds and exit")
    ap.add_argument("--probe", nargs=2, metavar=("SENSOR", "YYYY-MM-DD"),
                    help="make one raw request and print the response body")
    ap.add_argument("--no-availability", action="store_true",
                    help="ignore API date bounds and use config.py dates")
    args = ap.parse_args()

    if args.availability:
        check_availability(map_key())
        sys.exit(0)

    if args.probe:
        # Single raw request, body printed verbatim. Use this to debug a 400
        # instead of inferring the cause from a stack trace.
        sensor, when = args.probe
        k = map_key()
        u = (f"{config.FIRMS_BASE}/area/csv/{k}/{sensor}/{config.BBOX}/"
             f"{config.FIRMS_DAY_RANGE}/{when}")
        r = requests.get(u, timeout=120)
        print(f"URL    : {u.replace(k, '<MAP_KEY>')}")
        print(f"STATUS : {r.status_code}")
        print("BODY   :")
        print(r.text[:2000])
        sys.exit(0)

    sensors = config.FIRMS_SENSORS_NRT if args.nrt else config.FIRMS_SENSORS_SP
    out = args.out / ("nrt" if args.nrt else "sp")

    # NRT only covers roughly the last 2 months regardless of what you ask for.
    start = max(args.start, args.end - timedelta(days=60)) if args.nrt else args.start

    run(sensors, out, start, args.end, use_availability=not args.no_availability)