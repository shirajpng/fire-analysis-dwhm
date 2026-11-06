"""
Resumable extractor for the NASA FIRMS active fire archive.

Writes one CSV per (sensor, FIRMS_DAY_RANGE-day window) into a partitioned raw
landing zone. Safe to re-run: completed windows are skipped, so a failed run
resumes where it stopped. Windows the API confirms empty get a zero-byte marker
so they are not re-requested; failed requests never do, so they are retried.

Only whole windows that end on or before the archive's newest date are fetched.
A window straddling that date would be saved with some days missing and then
skipped forever, so it is held back until the archive covers all of it.

When the API does not confirm a sensor's dates (--no-availability, or a sensor
name the availability endpoint does not list), the archive's end is unknown.
A window is then saved only once the archive is shown to cover its last day:
either it has a detection on that day, or a later window has detections. The
trailing windows that cannot be proven are not written, and are fetched again
on the next run.

Usage:
    export FIRMS_MAP_KEY=your_key_here
    python extract_firms.py                 # science-quality archive
    python extract_firms_nrt.py             # near-real-time lives in its own script
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


class FetchError(Exception):
    """The API did not give a clean answer. Never recorded as empty."""


def fetch_window(key: str, sensor: str, start: date, sess) -> pd.DataFrame | None:
    """
    Fetch one FIRMS_DAY_RANGE-day window. Returns None if the API confirms it
    is empty.

    Raises FetchError for anything that is not a clean answer. That covers
    HTTP errors, exhausted retries and bodies that are neither CSV nor an
    explicit "no data". The caller must not turn those into empty markers, or
    the window is skipped on every later run.

    Two failure modes to be aware of:

    1. The API often signals problems in the response *body* with HTTP 200,
       e.g. "Error in processing: No data." or "Invalid MAP_KEY".
    2. It sometimes uses a real 400 instead, again with the reason in the body.
       requests' default error message throws that body away, so we keep it.
    """
    url = (f"{config.FIRMS_BASE}/area/csv/{key}/{sensor}/{config.BBOX}/"
           f"{config.FIRMS_DAY_RANGE}/{start.isoformat()}")

    try:
        resp = get_with_retry(url, sess, timeout=180)
    except requests.HTTPError as exc:
        body = (exc.response.text or "").strip()[:300] if exc.response is not None else ""
        code = exc.response.status_code if exc.response is not None else "?"
        raise FetchError(f"HTTP {code}: {body or '(empty body)'}") from exc
    except RuntimeError as exc:
        raise FetchError("exhausted retries") from exc

    text = resp.text.strip()
    low = text.lower()

    # A real answer, empty or not, is a CSV with a header row.
    if text.startswith("latitude"):
        df = pd.read_csv(io.StringIO(text))
        return df if not df.empty else None

    if "invalid" in low:
        sys.exit(f"API rejected the request: {text[:200]}")
    if "no data" in low:
        return None
    raise FetchError(f"unexpected body: {text[:200] or '(empty)'}")


def covers_window_end(df: pd.DataFrame, start: date) -> bool:
    """
    True if df has a detection on the window's last day.

    That proves the archive reached the end of the window, so the window is
    complete. False is not proof of a gap -- a day can simply have no fires --
    so callers hold such a window until a later one settles it.
    """
    if "acq_date" not in df.columns:
        return False
    newest = pd.to_datetime(df["acq_date"], errors="coerce").max()
    if pd.isna(newest):
        return False
    return newest.date() >= start + timedelta(days=config.FIRMS_DAY_RANGE - 1)


def save_window(df: pd.DataFrame, target: Path, sensor: str, start: date) -> int:
    """
    Annotate and write one window. Returns its row count.

    Written to .part and renamed, because an existing file is skipped on every
    later run: a write cut short by Ctrl-C would otherwise freeze a truncated
    window exactly like a partial one.
    """
    # Provenance columns. You need these for dedup across sensors and for the
    # SP-supersedes-NRT lineage story in the writeup.
    df["src_sensor"] = sensor
    df["src_window_start"] = start.isoformat()
    df["src_ingested_at"] = pd.Timestamp.now(tz="UTC").isoformat()

    part = target.with_suffix(target.suffix + ".part")
    df.to_csv(part, index=False)
    part.rename(target)

    record_manifest(target, source_url=f"firms:{sensor}:{start}",
                    rows=len(df), extra={"sensor": sensor})
    log.info("%-18s %s -> %7d rows", sensor, start, len(df))
    return len(df)


def flush_pending(pending: list[tuple[Path, date, pd.DataFrame]], sensor: str) -> int:
    """Save every held window, now that the archive is known to cover them."""
    rows = sum(save_window(df, target, sensor, start) for target, start, df in pending)
    pending.clear()
    return rows


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

    bounds: dict[str, tuple[date, date]] = {}
    if use_availability:
        bounds = fetch_availability(key)
        if not bounds:
            # Falling back to END_DATE here would request every window between
            # the archive's real end and today, get empty answers, and freeze
            # them as empty markers -- hiding those dates for good once the
            # archive catches up. Refuse instead.
            sys.exit("Could not read FIRMS availability; not guessing archive bounds. "
                     "Re-run later, or pass --no-availability to use config.py dates.")

    step = timedelta(days=config.FIRMS_DAY_RANGE)
    total_rows = 0
    windows_in_range = 0
    failures: list[str] = []

    for sensor, available_from in sensors.items():
        # Empty markers are only safe when the API itself said these dates are
        # in the archive. With config.py dates an "empty" answer may just mean
        # the archive has not reached that date yet.
        confirmed = sensor in bounds
        lo, hi = bounds.get(sensor, (available_from, end))

        cursor = max(start, lo) if lo else (max(start, available_from) if available_from else start)
        sensor_end = min(end, hi) if hi else end

        if cursor > sensor_end:
            log.warning("%s: nothing to fetch (%s .. %s outside available range)",
                        sensor, cursor, sensor_end)
            continue

        log.info("%s: fetching %s .. %s", sensor, cursor, sensor_end)
        if not confirmed:
            log.warning("%s: dates not confirmed by the API; a window is saved "
                        "only once the archive is shown to cover its last day, "
                        "and empty windows are not marked", sensor)
        sensor_rows = 0

        # Unconfirmed sensors only: windows that returned rows but whose last
        # day the archive has not yet been shown to cover. Any later rows
        # prove them complete; whatever is left at the end is not written.
        pending: list[tuple[Path, date, pd.DataFrame]] = []

        # Whole windows only: see the module docstring.
        while cursor + step - timedelta(days=1) <= sensor_end:
            windows_in_range += 1

            # Partition by sensor and year: browsable on disk, and a natural
            # partition key for Spark/dbt downstream.
            part = out_dir / f"sensor={sensor}" / f"year={cursor.year}"
            part.mkdir(parents=True, exist_ok=True)
            target = part / f"{cursor.isoformat()}.csv"

            if target.exists():
                # A non-empty file means the archive once reached this window,
                # so every earlier held window is complete.
                if pending and target.stat().st_size > 0:
                    sensor_rows += flush_pending(pending, sensor)
                cursor += step
                continue

            try:
                df = fetch_window(key, sensor, cursor, sess)
            except FetchError as exc:
                log.error("%-18s %s -> FAILED, will retry next run: %s",
                          sensor, cursor, exc)
                failures.append(f"{sensor} {cursor}")
            else:
                if df is None:
                    if confirmed:
                        target.touch()  # empty marker, prevents re-requesting
                        log.info("%-18s %s -> empty", sensor, cursor)
                    else:
                        log.info("%-18s %s -> empty (not marked)", sensor, cursor)
                elif confirmed:
                    sensor_rows += save_window(df, target, sensor, cursor)
                else:
                    # Rows here mean the archive extends past every earlier
                    # held window, so those are complete.
                    sensor_rows += flush_pending(pending, sensor)
                    if covers_window_end(df, cursor):
                        sensor_rows += save_window(df, target, sensor, cursor)
                    else:
                        pending.append((target, cursor, df))
                        log.info("%-18s %s -> %7d rows, held: archive may end "
                                 "inside this window", sensor, cursor, len(df))

            # Politeness pause. Dropped accidentally during an earlier refactor;
            # matters more now that the 5-day cap roughly doubles request count.
            # The MAP_KEY budget is 5000 transactions per 10 minutes, so this
            # keeps us comfortably inside it.
            time.sleep(config.PAUSE_SECONDS)
            cursor += step

        if pending:
            log.warning("%s: %d window(s) not saved because the archive may end "
                        "inside them (%s); they are fetched again next run",
                        sensor, len(pending),
                        ", ".join(str(start) for _, start, _ in pending))

        if cursor <= sensor_end:
            log.info("%s: holding back partial window %s .. %s until the archive "
                     "covers all of it", sensor, cursor, sensor_end)

        log.info("== %s total: %d rows ==", sensor, sensor_rows)
        total_rows += sensor_rows

    log.info("FIRMS TOTAL: %d rows", total_rows)

    # Counts windows already on disk too, so a re-run after a complete
    # extraction is not mistaken for a misconfiguration.
    if windows_in_range == 0:
        log.error("=" * 68)
        log.error("NO WINDOWS IN RANGE. Every sensor was skipped.")
        log.error("Requested range was %s .. %s", start, end)
        log.error("This usually means the availability bounds above do not")
        log.error("overlap START_DATE/END_DATE in config.py, or the sensor")
        log.error("names in config.py do not match those the API returned.")
        log.error("Re-run with --no-availability to ignore the API bounds.")
        log.error("=" * 68)
        raise SystemExit(2)

    if failures:
        log.error("%d window(s) failed and were not recorded; re-run to retry: %s",
                  len(failures), ", ".join(failures))
        raise SystemExit(1)

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
                    help="ignore API date bounds and use config.py dates "
                         "(empty windows are then never marked, only re-checked)")
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

    if args.nrt:
        # NRT needs epoch-aligned windows and refetching of windows still
        # filling, which this archive loop does not do. It has its own script.
        sys.exit("NRT moved to its own extractor: python extract_firms_nrt.py")

    run(config.FIRMS_SENSORS_SP, args.out / "sp", args.start, args.end,
        use_availability=not args.no_availability)