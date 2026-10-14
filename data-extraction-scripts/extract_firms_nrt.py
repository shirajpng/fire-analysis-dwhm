"""
Near-real-time (NRT) FIRMS fire detections, kept apart from the SP archive.

Why NRT needs capturing at all
------------------------------
NRT is published within hours of the overpass and later replaced by Standard
Processing (SP). The API only serves NRT for dates SP does not cover yet: in
Sep 2026, MODIS / S-NPP / NOAA-20 NRT starts on 2026-07-01, the day after SP
ends. When SP catches up, those NRT days vanish from the API. Having both
versions of the same fires -- the "NRT superseded by science quality" dedup
story -- is only possible if NRT is captured now and kept. Run this regularly
(weekly is enough; daily is better).

NOAA-21 is the exception: it has no SP product, and its NRT goes back to
2024-01-17. This is the only source for NOAA-21.

How this differs from extract_firms.py
--------------------------------------
1. Window starts are aligned to a fixed epoch, so the same dates always map to
   the same file. (Starting from "today minus 60 days" shifts every run and
   produces overlapping, duplicated windows.)
2. Recent windows are still filling. A window is frozen only once it ends more
   than FIRMS_NRT_SETTLE_DAYS before the newest date the API serves; until then
   it is refetched and overwritten each run.
3. A failed request never becomes an empty marker. Only a settled window that
   the API confirmed empty gets a zero-byte file.
4. Files are never deleted when a date leaves the API. That retained NRT is the
   point of the exercise.

Known schema difference for staging: NRT CSVs have no `type` column (SP does),
so gas flare / volcano flags must be derived for NRT rows, e.g. by matching
against locations SP flagged as static sources.

Usage:
    python extract_firms_nrt.py --dry-run          # plan only, one API call
    python extract_firms_nrt.py                    # all NRT sensors
    python extract_firms_nrt.py --sensor VIIRS_NOAA21_NRT

Output:
    raw/firms/nrt/sensor=VIIRS_SNPP_NRT/year=2026/2026-07-01.csv
    raw/firms/nrt/_windows.json     which windows are final vs still refreshing
"""

from __future__ import annotations

import argparse
import io
import json
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests

import config
from common import get_logger, get_with_retry, record_manifest, session
from extract_firms import fetch_availability, map_key

log = get_logger("firms-nrt")

OUT = config.RAW_FIRMS / "nrt"
STATE = OUT / "_windows.json"
STEP = timedelta(days=config.FIRMS_DAY_RANGE)


class FetchError(Exception):
    """The API did not give a clean answer. Never recorded as empty."""


# ---------------------------------------------------------------- windows

def align(d: date) -> date:
    """Snap a date back to the start of its epoch-aligned window."""
    offset = (d - config.FIRMS_NRT_EPOCH).days % config.FIRMS_DAY_RANGE
    return d - timedelta(days=offset)


def windows(lo: date, hi: date) -> list[date]:
    starts, cursor = [], align(lo)
    while cursor <= hi:
        starts.append(cursor)
        cursor += STEP
    return starts


def is_settled(start: date, newest: date) -> bool:
    window_end = start + STEP - timedelta(days=1)
    return window_end <= newest - timedelta(days=config.FIRMS_NRT_SETTLE_DAYS)


def target_for(sensor: str, start: date) -> Path:
    return OUT / f"sensor={sensor}" / f"year={start.year}" / f"{start.isoformat()}.csv"


# ---------------------------------------------------------------- fetch

def fetch(key: str, sensor: str, start: date, sess) -> pd.DataFrame:
    """
    One window. Returns a DataFrame, empty if the API says there were no fires.

    An empty window comes back as a header-only CSV with HTTP 200. Anything
    that does not start with the CSV header is treated as a failure.
    """
    url = (f"{config.FIRMS_BASE}/area/csv/{key}/{sensor}/{config.BBOX}/"
           f"{config.FIRMS_DAY_RANGE}/{start.isoformat()}")
    try:
        resp = get_with_retry(url, sess, timeout=180)
    except requests.HTTPError as exc:
        body = (exc.response.text or "").strip()[:200] if exc.response is not None else ""
        code = exc.response.status_code if exc.response is not None else "?"
        raise FetchError(f"HTTP {code}: {body or '(empty body)'}") from exc
    except RuntimeError as exc:
        raise FetchError("exhausted retries") from exc

    text = resp.text.strip()
    if not text.startswith("latitude"):
        if "invalid" in text.lower():
            sys.exit(f"API rejected the request: {text[:200]}")
        raise FetchError(f"unexpected body: {text[:200] or '(empty)'}")

    return pd.read_csv(io.StringIO(text))


def write_atomic(df: pd.DataFrame, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    part = target.with_suffix(target.suffix + ".part")
    df.to_csv(part, index=False)
    part.rename(target)


# ---------------------------------------------------------------- run

def run(sensors: list[str], dry_run: bool) -> None:
    key = map_key()
    bounds = fetch_availability(key)
    if not bounds:
        # Without the API's newest date there is no safe way to tell a settled
        # window from one still filling, and out-of-range dates return a valid
        # but empty CSV. Refuse rather than write misleading empties.
        sys.exit("Could not read FIRMS availability; not guessing NRT bounds.")

    state = load_state()
    sess = session()
    total_rows, failures = 0, []

    for sensor in sensors:
        if sensor not in bounds:
            log.warning("%s: not listed by the availability endpoint, skipping", sensor)
            continue

        lo, newest = bounds[sensor]
        lo = max(lo, config.START_DATE)
        starts = windows(lo, newest)

        final = [s for s in starts if state.get(f"{sensor}/{s}", {}).get("final")]
        todo = [s for s in starts if s not in final]
        log.info("%s: API serves %s .. %s | %d windows, %d final, %d to fetch",
                 sensor, lo, newest, len(starts), len(final), len(todo))

        if dry_run:
            refresh = [s for s in todo if not is_settled(s, newest)]
            if refresh:
                log.info("  still filling (refetched every run): %s",
                         ", ".join(str(s) for s in refresh))
            continue

        sensor_rows = 0
        for start in todo:
            settled = is_settled(start, newest)
            target = target_for(sensor, start)

            try:
                df = fetch(key, sensor, start, sess)
            except FetchError as exc:
                log.error("%-18s %s -> FAILED, will retry next run: %s",
                          sensor, start, exc)
                failures.append(f"{sensor} {start}")
                time.sleep(config.PAUSE_SECONDS)
                continue

            if df.empty:
                if settled:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(b"")  # confirmed-empty marker
                elif target.exists() and target.stat().st_size > 0:
                    # Rows seen earlier, none now. Keep what we had; the API
                    # does not normally retract NRT detections.
                    log.warning("%-18s %s -> now empty, keeping earlier file",
                                sensor, start)
                rows = 0
            else:
                df["src_sensor"] = sensor
                df["src_window_start"] = start.isoformat()
                df["src_ingested_at"] = pd.Timestamp.now(tz="UTC").isoformat()
                write_atomic(df, target)
                rows = len(df)
                record_manifest(target, source_url=f"firms:{sensor}:{start}", rows=rows,
                                extra={"sensor": sensor, "final": settled})

            state[f"{sensor}/{start}"] = {
                "final": settled,
                "rows": rows,
                "fetched_at": pd.Timestamp.now(tz="UTC").isoformat(),
            }
            save_state(state)

            sensor_rows += rows
            log.info("%-18s %s -> %6d rows%s", sensor, start, rows,
                     "" if settled else "  (still filling)")
            time.sleep(config.PAUSE_SECONDS)

        log.info("== %s: %d rows fetched this run ==", sensor, sensor_rows)
        total_rows += sensor_rows

    if dry_run:
        return

    log.info("NRT TOTAL this run: %d rows", total_rows)
    if failures:
        log.error("%d window(s) failed and were not recorded; re-run to retry: %s",
                  len(failures), ", ".join(failures))
        raise SystemExit(1)


# ---------------------------------------------------------------- state

def load_state() -> dict:
    if STATE.exists():
        return json.loads(STATE.read_text())
    return {}


def save_state(state: dict) -> None:
    STATE.parent.mkdir(parents=True, exist_ok=True)
    part = STATE.with_suffix(".json.part")
    part.write_text(json.dumps(state, indent=1, sort_keys=True))
    part.rename(STATE)


# ---------------------------------------------------------------- main

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sensor", action="append", choices=list(config.FIRMS_SENSORS_NRT),
                    help="limit to one sensor; repeat for several (default: all)")
    ap.add_argument("--dry-run", action="store_true",
                    help="show the window plan without fetching any windows")
    args = ap.parse_args()

    run(args.sensor or list(config.FIRMS_SENSORS_NRT), dry_run=args.dry_run)
