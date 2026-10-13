"""
Extract MODIS MOD13Q1 NDVI time series from AppEEARS as CSV.

Why point samples and not area samples: AppEEARS *area* requests return raster
(GeoTIFF / NetCDF-4). Only *point* requests return CSV. Since the warehouse
needs tabular rows, we lay a regular grid of points over the study area and
submit those.

This script is asynchronous by nature. Tasks are queued server-side and can take
hours. It is safe to run repeatedly: submitted task ids are cached to disk, and
re-running picks up status and downloads anything that has finished.

Usage:
    export EARTHDATA_USER=your_username
    export EARTHDATA_PASS=your_password

    python extract_appeears.py --submit    # create and queue the tasks
    python extract_appeears.py --status    # check progress
    python extract_appeears.py --download  # fetch whatever has finished
    python extract_appeears.py --wait      # poll + download until all done

Tasks are queued server-side and can take hours. Until one reaches "done",
_tasks.json is the only local file -- that is expected, not a failure.

Register for Earthdata Login at https://urs.earthdata.nasa.gov/

CAVEAT: AppEEARS enforces a per-request limit on the number of coordinates,
and the exact value has changed over time. config.NDVI_POINTS_PER_TASK is set
conservatively. If submission is rejected, lower it before assuming the script
is broken.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import requests

import config
from common import download, get_logger, record_manifest, session

log = get_logger("appeears")

TASK_CACHE = config.RAW_NDVI / "_tasks.json"


# ---------------------------------------------------------------- auth

def login(sess: requests.Session) -> str:
    user = os.environ.get("EARTHDATA_USER")
    password = os.environ.get("EARTHDATA_PASS")
    if not (user and password):
        sys.exit("Set EARTHDATA_USER and EARTHDATA_PASS. "
                 "Register at https://urs.earthdata.nasa.gov/")

    resp = sess.post(f"{config.APPEEARS_BASE}/login",
                     auth=(user, password), timeout=60)
    resp.raise_for_status()
    token = resp.json()["token"]
    log.info("authenticated with AppEEARS")
    return token


def auth_header(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------- grid

def build_point_grid() -> list[dict]:
    """
    Regular lat/lon grid over the study bounding box.

    A 0.1 degree step over Thailand is roughly 8,000 points. At ~23 sixteen-day
    composites per year over 14 years that is well into the millions of rows,
    which is the point.

    Note this grid is naive: it includes sea and non-vegetated cells. Filter in
    staging using the admin boundaries rather than here, so the raw zone stays
    a faithful record of what was requested.
    """
    west, south, east, north = (float(x) for x in config.BBOX.split(","))
    step = config.NDVI_GRID_STEP

    points = []
    idx = 0
    lat = south
    while lat <= north:
        lon = west
        while lon <= east:
            points.append({
                "id": f"p{idx:06d}",
                "longitude": round(lon, 4),
                "latitude": round(lat, 4),
                "category": "grid",
            })
            idx += 1
            lon += step
        lat += step

    log.info("built grid of %d points at %.2f degree spacing", len(points), step)
    return points


def chunks(seq: list, size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


# ---------------------------------------------------------------- tasks

def build_task(points: list[dict], name: str) -> dict:
    return {
        "task_type": "point",
        "task_name": name,
        "params": {
            "dates": [{
                "startDate": config.START_DATE.strftime("%m-%d-%Y"),
                "endDate": config.END_DATE.strftime("%m-%d-%Y"),
            }],
            "layers": [
                {"product": config.NDVI_PRODUCT, "layer": layer}
                for layer in config.NDVI_LAYERS
            ],
            "coordinates": points,
        },
    }


def submit(sess: requests.Session, token: str) -> None:
    points = build_point_grid()
    batches = list(chunks(points, config.NDVI_POINTS_PER_TASK))
    log.info("submitting %d task(s)", len(batches))

    cache = load_cache()

    for n, batch in enumerate(batches):
        name = f"itcs544_ndvi_{config.ISO3}_{n:03d}"
        if name in cache:
            log.info("already submitted: %s", name)
            continue

        resp = sess.post(f"{config.APPEEARS_BASE}/task",
                         json=build_task(batch, name),
                         headers=auth_header(token), timeout=120)

        if resp.status_code >= 400:
            log.error("submission rejected (%s): %s", resp.status_code, resp.text[:300])
            log.error("If this mentions coordinate limits, lower "
                      "NDVI_POINTS_PER_TASK in config.py and re-run.")
            sys.exit(1)

        task_id = resp.json()["task_id"]
        cache[name] = {"task_id": task_id, "points": len(batch)}
        save_cache(cache)
        log.info("queued %s -> %s", name, task_id)
        time.sleep(config.PAUSE_SECONDS)

    log.info("all tasks queued. Processing is server-side and can take hours. "
             "Re-run with --status to check.")


def status(sess: requests.Session, token: str, verbose: bool = True) -> dict:
    """
    Poll every cached task. Returns {task_name: state}.

    AppEEARS states seen in practice: pending, queued, processing, done, error.
    Errors matter: a failed task never becomes downloadable, so --download would
    silently skip it forever. Those are called out explicitly.
    """
    cache = load_cache()
    if not cache:
        sys.exit("No tasks cached. Run with --submit first.")

    states: dict[str, str] = {}
    for name, info in cache.items():
        try:
            resp = sess.get(f"{config.APPEEARS_BASE}/task/{info['task_id']}",
                            headers=auth_header(token), timeout=60)
            resp.raise_for_status()
            states[name] = resp.json().get("status", "unknown")
        except Exception as exc:
            states[name] = f"lookup-failed ({type(exc).__name__})"

        if verbose:
            log.info("%-28s %s", name, states[name])

    counts: dict[str, int] = {}
    for state in states.values():
        counts[state] = counts.get(state, 0) + 1

    summary = "  ".join(f"{k}={v}" for k, v in sorted(counts.items()))
    log.info("SUMMARY: %s  (%d tasks)", summary, len(states))

    failed = [n for n, s in states.items() if "error" in s.lower()
              or "failed" in s.lower()]
    if failed:
        log.error("%d task(s) failed and will never download: %s",
                  len(failed), ", ".join(failed))
        log.error("Remove them from %s and re-run --submit to retry.", TASK_CACHE)

    return states


def wait_and_download(sess: requests.Session, token: str,
                      interval: int = 300, max_hours: float = 24.0) -> None:
    """
    Poll until every task is finished, downloading each as it completes.

    Designed to be left running. Safe to interrupt: already-downloaded files are
    skipped on the next run, so nothing is lost or repeated.
    """
    deadline = time.time() + max_hours * 3600
    round_n = 0

    while time.time() < deadline:
        round_n += 1
        log.info("--- poll %d ---", round_n)
        states = status(sess, token, verbose=False)

        # Download whatever is ready now rather than waiting for the last task.
        if any(s == "done" for s in states.values()):
            download_bundles(sess, token)

        outstanding = [n for n, s in states.items()
                       if s not in ("done",) and "error" not in s.lower()
                       and "failed" not in s.lower()]

        if not outstanding:
            log.info("all tasks finished")
            return

        log.info("%d task(s) still running; next poll in %d min",
                 len(outstanding), interval // 60)
        time.sleep(interval)

    log.warning("gave up after %.1f hours; re-run --wait to continue", max_hours)


def download_bundles(sess: requests.Session, token: str) -> None:
    cache = load_cache()
    config.RAW_NDVI.mkdir(parents=True, exist_ok=True)

    for name, info in cache.items():
        task_id = info["task_id"]

        resp = sess.get(f"{config.APPEEARS_BASE}/bundle/{task_id}",
                        headers=auth_header(token), timeout=60)
        if resp.status_code != 200:
            log.info("%s not ready yet", name)
            continue

        for item in resp.json().get("files", []):
            fname = item["file_name"]
            if not fname.lower().endswith(".csv"):
                continue  # skip the json metadata sidecars

            target = config.RAW_NDVI / f"{name}__{Path(fname).name}"
            if target.exists():
                continue

            url = f"{config.APPEEARS_BASE}/bundle/{task_id}/{item['file_id']}"
            r = sess.get(url, headers=auth_header(token), stream=True, timeout=600)
            r.raise_for_status()

            target.parent.mkdir(parents=True, exist_ok=True)
            with open(target, "wb") as fh:
                for chunk in r.iter_content(1 << 20):
                    fh.write(chunk)

            try:
                with open(target) as fh:
                    rows = max(0, sum(1 for _ in fh) - 1)
            except Exception:
                rows = None

            record_manifest(target, source_url=url, rows=rows,
                            extra={"task": name, "task_id": task_id})
            log.info("downloaded %s (%s rows)", target.name,
                     f"{rows:,}" if rows is not None else "?")


# ---------------------------------------------------------------- cache

def load_cache() -> dict:
    if TASK_CACHE.exists():
        return json.loads(TASK_CACHE.read_text())
    return {}


def save_cache(cache: dict) -> None:
    TASK_CACHE.parent.mkdir(parents=True, exist_ok=True)
    TASK_CACHE.write_text(json.dumps(cache, indent=2))


# ---------------------------------------------------------------- main

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--submit", action="store_true")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--download", action="store_true")
    ap.add_argument("--wait", action="store_true",
                    help="poll until all tasks finish, downloading as they complete")
    ap.add_argument("--interval", type=int, default=300,
                    help="seconds between polls when using --wait (default 300)")
    ap.add_argument("--max-hours", type=float, default=24.0,
                    help="give up after this many hours (default 24)")
    args = ap.parse_args()

    if not any([args.submit, args.status, args.download, args.wait]):
        ap.error("pick one of --submit, --status, --download, --wait")

    s = session()
    tok = login(s)

    if args.submit:
        submit(s, tok)
    if args.status:
        status(s, tok)
    if args.download:
        download_bundles(s, tok)
    if args.wait:
        wait_and_download(s, tok, interval=args.interval, max_hours=args.max_hours)