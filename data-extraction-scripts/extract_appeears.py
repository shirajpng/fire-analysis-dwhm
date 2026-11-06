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

# Prefix of the status recorded when a task's status could not be fetched.
LOOKUP_FAILED = "lookup-failed"


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

    A 0.1 degree step over the Thailand box is roughly 14,600 points. At ~23
    sixteen-day composites per year over 14 years that is well into the
    millions of rows, which is the point.

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


def server_tasks(sess: requests.Session, token: str) -> dict[str, dict]:
    """
    Every task on the account that is not failed, keyed by task_name.

    If two live tasks share a name, the most recently created one wins.
    Shared by the NDVI and land cover extractors.
    """
    found: dict[str, dict] = {}
    offset, page = 0, 100
    while True:
        resp = sess.get(f"{config.APPEEARS_BASE}/task",
                        params={"limit": page, "offset": offset},
                        headers=auth_header(token), timeout=120)
        resp.raise_for_status()
        batch = resp.json()
        for t in sorted(batch, key=lambda t: t.get("created") or ""):
            if not is_failed(t.get("status") or ""):
                found[t["task_name"]] = t
        if len(batch) < page:
            return found
        offset += page


def post_task(sess: requests.Session, token: str, name: str, payload: dict,
              limit_setting: str) -> str:
    """
    Submit one task and return its id. Shared by NDVI and land cover.

    A 5xx or a dropped connection does NOT mean the task was not created:
    AppEEARS can accept the task and then time out at the gateway. Blindly
    retrying would queue a duplicate. So after any ambiguous failure, look the
    task up by name first and only resubmit if it is genuinely absent.

    `limit_setting` names the config.py knob to lower if AppEEARS rejects the
    request outright, so the error message points at the right one.
    """
    for attempt in range(config.MAX_RETRIES):
        try:
            resp = sess.post(f"{config.APPEEARS_BASE}/task", json=payload,
                             headers=auth_header(token), timeout=180)
            if resp.status_code < 400:
                return resp.json()["task_id"]
            if resp.status_code < 500 and resp.status_code != 429:
                log.error("submission rejected (%s): %s",
                          resp.status_code, resp.text[:300])
                log.error("If this mentions coordinate limits, lower "
                          "%s in config.py and re-run.", limit_setting)
                sys.exit(1)
            problem = f"HTTP {resp.status_code}"
        except requests.RequestException as exc:
            problem = type(exc).__name__

        wait = 15 * 2 ** attempt
        log.warning("%s on %s (attempt %d/%d); checking server in %ds",
                    problem, name, attempt + 1, config.MAX_RETRIES, wait)
        time.sleep(wait)

        try:
            existing = server_tasks(sess, token).get(name)
        except requests.RequestException as exc:
            log.warning("could not list tasks (%s); will retry submit",
                        type(exc).__name__)
            continue
        if existing:
            log.info("%s was created despite the error; adopting it", name)
            return existing["task_id"]

    log.error("gave up on %s after %d attempts. AppEEARS looks unhealthy; "
              "re-run --submit later, completed tasks are kept.",
              name, config.MAX_RETRIES)
    sys.exit(1)


def submit(sess: requests.Session, token: str) -> None:
    points = build_point_grid()
    batches = list(chunks(points, config.NDVI_POINTS_PER_TASK))
    tasks = [(f"itcs544_ndvi_{config.ISO3}_{n:03d}", batch)
             for n, batch in enumerate(batches)]
    log.info("submitting %d task(s)", len(tasks))

    cache = load_cache()

    # Adopt tasks that exist server-side but never reached the cache, e.g. a
    # previous run was killed between creating a task and saving its id.
    remote = server_tasks(sess, token)
    for name, batch in tasks:
        if name not in cache and name in remote:
            cache[name] = {"task_id": remote[name]["task_id"], "points": len(batch)}
            log.info("adopted existing server task %s", name)
    save_cache(cache)

    for name, batch in tasks:
        if name in cache:
            log.info("already submitted: %s", name)
            continue

        task_id = post_task(sess, token, name, build_task(batch, name),
                            "NDVI_POINTS_PER_TASK")
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
            states[name] = f"{LOOKUP_FAILED} ({type(exc).__name__})"

        if verbose:
            log.info("%-28s %s", name, states[name])

    counts: dict[str, int] = {}
    for state in states.values():
        counts[state] = counts.get(state, 0) + 1

    summary = "  ".join(f"{k}={v}" for k, v in sorted(counts.items()))
    log.info("SUMMARY: %s  (%d tasks)", summary, len(states))

    report_problems(states, TASK_CACHE)
    return states


def is_failed(state: str) -> bool:
    """
    True only for tasks AppEEARS itself reports as failed.

    A status we could not fetch says nothing about the task, so it is not a
    failure -- even though the exception name in it often contains "Error".
    """
    if state.startswith(LOOKUP_FAILED):
        return False
    return "error" in state.lower() or "failed" in state.lower()


def report_problems(states: dict[str, str], task_cache: Path) -> None:
    unreachable = [n for n, s in states.items() if s.startswith(LOOKUP_FAILED)]
    if unreachable:
        log.warning("%d task status lookup(s) failed; they will be checked "
                    "again on the next poll: %s", len(unreachable), ", ".join(unreachable))

    failed = [n for n, s in states.items() if is_failed(s)]
    if failed:
        log.error("%d task(s) failed and will never download: %s",
                  len(failed), ", ".join(failed))
        log.error("Remove them from %s and re-run --submit to retry.", task_cache)


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

        outstanding = [n for n, s in states.items() if s != "done" and not is_failed(s)]

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
            if not fetch_bundle_file(sess, token, url, target):
                continue

            try:
                with open(target) as fh:
                    rows = max(0, sum(1 for _ in fh) - 1)
            except Exception:
                rows = None

            record_manifest(target, source_url=url, rows=rows,
                            extra={"task": name, "task_id": task_id})
            log.info("downloaded %s (%s rows)", target.name,
                     f"{rows:,}" if rows is not None else "?")


def fetch_bundle_file(sess: requests.Session, token: str, url: str,
                      target: Path) -> bool:
    """
    Download one bundle file to target. Returns False if every attempt failed.

    The bundle endpoint 302s to a presigned CloudFront URL, and that second
    hop intermittently returns an empty-bodied 400. A fresh redirect is signed
    on every attempt, so retrying clears it. Without this a single bad
    signature aborted the whole run and took the other finished tasks with it.
    """
    part = target.with_suffix(target.suffix + ".part")
    for attempt in range(1, config.MAX_RETRIES + 1):
        try:
            r = sess.get(url, headers=auth_header(token), stream=True, timeout=600)
            r.raise_for_status()

            target.parent.mkdir(parents=True, exist_ok=True)
            with open(part, "wb") as fh:
                for chunk in r.iter_content(1 << 20):
                    fh.write(chunk)
            part.rename(target)
            return True
        except requests.RequestException as exc:
            part.unlink(missing_ok=True)
            code = getattr(exc.response, "status_code", "?")
            if attempt == config.MAX_RETRIES:
                log.error("%s failed after %d attempts (HTTP %s); "
                          "left for the next run", target.name, attempt, code)
            else:
                log.warning("%s attempt %d/%d failed (HTTP %s); retrying",
                            target.name, attempt, config.MAX_RETRIES, code)
                time.sleep(config.PAUSE_SECONDS * 2 ** attempt)
    return False


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