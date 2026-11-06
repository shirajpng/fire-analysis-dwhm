"""
Extract MODIS MOD13Q1 NDVI time series from AppEEARS as CSV, incrementally.

This module is also the AppEEARS engine for extract_landcover.py: both define
a Job and hand it to main(), so they share one tested implementation of
submission, collection and resubmission.

Why point samples and not area samples: AppEEARS *area* requests return raster
(GeoTIFF / NetCDF-4). Only *point* requests return CSV. Since the warehouse
needs tabular rows, we lay a regular grid of points over the study area and
submit those.

Incremental by design
---------------------
The grid is split into fixed batches of points. Each batch is requested as a
series of date ranges, one AppEEARS task per range:

    itcs544_ndvi_THA_007                      legacy: one task, START_DATE .. first run
    itcs544_ndvi_THA_007_20260830_20261106    the next range, and so on

`--sync` is what run_all.sh calls. It never blocks:
    1. refresh the status of every outstanding task
    2. download every finished one
    3. request the next range for each batch that is due

A batch is due when it has no outstanding task, its newest downloaded
observation is at least `new_data_days` old, and its last request is at least
`recheck_days` old. Coverage comes from the dates actually present in the
downloaded CSVs, not from the requested range, because composites are
published late: a range requested on 15 Sep cannot contain the composite dated
14 Sep, so the next range starts the day after the newest observation on disk.
Consecutive ranges may return the same composite at their boundary; staging
dedupes on (ID, Date).

AppEEARS deletes results about 30 days after a task finishes ("expired"). A
task that expires, or fails, before it is downloaded is dropped from coverage
and its range is requested again on the next run. The Airflow DAG runs --sync
daily, which collects everything well inside that window.

Usage:
    python extract_appeears.py --sync      # collect finished, request due ranges
    python extract_appeears.py --dry-run   # show what --sync would request
    python extract_appeears.py --status
    python extract_appeears.py --download  # collect finished tasks only
    python extract_appeears.py --submit    # request due ranges only
    python extract_appeears.py --wait      # block until outstanding tasks are collected

Credentials: EARTHDATA_USER / EARTHDATA_PASS (from .env). Register at
https://urs.earthdata.nasa.gov/ and accept the AppEEARS EULA once.

CAVEAT: AppEEARS limits coordinates per request, and the limit has changed
over time. If submission is rejected, lower the *_POINTS_PER_TASK setting --
but note that changing it re-cuts the batches, which the cache refuses (see
batches_for).
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

import config
from common import get_logger, record_manifest, session

log = get_logger("appeears")


# ---------------------------------------------------------------- job spec

@dataclass(frozen=True)
class Job:
    """Everything that differs between the NDVI and land cover extractions."""

    label: str                 # "ndvi" / "landcover": task names and log lines
    product: str
    layers: tuple[str, ...]
    out_dir: Path
    points_per_task: int
    limit_setting: str         # config.py knob named in the "rejected" error
    first_start: date          # start of the very first range of every batch
    new_data_days: int         # newest observation must be this old to ask again
    recheck_days: int          # and the last request at least this old

    @property
    def cache_path(self) -> Path:
        return self.out_dir / "_tasks.json"

    @property
    def prefix(self) -> str:
        return f"itcs544_{self.label}_{config.ISO3}"


NDVI = Job(
    label="ndvi",
    product=config.NDVI_PRODUCT,
    layers=tuple(config.NDVI_LAYERS),
    out_dir=config.RAW_NDVI,
    points_per_task=config.NDVI_POINTS_PER_TASK,
    limit_setting="NDVI_POINTS_PER_TASK",
    first_start=config.START_DATE,
    new_data_days=config.NDVI_NEW_DATA_DAYS,
    recheck_days=config.NDVI_RECHECK_DAYS,
)


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

    At 0.1 degree over the Thailand box this is 14,560 points (160 x 91;
    floating-point accumulation stops the latitude loop just short of 21.0).
    Deterministic, so point ids line up across NDVI and land cover.

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


# ---------------------------------------------------------------- statuses

def is_failed(state: str) -> bool:
    """True only for tasks AppEEARS itself reports as failed."""
    low = state.lower()
    return "error" in low or "failed" in low


def is_gone(state: str) -> bool:
    """AppEEARS will never serve results for this task: it failed or expired."""
    return is_failed(state) or state.lower() == "expired"


def is_dead(entry: dict) -> bool:
    """Lost before we downloaded it. Ignored for coverage; its range is re-requested."""
    return not entry.get("downloaded") and is_gone(entry.get("status") or "")


def is_outstanding(entry: dict) -> bool:
    """Submitted, not downloaded, and still able to produce results."""
    return not entry.get("downloaded") and not is_dead(entry)


# ---------------------------------------------------------------- cache

def load_cache(job: Job) -> dict:
    """
    {"version": 2, "grid": {...}, "tasks": {task_name: entry}}

    The old one-shot format was a flat {task_name: {task_id, points}}; it is
    wrapped here and filled in by migrate().
    """
    if not job.cache_path.exists():
        return {"version": 2, "grid": None, "tasks": {}}
    data = json.loads(job.cache_path.read_text())
    if "tasks" not in data:
        data = {"version": 2, "grid": None, "tasks": data}
    return data


def save_cache(job: Job, cache: dict) -> None:
    job.cache_path.parent.mkdir(parents=True, exist_ok=True)
    part = job.cache_path.with_suffix(".json.part")
    part.write_text(json.dumps(cache, indent=1, sort_keys=True))
    part.rename(job.cache_path)


def grid_signature(job: Job) -> dict:
    return {"bbox": config.BBOX, "step": config.NDVI_GRID_STEP,
            "points_per_task": job.points_per_task}


def batches_for(job: Job, cache: dict) -> list[list[dict]]:
    """
    The point batches, refusing to continue if the grid was re-cut.

    Batch numbers are only meaningful while the grid and batch size stay the
    same. Changing either would make "batch 7 is covered to August" refer to
    different points, and silently leave gaps.
    """
    sig = grid_signature(job)
    if cache.get("grid") and cache["grid"] != sig:
        sys.exit(f"{job.label}: grid settings changed since the cached tasks were created.\n"
                 f"  cached:  {cache['grid']}\n  current: {sig}\n"
                 f"Restore the old settings in config.py, or move {job.out_dir} aside "
                 "to start a fresh extraction.")
    cache["grid"] = sig
    return list(chunks(build_point_grid(), job.points_per_task))


def check_points(job: Job, cache: dict, batches: list[list[dict]]) -> None:
    """Cross-check cached point counts, which also catches a re-cut legacy cache."""
    for name, e in cache["tasks"].items():
        b = e.get("batch")
        if b is None or not e.get("points"):
            continue
        if b >= len(batches) or e["points"] != len(batches[b]):
            sys.exit(f"{job.label}: cached task {name} has {e['points']} points but batch "
                     f"{b} of the current grid has "
                     f"{len(batches[b]) if b < len(batches) else 'no'} points. The grid "
                     "or batch size changed; see batches_for().")


# ---------------------------------------------------------------- local files

def scan_csv(path: Path) -> tuple[int, str | None]:
    """(data rows, newest value in the Date column) for one AppEEARS CSV."""
    rows, newest = 0, None
    with open(path, newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader, [])
        col = header.index("Date") if "Date" in header else None
        for row in reader:
            rows += 1
            if col is not None and len(row) > col:
                d = row[col][:10]
                if newest is None or d > newest:
                    newest = d
    return rows, newest


def task_files(job: Job, name: str) -> list[Path]:
    # The double underscore separates task name from AppEEARS' own file name,
    # so legacy "..._007" never matches "..._007_20260830_20261106".
    return sorted(job.out_dir.glob(f"{name}__*.csv"))


def record_local(job: Job, name: str, entry: dict) -> list[Path]:
    """Store file names, row count and newest observation of a task's CSVs."""
    files = task_files(job, name)
    rows, newest = 0, None
    for p in files:
        n, d = scan_csv(p)
        rows += n
        if d and (newest is None or d > newest):
            newest = d
    entry["files"] = [p.name for p in files]
    entry["rows"] = rows
    entry["data_end"] = newest
    return files


# ---------------------------------------------------------------- API

def task_info(sess: requests.Session, token: str, task_id: str) -> dict:
    resp = sess.get(f"{config.APPEEARS_BASE}/task/{task_id}",
                    headers=auth_header(token), timeout=60)
    resp.raise_for_status()
    return resp.json()


def server_tasks(sess: requests.Session, token: str) -> dict[str, dict]:
    """
    Every task on the account that can still produce results, keyed by name.

    Failed and expired tasks are left out, so they are never adopted in place
    of a fresh submission. If two live tasks share a name, the newest wins.
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
            if not is_gone(t.get("status") or ""):
                found[t["task_name"]] = t
        if len(batch) < page:
            return found
        offset += page


def post_task(sess: requests.Session, token: str, name: str, payload: dict,
              limit_setting: str) -> str:
    """
    Submit one task and return its id.

    A 5xx or a dropped connection does NOT mean the task was not created:
    AppEEARS can accept the task and then time out at the gateway. Blindly
    retrying would queue a duplicate. So after any ambiguous failure, look the
    task up by name first and only resubmit if it is genuinely absent.
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
              "re-run later, completed tasks are kept.", name, config.MAX_RETRIES)
    sys.exit(1)


def fetch_bundle_file(sess: requests.Session, token: str, url: str,
                      target: Path) -> bool:
    """
    Download one bundle file to target. Returns False if every attempt failed.

    The bundle endpoint 302s to a presigned CloudFront URL, and that second
    hop intermittently returns an empty-bodied 400. A fresh redirect is signed
    on every attempt, so retrying clears it.
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


# ---------------------------------------------------------------- steps

LEGACY_NAME = re.compile(r"_(\d{3})$")


def migrate(job: Job, cache: dict, sess: requests.Session, token: str) -> None:
    """
    Fill in batch, date range and download state for tasks cached by the old
    one-shot version, which recorded only {task_id, points}.

    The range comes from AppEEARS itself (it keeps task parameters after the
    results expire), and download state from the CSVs on disk. Any lookup
    failure stops the run: guessing a range here would mis-state coverage.
    """
    todo = [n for n, e in cache["tasks"].items() if "start" not in e or "batch" not in e]
    if not todo:
        return
    log.info("%s: upgrading %d cached task(s) from the one-shot format", job.label, len(todo))

    for name in todo:
        e = cache["tasks"][name]
        m = LEGACY_NAME.search(name)
        if not m:
            sys.exit(f"{job.label}: cannot tell which batch cached task {name} belongs to")
        e["batch"] = int(m.group(1))

        info = task_info(sess, token, e["task_id"])
        rng = info["params"]["dates"][0]
        e["start"] = datetime.strptime(rng["startDate"], "%m-%d-%Y").date().isoformat()
        e["end"] = datetime.strptime(rng["endDate"], "%m-%d-%Y").date().isoformat()
        e["status"] = info.get("status", "unknown")

        e["downloaded"] = bool(record_local(job, name, e))


def refresh(job: Job, cache: dict, sess: requests.Session, token: str) -> None:
    """Update the status of every outstanding task. A failed lookup keeps the old one."""
    for name, e in cache["tasks"].items():
        if not is_outstanding(e):
            continue
        try:
            e["status"] = task_info(sess, token, e["task_id"]).get("status", "unknown")
        except Exception as exc:
            log.warning("%s: status lookup failed (%s); checked again next run",
                        name, type(exc).__name__)


def download_finished(job: Job, cache: dict, sess: requests.Session,
                      token: str) -> tuple[int, int]:
    """
    Download every finished task not yet on disk. Returns (collected, failed).

    A task is marked downloaded only once every CSV in its bundle is on disk,
    so a partial download is completed on the next run, not mistaken for coverage.
    """
    job.out_dir.mkdir(parents=True, exist_ok=True)
    collected = failed = 0

    for name, e in cache["tasks"].items():
        if not is_outstanding(e) or e.get("status") != "done":
            continue

        resp = sess.get(f"{config.APPEEARS_BASE}/bundle/{e['task_id']}",
                        headers=auth_header(token), timeout=60)
        if resp.status_code != 200:
            log.warning("%s: done, but bundle unavailable (HTTP %s); retried next run",
                        name, resp.status_code)
            failed += 1
            continue

        complete = True
        for item in resp.json().get("files", []):
            fname = item["file_name"]
            if not fname.lower().endswith(".csv"):
                continue  # skip the json metadata sidecars

            target = job.out_dir / f"{name}__{Path(fname).name}"
            if target.exists():
                continue

            url = f"{config.APPEEARS_BASE}/bundle/{e['task_id']}/{item['file_id']}"
            if not fetch_bundle_file(sess, token, url, target):
                complete = False
                continue

            rows, _ = scan_csv(target)
            record_manifest(target, source_url=url, rows=rows,
                            extra={"task": name, "task_id": e["task_id"],
                                   "product": job.product,
                                   "range": f"{e['start']}..{e['end']}"})
            log.info("downloaded %s (%s rows)", target.name, f"{rows:,}")

        if not complete:
            failed += 1
            continue

        record_local(job, name, e)
        e["downloaded"] = True
        save_cache(job, cache)
        collected += 1
        log.info("%s: collected, %s rows, newest observation %s",
                 name, f"{e['rows']:,}", e["data_end"] or "(none)")

    return collected, failed


def due_ranges(job: Job, cache: dict, n_batches: int,
               today: date) -> tuple[list[tuple[int, date, date]], int, int]:
    """
    Which batches need a new range. Returns (due, waiting, not_due), where
    due is [(batch, start, end)].
    """
    by_batch: dict[int, list[dict]] = {}
    for e in cache["tasks"].values():
        by_batch.setdefault(e["batch"], []).append(e)

    due, waiting, not_due = [], 0, 0
    for b in range(n_batches):
        entries = by_batch.get(b, [])

        if any(is_outstanding(e) for e in entries):
            waiting += 1
            continue

        ends = [date.fromisoformat(e["data_end"]) for e in entries
                if e.get("downloaded") and e.get("data_end")]
        data_end = max(ends) if ends else None
        start = data_end + timedelta(days=1) if data_end else job.first_start

        if data_end and (today - data_end).days < job.new_data_days:
            not_due += 1
            continue

        asked = [date.fromisoformat(e["end"]) for e in entries if not is_dead(e)]
        if asked and (today - max(asked)).days < job.recheck_days:
            not_due += 1
            continue

        if start > today:
            not_due += 1
            continue

        due.append((b, start, today))

    return due, waiting, not_due


def range_name(job: Job, batch: int, start: date, end: date) -> str:
    return f"{job.prefix}_{batch:03d}_{start:%Y%m%d}_{end:%Y%m%d}"


def build_task(job: Job, points: list[dict], name: str, start: date, end: date) -> dict:
    return {
        "task_type": "point",
        "task_name": name,
        "params": {
            "dates": [{"startDate": start.strftime("%m-%d-%Y"),
                       "endDate": end.strftime("%m-%d-%Y")}],
            "layers": [{"product": job.product, "layer": layer} for layer in job.layers],
            "coordinates": points,
        },
    }


def submit_due(job: Job, cache: dict, sess: requests.Session, token: str,
               batches: list[list[dict]], today: date) -> int:
    due, waiting, not_due = due_ranges(job, cache, len(batches), today)
    log.info("%s: %d batch(es) due a new range, %d waiting on an outstanding "
             "task, %d not due yet", job.label, len(due), waiting, not_due)
    if not due:
        return 0

    # Adopt tasks created by a run that died between POST and saving the id.
    remote = server_tasks(sess, token)

    for b, start, end in due:
        name = range_name(job, b, start, end)
        if name in remote:
            task_id = remote[name]["task_id"]
            log.info("adopted existing server task %s", name)
        else:
            task_id = post_task(sess, token, name,
                                build_task(job, batches[b], name, start, end),
                                job.limit_setting)
            time.sleep(config.PAUSE_SECONDS)

        cache["tasks"][name] = {
            "task_id": task_id, "batch": b, "points": len(batches[b]),
            "start": start.isoformat(), "end": end.isoformat(),
            "status": "pending", "downloaded": False,
            "submitted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        save_cache(job, cache)
        log.info("queued %s -> %s", name, task_id)

    return len(due)


def summarize(job: Job, cache: dict, n_batches: int, verbose: bool = False) -> None:
    tasks = cache["tasks"]
    done = [e for e in tasks.values() if e.get("downloaded")]
    outstanding = {n: e for n, e in tasks.items() if is_outstanding(e)}
    dead = {n: e for n, e in tasks.items() if is_dead(e)}

    states: dict[str, int] = {}
    for e in outstanding.values():
        states[e.get("status", "?")] = states.get(e.get("status", "?"), 0) + 1
    state_txt = ", ".join(f"{k}={v}" for k, v in sorted(states.items())) or "none"

    log.info("%s: %d task(s): %d downloaded (%s rows), %d outstanding (%s), %d lost",
             job.label, len(tasks), len(done), f"{sum(e.get('rows', 0) for e in done):,}",
             len(outstanding), state_txt, len(dead))

    newest: dict[int, str] = {}
    for e in done:
        if e.get("data_end"):
            newest[e["batch"]] = max(newest.get(e["batch"], ""), e["data_end"])
    if newest:
        log.info("%s: newest observation per batch ranges %s .. %s; %d of %d batch(es) "
                 "have data", job.label, min(newest.values()), max(newest.values()),
                 len(newest), n_batches)
    else:
        log.info("%s: no observations downloaded yet", job.label)

    if dead:
        log.warning("%s: %d task(s) expired or failed before download; their ranges "
                    "are requested again when due: %s", job.label, len(dead),
                    ", ".join(sorted(dead)[:5]) + (" ..." if len(dead) > 5 else ""))

    if verbose:
        for name, e in sorted(tasks.items()):
            state = "downloaded" if e.get("downloaded") else e.get("status", "?")
            log.info("  %-48s %s .. %s  %-10s %s", name, e.get("start"), e.get("end"),
                     state, f"newest {e['data_end']}" if e.get("data_end") else "")


def wait(job: Job, cache: dict, sess: requests.Session, token: str,
         interval: int, max_hours: float) -> None:
    """Poll and collect until nothing is outstanding. Safe to interrupt."""
    deadline = time.time() + max_hours * 3600
    while True:
        refresh(job, cache, sess, token)
        save_cache(job, cache)
        download_finished(job, cache, sess, token)

        left = [n for n, e in cache["tasks"].items() if is_outstanding(e)]
        if not left:
            log.info("%s: nothing outstanding", job.label)
            return
        if time.time() + interval > deadline:
            log.warning("%s: gave up after %.1f hours with %d task(s) outstanding; "
                        "the next --sync collects them", job.label, max_hours, len(left))
            return
        log.info("%s: %d task(s) outstanding; next poll in %d min",
                 job.label, len(left), interval // 60)
        time.sleep(interval)


# ---------------------------------------------------------------- main

def main(job: Job, doc: str | None = None) -> None:
    ap = argparse.ArgumentParser(description=doc or __doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--sync", action="store_true",
                      help="collect finished tasks, then request due ranges (scheduled runs)")
    mode.add_argument("--dry-run", action="store_true",
                      help="show what --sync would request; changes nothing")
    mode.add_argument("--status", action="store_true", help="list every task")
    mode.add_argument("--download", action="store_true", help="collect finished tasks only")
    mode.add_argument("--submit", action="store_true", help="request due ranges only")
    mode.add_argument("--wait", action="store_true",
                      help="poll until every outstanding task is collected")
    ap.add_argument("--interval", type=int, default=300,
                    help="seconds between polls with --wait (default 300)")
    ap.add_argument("--max-hours", type=float, default=24.0,
                    help="give up waiting after this many hours (default 24)")
    args = ap.parse_args()

    today = date.today()
    s = session()
    tok = login(s)

    cache = load_cache(job)
    batches = batches_for(job, cache)
    migrate(job, cache, s, tok)
    check_points(job, cache, batches)

    # Every mode decides from current statuses: an "expired" that is still
    # cached as "done" would otherwise block its batch or fail a download.
    refresh(job, cache, s, tok)

    if args.dry_run:
        due, waiting, not_due = due_ranges(job, cache, len(batches), today)
        ready = sum(1 for e in cache["tasks"].values()
                    if is_outstanding(e) and e.get("status") == "done")
        log.info("%s DRY RUN: %d finished task(s) would be downloaded first", job.label, ready)
        log.info("%s DRY RUN: %d range(s) would be requested, %d batch(es) waiting, "
                 "%d not due", job.label, len(due), waiting, not_due)
        for b, start, end in due[:5]:
            log.info("  %s", range_name(job, b, start, end))
        if len(due) > 5:
            log.info("  ... %d more", len(due) - 5)
        summarize(job, cache, len(batches))
        return

    save_cache(job, cache)
    failed = 0

    if args.status:
        summarize(job, cache, len(batches), verbose=True)
        return

    if args.download or args.sync:
        _, failed = download_finished(job, cache, s, tok)

    if args.submit or args.sync:
        submit_due(job, cache, s, tok, batches, today)

    if args.wait:
        wait(job, cache, s, tok, args.interval, args.max_hours)

    summarize(job, cache, len(batches))
    if failed:
        log.error("%s: %d finished task(s) could not be downloaded; retried next run",
                  job.label, failed)
        raise SystemExit(1)


if __name__ == "__main__":
    main(NDVI)
