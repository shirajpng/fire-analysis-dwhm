"""
Extract MODIS MCD12Q1 annual land cover from AppEEARS as CSV.

This feeds the land cover SCD Type 2 dimension. MCD12Q1 is one classification
per 500 m pixel per year, so a point whose class changes between years (forest
to cropland, say) becomes a new dimension version with real effective dates.

It runs separately from the NDVI extraction on purpose: its own task names, its
own task cache and its own output folder, so neither job can resume, skip or
overwrite the other. It does reuse the NDVI point grid, which means point ids
(p000000, ...) are identical across both outputs and staging can join them
directly.

Usage:
    python extract_landcover.py --dry-run   # show the plan, submit nothing
    python extract_landcover.py --submit    # create and queue the tasks
    python extract_landcover.py --status    # check progress
    python extract_landcover.py --download  # fetch whatever has finished
    python extract_landcover.py --wait      # poll + download until all done

Credentials are the same EARTHDATA_USER / EARTHDATA_PASS the NDVI script uses.

Output is small next to NDVI: one row per point per year, so ~14,600 points
x ~13 years is under 200k rows. It is a dimension source, not a volume source.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date
from pathlib import Path

import requests

import config
from common import get_logger, record_manifest, session
from extract_appeears import auth_header, build_point_grid, chunks, login

log = get_logger("landcover")

TASK_CACHE = config.RAW_LANDCOVER / "_tasks.json"


# ---------------------------------------------------------------- tasks

def task_name(n: int) -> str:
    return f"itcs544_landcover_{config.ISO3}_{n:03d}"


def build_task(points: list[dict], name: str) -> dict:
    # MCD12Q1 composites are stamped 1 January. Starting at START_DATE itself
    # (2012-01-20) would silently drop the first year, so snap to 1 January.
    start = date(config.START_DATE.year, 1, 1)

    return {
        "task_type": "point",
        "task_name": name,
        "params": {
            "dates": [{
                "startDate": start.strftime("%m-%d-%Y"),
                "endDate": config.END_DATE.strftime("%m-%d-%Y"),
            }],
            "layers": [
                {"product": config.LANDCOVER_PRODUCT, "layer": layer}
                for layer in config.LANDCOVER_LAYERS
            ],
            "coordinates": points,
        },
    }


def plan() -> list[tuple[str, list[dict]]]:
    points = build_point_grid()
    batches = chunks(points, config.LANDCOVER_POINTS_PER_TASK)
    return [(task_name(n), batch) for n, batch in enumerate(batches)]


def dry_run() -> None:
    tasks = plan()
    cache = load_cache()
    pending = [name for name, _ in tasks if name not in cache]

    log.info("product %s, layers %s", config.LANDCOVER_PRODUCT,
             ", ".join(config.LANDCOVER_LAYERS))
    log.info("%d task(s) total, %d already submitted, %d to submit",
             len(tasks), len(tasks) - len(pending), len(pending))

    name, batch = tasks[0]
    payload = build_task(batch, name)
    payload["params"]["coordinates"] = batch[:2] + [f"... {len(batch) - 2} more"]
    print(json.dumps(payload, indent=2))


def server_tasks(sess: requests.Session, token: str) -> dict[str, dict]:
    """
    Every task on the account that is not failed, keyed by task_name.

    If two live tasks share a name, the most recently created one wins.
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


def post_task(sess: requests.Session, token: str, name: str,
              batch: list[dict]) -> str:
    """
    Submit one task and return its id.

    A 5xx or a dropped connection does NOT mean the task was not created:
    AppEEARS can accept the task and then time out at the gateway. Blindly
    retrying would queue a duplicate. So after any ambiguous failure, look the
    task up by name first and only resubmit if it is genuinely absent.
    """
    for attempt in range(config.MAX_RETRIES):
        try:
            resp = sess.post(f"{config.APPEEARS_BASE}/task",
                             json=build_task(batch, name),
                             headers=auth_header(token), timeout=180)
            if resp.status_code < 400:
                return resp.json()["task_id"]
            if resp.status_code < 500 and resp.status_code != 429:
                log.error("submission rejected (%s): %s",
                          resp.status_code, resp.text[:300])
                log.error("If this mentions coordinate limits, lower "
                          "LANDCOVER_POINTS_PER_TASK in config.py and re-run.")
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
    tasks = plan()
    cache = load_cache()
    log.info("submitting %d task(s)", len(tasks))

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

        task_id = post_task(sess, token, name, batch)
        cache[name] = {"task_id": task_id, "points": len(batch)}
        save_cache(cache)
        log.info("queued %s -> %s", name, task_id)
        time.sleep(config.PAUSE_SECONDS)

    log.info("all tasks queued. Re-run with --status to check.")


def status(sess: requests.Session, token: str, verbose: bool = True) -> dict:
    """Poll every cached task. Returns {task_name: state}."""
    cache = load_cache()
    if not cache:
        sys.exit("No land cover tasks cached. Run with --submit first.")

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
            log.info("%-32s %s", name, states[name])

    counts: dict[str, int] = {}
    for state in states.values():
        counts[state] = counts.get(state, 0) + 1
    log.info("SUMMARY: %s  (%d tasks)",
             "  ".join(f"{k}={v}" for k, v in sorted(counts.items())), len(states))

    failed = [n for n, s in states.items() if is_failed(s)]
    if failed:
        log.error("%d task(s) failed and will never download: %s",
                  len(failed), ", ".join(failed))
        log.error("Remove them from %s and re-run --submit to retry.", TASK_CACHE)

    return states


def is_failed(state: str) -> bool:
    return "error" in state.lower() or "failed" in state.lower()


def wait_and_download(sess: requests.Session, token: str,
                      interval: int = 300, max_hours: float = 24.0) -> None:
    deadline = time.time() + max_hours * 3600
    round_n = 0

    while time.time() < deadline:
        round_n += 1
        log.info("--- poll %d ---", round_n)
        states = status(sess, token, verbose=False)

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
    config.RAW_LANDCOVER.mkdir(parents=True, exist_ok=True)

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

            target = config.RAW_LANDCOVER / f"{name}__{Path(fname).name}"
            if target.exists():
                continue

            url = f"{config.APPEEARS_BASE}/bundle/{task_id}/{item['file_id']}"
            r = sess.get(url, headers=auth_header(token), stream=True, timeout=600)
            r.raise_for_status()

            # Write to .part and rename, so an interrupted download is retried
            # next run instead of being skipped as if complete.
            part = target.with_suffix(target.suffix + ".part")
            with open(part, "wb") as fh:
                for chunk in r.iter_content(1 << 20):
                    fh.write(chunk)
            part.rename(target)

            with open(target) as fh:
                rows = max(0, sum(1 for _ in fh) - 1)

            record_manifest(target, source_url=url, rows=rows,
                            extra={"task": name, "task_id": task_id,
                                   "product": config.LANDCOVER_PRODUCT})
            log.info("downloaded %s (%s rows)", target.name, f"{rows:,}")


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
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true",
                    help="print the task plan and first payload, submit nothing")
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

    if not any([args.dry_run, args.submit, args.status, args.download, args.wait]):
        ap.error("pick one of --dry-run, --submit, --status, --download, --wait")

    if args.dry_run:
        dry_run()
        sys.exit(0)

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
