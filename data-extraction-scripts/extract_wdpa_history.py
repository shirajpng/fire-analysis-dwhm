"""
Export historical WDPA monthly snapshots from Earth Engine.

This is what makes the SCD Type 2 dimension possible. Protected Planet serves
only the current release, but Earth Engine keeps every monthly version from
July 2017 onward as WCMC/WDPA/YYYYMM/polygons. Diffing consecutive snapshots
gives real effective_from / effective_to ranges.

Setup (once):
    pip install earthengine-api
    earthengine authenticate
    export EE_PROJECT=your-cloud-project-id     # or put it in .env

Usage:
    python extract_wdpa_history.py --list              # which snapshots exist
    python extract_wdpa_history.py --submit            # queue Drive exports
    python extract_wdpa_history.py --submit --quarterly  # fewer, faster
    python extract_wdpa_history.py --status            # check export tasks

Exports land in a Google Drive folder, not on your disk. Download them from
Drive into raw/wdpa_history/ once the tasks finish.

Notes
-----
Attributes only, no geometry. Geometry comes from the Protected Planet
geodatabase; these snapshots exist purely to detect attribute changes over
time, and dropping geometry makes the exports enormously faster and smaller.

Licensing is unchanged: WDPA cannot be redistributed. Keep exports out of git.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date

import config

try:
    import ee
except ImportError:
    sys.exit("earthengine-api is not installed. Run: pip install earthengine-api")

from common import get_logger

log = get_logger("wdpa-hist")

# Earth Engine history begins here.
HISTORY_START = date(2017, 7, 1)

# Attributes that matter for change detection. WDPA_PID is the natural key;
# the rest are the slowly changing attributes. Narrowing the column list keeps
# exports small and diffing cheap.
SCD_FIELDS = [
    "WDPA_PID", "WDPAID", "NAME", "DESIG_ENG", "DESIG_TYPE", "IUCN_CAT",
    "MARINE", "REP_AREA", "GIS_AREA", "STATUS", "STATUS_YR",
    "GOV_TYPE", "OWN_TYPE", "MANG_AUTH", "VERIF", "ISO3", "PARENT_ISO3",
]

DRIVE_FOLDER = "ITCS544_WDPA_history"


def init() -> None:
    project = os.environ.get("EE_PROJECT")
    if not project:
        sys.exit("EE_PROJECT is not set. Add it to .env:\n"
                 "  EE_PROJECT=your-cloud-project-id\n"
                 "Find it at console.cloud.google.com after registering at "
                 "earthengine.google.com")

    log.info("initializing Earth Engine (project=%s)...", project)
    try:
        ee.Initialize(project=project)
    except Exception as exc:
        sys.exit(f"Earth Engine init failed: {exc}\n"
                 "Have you run `earthengine authenticate` and enabled the "
                 "Earth Engine API on this project?")
    log.info("Earth Engine ready")


def candidate_months(quarterly: bool = False) -> list[str]:
    """YYYYMM strings from July 2017 to now."""
    out = []
    y, m = HISTORY_START.year, HISTORY_START.month
    today = date.today()

    while (y, m) <= (today.year, today.month):
        if not quarterly or m in (1, 4, 7, 10):
            out.append(f"{y}{m:02d}")
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return out


def list_available(quarterly: bool, verify: bool = False) -> list[str]:
    """
    Discover available snapshots with ONE call.

    An earlier version probed each of ~111 months individually with getAsset.
    That is ~111 network round trips and looks like a hang. listAssets returns
    the whole WCMC/WDPA collection in a single request.
    """
    log.info("listing WCMC/WDPA assets...")
    try:
        resp = ee.data.listAssets({"parent": "projects/earthengine-public/assets/WCMC/WDPA"})
    except Exception as exc:
        log.error("listAssets failed: %s", exc)
        log.error("Falling back to per-month probing (slow, ~1 request/month)")
        return _probe_months(quarterly)

    assets = resp.get("assets", []) if isinstance(resp, dict) else []
    available = set()
    for asset in assets:
        name = asset.get("id") or asset.get("name") or ""
        tail = name.rstrip("/").split("/")[-1]
        if tail.isdigit() and len(tail) == 6:
            available.add(tail)

    if not available:
        log.warning("listAssets returned nothing recognisable; probing instead")
        return _probe_months(quarterly)

    wanted = [m for m in candidate_months(quarterly) if m in available]

    log.info("%d month entry(s) exist in Earth Engine (%s .. %s)",
             len(available), min(available), max(available))
    log.info("%d match the requested cadence", len(wanted))

    if not verify:
        # No pre-flight check by default. Listing a month as a folder is
        # unreliable -- entries from 201912 onward are not browsable that way
        # even though their polygons tables read fine. Since submit_exports
        # handles failures per month, an unusable snapshot costs one logged
        # error instead of a wasted verification pass.
        return wanted

    log.info("probing polygons asset in each month (--verify)...")
    verified, unusable = [], []
    for m in wanted:
        try:
            ee.data.getAsset(f"WCMC/WDPA/{m}/polygons")
            verified.append(m)
        except Exception:
            unusable.append(m)

    if unusable:
        log.warning("%d month(s) have no readable polygons table: %s",
                    len(unusable), ", ".join(unusable))
    log.info("%d snapshot(s) usable", len(verified))
    return verified


def _probe_months(quarterly: bool) -> list[str]:
    """Slow fallback: one getAsset per month."""
    found = []
    months = candidate_months(quarterly)
    for i, yyyymm in enumerate(months, 1):
        try:
            ee.data.getAsset(f"WCMC/WDPA/{yyyymm}/polygons")
            found.append(yyyymm)
            log.info("[%d/%d] %s available", i, len(months), yyyymm)
        except Exception:
            log.info("[%d/%d] %s missing", i, len(months), yyyymm)
    return found


def already_submitted() -> set[str]:
    """
    Descriptions of wdpa_ export tasks already known to Earth Engine.

    Lets a re-run resume rather than duplicating work. Tasks that previously
    FAILED are not included, so they get retried.
    """
    try:
        ops = ee.data.listOperations()
    except Exception as exc:
        log.warning("could not list existing tasks (%s); may re-submit duplicates", exc)
        return set()

    seen = set()
    for op in ops:
        meta = op.get("metadata", {})
        desc = meta.get("description", "")
        state = str(meta.get("state", "")).upper()
        if desc.startswith("wdpa_") and state not in ("FAILED", "CANCELLED"):
            seen.add(desc)
    return seen


def submit_exports(months: list[str]) -> None:
    """
    Queue one Drive export per snapshot, filtered to the study country.

    Filtering on both ISO3 and PARENT_ISO3 catches areas recorded against a
    parent territory, which a plain ISO3 filter would miss.

    Failures are per-month, not fatal: one missing asset must not abandon the
    other 90-odd exports.
    """
    existing = already_submitted()
    if existing:
        log.info("%d export(s) already queued or done; those are skipped", len(existing))

    queued = skipped = failed = 0

    for yyyymm in months:
        description = f"wdpa_{config.ISO3}_{yyyymm}"

        if description in existing:
            skipped += 1
            continue

        try:
            fc = ee.FeatureCollection(f"WCMC/WDPA/{yyyymm}/polygons")
            fc = fc.filter(ee.Filter.Or(
                ee.Filter.eq("ISO3", config.ISO3),
                ee.Filter.eq("PARENT_ISO3", config.ISO3),
            ))

            task = ee.batch.Export.table.toDrive(
                collection=fc,
                description=description,
                folder=DRIVE_FOLDER,
                fileNamePrefix=description,
                fileFormat="CSV",
                selectors=SCD_FIELDS,   # attributes only, geometry dropped
            )
            task.start()
            queued += 1
            log.info("queued %s (task %s)", yyyymm, task.id)

        except Exception as exc:
            failed += 1
            log.error("SKIPPED %s -- %s", yyyymm, str(exc)[:160])

    log.info("=" * 60)
    log.info("queued=%d  skipped=%d  failed=%d  -> Drive folder '%s'",
             queued, skipped, failed, DRIVE_FOLDER)
    if failed:
        log.warning("%d month(s) failed. Re-running --submit retries only those.", failed)
    log.info("Check progress at code.earthengine.google.com (Tasks tab) or with --status")


def show_status() -> None:
    tasks = ee.data.listOperations()
    if not tasks:
        log.info("no tasks found")
        return

    counts: dict[str, int] = {}
    for t in tasks:
        meta = t.get("metadata", {})
        desc = meta.get("description", "?")
        state = meta.get("state", "UNKNOWN")
        if not desc.startswith("wdpa_"):
            continue
        counts[state] = counts.get(state, 0) + 1
        log.info("%-28s %s", desc, state)

    if counts:
        log.info("SUMMARY: %s", "  ".join(f"{k}={v}" for k, v in sorted(counts.items())))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--list", action="store_true", help="probe which snapshots exist")
    ap.add_argument("--submit", action="store_true", help="queue Drive exports")
    ap.add_argument("--status", action="store_true", help="check export tasks")
    ap.add_argument("--quarterly", action="store_true",
                    help="Jan/Apr/Jul/Oct only (~37 instead of ~110 snapshots)")
    ap.add_argument("--verify", action="store_true",
                    help="probe each polygons asset before submitting (slow)")
    args = ap.parse_args()

    if not any([args.list, args.submit, args.status]):
        ap.error("pick one of --list, --submit, --status")

    init()

    if args.list:
        list_available(args.quarterly, verify=args.verify)
    if args.submit:
        submit_exports(list_available(args.quarterly, verify=args.verify))
    if args.status:
        show_status()