"""
Extractor for the FIRMS country-year archive -- the spatial-clip reference set.

These are the same science-quality detections as extract_firms.py pulls, but
clipped by NASA to the country border rather than to config.BBOX. Our bounding
box is deliberately generous and takes in parts of Myanmar, Laos, Cambodia and
Malaysia, so roughly three quarters of the raw fact rows are discarded when
staging clips to the admin boundary. These files are how that clip is
*verified* instead of asserted.

They are therefore NOT an ingest source. Every row here is already in
raw/firms/sp. inventory.py reports them separately and leaves them out of the
record-count total, because counting them would inflate the headline number
with rows the warehouse already holds.

One file per sensor per year:

    raw/firms_country/sensor=MODIS_SP/year=2024/modis_2024_Thailand.csv

Files are stored byte-for-byte as served, with no added provenance columns, so
the SHA-256 in the manifest is the checksum of what NASA actually sent. That is
what makes them usable as a baseline; extract_firms.py annotates its output
because that output is ingest input, and this is not.

No credentials needed. This is an open download, unlike the /api endpoints.

Usage:
    python extract_firms_country.py              # fetch everything available
    python extract_firms_country.py --list       # probe availability, download nothing
    python extract_firms_country.py --years 2023 2024
    python extract_firms_country.py --sensors modis
"""

from __future__ import annotations

import argparse
import time
from datetime import date
from pathlib import Path

import requests

import config
from common import get_logger, get_with_retry, record_manifest, session

log = get_logger("firms-country")

# A real file starts with the CSV header. A missing one comes back as HTTP 200
# carrying a JSON error body, so the status code tells you nothing.
CSV_SENTINEL = "latitude"


def url_for(slug: str, year: int) -> str:
    name = f"{slug}_{year}_{config.FIRMS_COUNTRY_NAME}.csv"
    return f"{config.FIRMS_COUNTRY_BASE}/{slug}/{year}/{name}"


def target_for(slug: str, year: int, out_dir: Path) -> Path:
    sensor = config.FIRMS_COUNTRY_SENSORS[slug]
    name = f"{slug}_{year}_{config.FIRMS_COUNTRY_NAME}.csv"
    return out_dir / f"sensor={sensor}" / f"year={year}" / name


def exists_remotely(slug: str, year: int, sess: requests.Session) -> bool:
    """
    Cheap availability probe: ask for the first 80 bytes and look at them.

    A HEAD request is useless here -- the server answers 200 with a zero-length
    body for both present and absent files -- and a full GET to test a 25-year
    range would pull hundreds of megabytes just to discover what exists.
    """
    try:
        resp = sess.get(url_for(slug, year), headers={"Range": "bytes=0-80"},
                        timeout=30)
    except Exception as exc:
        log.warning("probe failed for %s %d: %s", slug, year, exc)
        return False

    return resp.text.lstrip().startswith(CSV_SENTINEL)


def count_rows(path: Path) -> int:
    with open(path, "rb") as fh:
        n = sum(1 for _ in fh)
    return max(0, n - 1)


def fetch(slug: str, year: int, out_dir: Path, sess: requests.Session) -> int | None:
    """
    Download one country-year file. Returns row count, or None if not fetched.

    Writes to .part and renames on success, and refuses to keep a file whose
    first line is not the CSV header -- otherwise a JSON error body served with
    HTTP 200 lands on disk as a plausible-looking .csv and is only noticed
    much later, in staging.
    """
    target = target_for(slug, year, out_dir)

    if target.exists() and target.stat().st_size > 0:
        log.info("skip (exists): %s", target.name)
        return None

    url = url_for(slug, year)
    target.parent.mkdir(parents=True, exist_ok=True)
    part = target.with_suffix(".csv.part")

    try:
        resp = get_with_retry(url, sess, stream=True, timeout=300)
    except Exception as exc:
        log.error("%s %d -> %s", slug, year, exc)
        return None

    written = 0
    first = b""
    with open(part, "wb") as fh:
        for chunk in resp.iter_content(1 << 20):
            if not first:
                first = chunk[:80]
            fh.write(chunk)
            written += len(chunk)

    if not first.lstrip().startswith(CSV_SENTINEL.encode()):
        part.unlink(missing_ok=True)
        log.error("%s %d -> not a CSV (%s); discarded",
                  slug, year, first[:60].decode("utf-8", "replace"))
        return None

    part.rename(target)
    rows = count_rows(target)

    record_manifest(
        target,
        source_url=url,
        bytes_written=written,
        rows=rows,
        extra={
            "sensor": config.FIRMS_COUNTRY_SENSORS[slug],
            "year": year,
            "role": "spatial-clip reference, not ingest input",
        },
    )
    log.info("%-18s %d -> %8d rows (%.1f MiB)",
             config.FIRMS_COUNTRY_SENSORS[slug], year, rows, written / (1 << 20))
    return rows


def run(slugs: list[str], years: list[int], out_dir: Path,
        list_only: bool = False) -> int:
    sess = session()
    total = 0

    for slug in slugs:
        sensor = config.FIRMS_COUNTRY_SENSORS[slug]
        found: list[int] = []

        for year in years:
            target = target_for(slug, year, out_dir)

            # Already on disk: no probe, no request. After a full run this
            # leaves only the not-yet-published years to check, so re-running
            # costs a handful of range requests rather than a full sweep.
            if target.exists() and target.stat().st_size > 0 and not list_only:
                found.append(year)
                continue

            if not exists_remotely(slug, year, sess):
                continue

            found.append(year)
            if not list_only:
                rows = fetch(slug, year, out_dir, sess)
                if rows:
                    total += rows
            time.sleep(config.PAUSE_SECONDS)

        if found:
            log.info("== %-18s %d year(s): %d..%d ==",
                     sensor, len(found), min(found), max(found))
        else:
            log.warning("== %-18s nothing available ==", sensor)

    if list_only:
        return 0

    log.info("FIRMS country archive: %d new rows", total)
    return total


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, default=config.RAW_FIRMS_COUNTRY)
    ap.add_argument("--sensors", nargs="+", choices=sorted(config.FIRMS_COUNTRY_SENSORS),
                    default=sorted(config.FIRMS_COUNTRY_SENSORS),
                    help="URL slugs to fetch (default: all)")
    ap.add_argument("--years", nargs="+", type=int,
                    help="explicit years (default: %d..current, availability probed)"
                         % config.FIRMS_COUNTRY_FIRST_YEAR)
    ap.add_argument("--list", action="store_true",
                    help="probe and print what exists, download nothing")
    args = ap.parse_args()

    requested = args.years or list(
        range(config.FIRMS_COUNTRY_FIRST_YEAR, date.today().year + 1)
    )

    log.info("country token: %r  (override FIRMS_COUNTRY_NAME in config.py "
             "if FIRMS spells it differently)", config.FIRMS_COUNTRY_NAME)

    run(args.sensors, requested, args.out, list_only=args.list)
