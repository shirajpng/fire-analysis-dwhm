"""
Download the current WDPA public release from Protected Planet.

No API token required. The month segment in the filename is discovered at
runtime by following a stable redirect, never hardcoded, because it rolls over
on every monthly release.

Usage:
    python extract_wdpa.py                # global geodatabase (~1.1 GB)
    python extract_wdpa.py --country THA  # country zip, much smaller
    python extract_wdpa.py --both         # country zip + global gdb

IMPORTANT -- licensing
    WDPA materials may not be redistributed without written permission from
    UNEP-WCMC. These files must stay out of the git repository. The repo ships
    this script and the citation, not the data. Check .gitignore.

IMPORTANT -- geometry
    The *_csv download is the attribute table only, with no polygons. The
    point-in-polygon join needs the geodatabase. Take both: geometry from the
    gdb, cheap attribute diffing from the csv.

IMPORTANT -- history
    protectedplanet.net serves only the CURRENT release. Month-to-month history
    for the SCD Type 2 dimension is not available here. Either pull monthly
    assets from Earth Engine (WCMC/WDPA/YYYYMM, from July 2017), or fall back to
    building weaker Type 2 validity ranges from the STATUS_YR field.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import config
from common import download, get_logger, record_manifest, session

log = get_logger("wdpa")

CITATION = (
    "UNEP-WCMC and IUCN ({year}), Protected Planet: The World Database on "
    "Protected Areas (WDPA) [Online], {month} {year}, Cambridge, UK: "
    "UNEP-WCMC and IUCN. Available at: www.protectedplanet.net."
)


def resolve_current_release(sess) -> tuple[str, str]:
    """
    Follow the stable redirect to discover the current release filename.

    Returns (resolved_url, filename). The filename embeds the release month,
    e.g. WDPA_WDOECM_Aug2026_Public.zip -- that string IS your version
    identifier and belongs in the ETL audit table.
    """
    resp = sess.get(config.WDPA_CURRENT_GLOBAL, stream=True, timeout=120,
                    allow_redirects=True)
    resp.raise_for_status()
    resp.close()

    filename = resp.url.rstrip("/").split("/")[-1].split("?")[0]
    log.info("current release resolves to: %s", filename)
    return resp.url, filename


def parse_release_month(filename: str) -> tuple[str, str]:
    """Pull ('Aug', '2026') out of a WDPA release filename."""
    m = re.search(r"([A-Z][a-z]{2})(\d{4})", filename)
    if not m:
        return ("unknown", "unknown")
    return m.group(1), m.group(2)


def country_urls(month: str, year: str, iso3: str) -> dict[str, str]:
    """
    Country-scoped downloads on the CloudFront distribution.

    Current releases use the WDPA_WDOECM_ prefix (protected areas plus OECMs in
    one package). The older bare WDPA_ prefix is no longer served and has been
    removed: it only produced 404s.
    """
    stem = f"{month}{year}"
    return {
        "gdb": f"{config.WDPA_CDN}/WDPA_WDOECM_{stem}_Public_{iso3}.zip",
        "csv": f"{config.WDPA_CDN}/WDPA_WDOECM_{stem}_Public_{iso3}_csv.zip",
    }


def try_download(url: str, target: Path, sess) -> Path | None:
    try:
        return download(url, target, sess=sess)
    except Exception as exc:
        log.info("not available: %s (%s)", url.split("/")[-1], type(exc).__name__)
        return None


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=config.RAW_WDPA)
    ap.add_argument("--country", default=None,
                    help=f"ISO3 code, e.g. {config.ISO3}. Omit for global.")
    ap.add_argument("--both", action="store_true",
                    help="download the country zip AND the global geodatabase")
    args = ap.parse_args()

    sess = session()
    args.out.mkdir(parents=True, exist_ok=True)

    url, filename = resolve_current_release(sess)
    month, year = parse_release_month(filename)

    citation = CITATION.format(month=_full_month(month), year=year)
    (args.out / f"CITATION_{month}{year}.txt").write_text(citation + "\n")
    log.info("citation written for %s%s -- required in the final report", month, year)

    iso3 = args.country or (config.ISO3 if not args.both else config.ISO3)

    got_any = False
    if args.country or args.both:
        for label, curl in country_urls(month, year, iso3).items():
            target = args.out / curl.split("/")[-1]
            if try_download(curl, target, sess):
                got_any = True

        if not got_any:
            log.warning(
                "No country file matched the expected naming pattern. "
                "Download manually from protectedplanet.net and drop the zip "
                "into %s -- everything downstream reads from that directory.",
                args.out,
            )

    if args.both or not args.country:
        log.info("fetching global geodatabase (~1.1 GB, this takes a while)")
        target = args.out / filename
        download(url, target, sess=sess)
        record_manifest(target, source_url=config.WDPA_CURRENT_GLOBAL,
                        extra={"release": f"{month}{year}", "scope": "global"})


def _full_month(abbr: str) -> str:
    names = {"Jan": "January", "Feb": "February", "Mar": "March", "Apr": "April",
             "May": "May", "Jun": "June", "Jul": "July", "Aug": "August",
             "Sep": "September", "Oct": "October", "Nov": "November",
             "Dec": "December"}
    return names.get(abbr, abbr)


if __name__ == "__main__":
    main()
