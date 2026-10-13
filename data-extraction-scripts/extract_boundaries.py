"""
Download administrative boundaries for the study region from GADM.

Feeds two things:
  1. The location dimension hierarchy: point -> district -> province -> country,
     which is what makes roll-up and drill-down work in the OLAP layer.
  2. Spatial clipping in staging, so the generous extraction bounding box gets
     trimmed to the actual country.

GADM 4.1 ships one GeoPackage per country containing every admin level, which
is more convenient than juggling separate shapefiles.

Usage:
    python extract_boundaries.py
    python extract_boundaries.py --inspect   # list layers after download

Licensing: GADM is free for academic and other non-commercial use;
redistribution is not permitted. Same treatment as WDPA -- keep it out of the
repository and ship this script instead.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import config
from common import download, get_logger, session

log = get_logger("boundaries")


def inspect(path: Path) -> None:
    """List layers and feature counts. Needs geopandas/pyogrio installed."""
    try:
        import pyogrio
    except ImportError:
        log.warning("pyogrio not installed; skipping inspection. "
                    "pip install pyogrio geopandas")
        return

    try:
        layers = pyogrio.list_layers(path)
    except Exception as exc:
        log.error("could not read %s: %s", path.name, exc)
        return

    log.info("layers in %s:", path.name)
    for name, geom_type in layers:
        try:
            info = pyogrio.read_info(path, layer=name)
            log.info("  %-24s %-14s %6d features", name, geom_type,
                     info.get("features", -1))
        except Exception:
            log.info("  %-24s %s", name, geom_type)

    log.info("ADM_ADM_0 is country, _1 province, _2 district. "
             "Levels 1 and 2 are what the location dimension needs.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=config.RAW_BOUNDARIES)
    ap.add_argument("--inspect", action="store_true")
    args = ap.parse_args()

    target = args.out / f"gadm41_{config.ISO3}.gpkg"
    sess = session()

    try:
        download(config.GADM_URL, target, sess=sess)
    except Exception as exc:
        log.error("download failed: %s", exc)
        log.error("Fetch manually from https://gadm.org/download_country.html "
                  "(choose Geopackage) and place it at %s", target)
        raise SystemExit(1)

    if args.inspect:
        inspect(target)
