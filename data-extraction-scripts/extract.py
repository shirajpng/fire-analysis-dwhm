"""
Single entry point for every extractor. Each source runs independently.

    python extract.py list              # what is available and what has landed
    python extract.py firms             # one source at a time
    python extract.py wdpa
    python extract.py ndvi
    python extract.py landcover
    python extract.py boundaries
    python extract.py inventory

Arguments after the source name are passed straight through, so the
per-script flags all still work:

    python extract.py firms --dry-run
    python extract.py firms --sensor MODIS_SP --limit 3
    python extract.py ndvi --status
    python extract.py wdpa --country THA

This is a convenience wrapper. Calling the scripts directly is equivalent.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import config

HERE = Path(__file__).resolve().parent

SOURCES = {
    "boundaries": ("extract_boundaries.py", "GADM admin levels 0-3", config.RAW_BOUNDARIES),
    "wdpa": ("extract_wdpa.py", "Protected areas, current release", config.RAW_WDPA),
    "wdpa-history": ("extract_wdpa_history.py",
                     "WDPA monthly snapshots via Earth Engine (SCD2)", None),
    "ndvi": ("extract_appeears.py", "MOD13Q1 NDVI via AppEEARS (async)", config.RAW_NDVI),
    "landcover": ("extract_landcover.py", "MCD12Q1 land cover via AppEEARS (async, SCD2)",
                  config.RAW_LANDCOVER),
    "firms": ("extract_firms.py", "Active fire detections (the big one)", config.RAW_FIRMS / "sp"),
    "firms-nrt": ("extract_firms_nrt.py", "NRT fire detections (run regularly)",
                  config.RAW_FIRMS / "nrt"),
    "inventory": ("inventory.py", "Row counts and schema report", None),
}


def show_list() -> None:
    print(f"\nSources for {config.REGION_NAME} ({config.ISO3})")
    print(f"Raw zone: {config.RAW.resolve()}\n")

    for name, (script, desc, folder) in SOURCES.items():
        if folder is None:
            state = ""
        elif not folder.exists():
            state = "  [not yet run]"
        else:
            n = len([p for p in folder.rglob("*") if p.is_file()])
            state = f"  [{n} file(s)]" if n else "  [EMPTY]"
        print(f"  {name:<12} {desc}{state}")

    print("\n  python extract.py <source> [flags]")
    print("  python extract.py firms --dry-run     <- start here when debugging\n")


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help", "list"):
        show_list()
        return 0

    source = sys.argv[1]
    if source not in SOURCES:
        print(f"Unknown source: {source}")
        print(f"Expected one of: {', '.join(SOURCES)}")
        return 1

    script = HERE / SOURCES[source][0]
    cmd = [sys.executable, str(script), *sys.argv[2:]]

    print(f"--> {' '.join(cmd[1:])}\n")
    return subprocess.call(cmd, cwd=HERE)


if __name__ == "__main__":
    sys.exit(main())