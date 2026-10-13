"""
Central configuration for the ITCS544 extraction layer.

Everything region-specific lives here so that switching study area is a
one-file change. Nothing below this file should hardcode a bounding box,
an ISO code or a date.

Importing this module also loads .env into the environment, so every script
picks up credentials whether it is run through run_all.sh or directly.
"""

import os
from datetime import date
from pathlib import Path

# ---------------------------------------------------------------- .env

def _load_dotenv() -> None:
    """
    Minimal .env loader. No dependency on python-dotenv.

    Searches the directory of this file and its parents, so it works no matter
    what the current working directory is. Values already present in the
    environment win, which means an explicit `export` still overrides the file.
    """
    here = Path(__file__).resolve().parent

    for folder in (here, *here.parents):
        candidate = folder / ".env"
        if not candidate.is_file():
            continue

        for raw in candidate.read_text().splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")

            # Strip inline comments, but only when clearly separated, so a
            # value containing '#' is not mangled.
            if " #" in value:
                value = value.split(" #", 1)[0].strip()

            if key and key not in os.environ:
                os.environ[key] = value
        return


_load_dotenv()

# ---------------------------------------------------------------- study area

REGION_NAME = "Thailand"
ISO3 = "THA"

# west,south,east,north. Deliberately generous: clip to admin boundaries in
# the staging layer, not at extraction time. Over-extracting is cheap and
# re-extracting is not.
BBOX = "97,5,106,21"

# GeoJSON-ready version of the same box (lon, lat order).
BBOX_POLYGON = [
    [97.0, 5.0],
    [106.0, 5.0],
    [106.0, 21.0],
    [97.0, 21.0],
    [97.0, 5.0],
]

# ---------------------------------------------------------------- time range

# Earliest date we care about. MODIS goes back to Nov 2000; starting at the
# VIIRS era gives a denser, more consistent record. Set to 2000-11-01 if you
# want the full MODIS history for the scalability experiments.
START_DATE = date(2012, 1, 20)
END_DATE = date.today()

# ---------------------------------------------------------------- FIRMS

FIRMS_BASE = "https://firms.modaps.eosdis.nasa.gov/api"

# Standard Processing (science quality) sources only, with first availability.
# NOAA-21 is excluded on purpose: only VIIRS_NOAA21_NRT exists, there is no SP
# product, so it cannot join an archive-based build. Say this in the report.
FIRMS_SENSORS_SP = {
    "MODIS_SP": date(2000, 11, 1),
    "VIIRS_SNPP_SP": date(2012, 1, 20),
    "VIIRS_NOAA20_SP": date(2018, 4, 1),
}

# NRT sources. Only needed if you want to demonstrate the
# "NRT superseded by science quality" lineage story. These cover roughly the
# last 2 months only.
FIRMS_SENSORS_NRT = {
    "MODIS_NRT": None,
    "VIIRS_SNPP_NRT": None,
    "VIIRS_NOAA20_NRT": None,
    "VIIRS_NOAA21_NRT": None,
}

# Days per request. The API enforces [1..5] and returns
#   HTTP 400 "Invalid day range. Expects [1..5]."
# outside it. Note the public docs still describe a maximum of 10; the endpoint
# is stricter than its documentation, so trust this value, not the docs.
FIRMS_DAY_RANGE = 5

# ---------------------------------------------------------------- WDPA

# Stable redirect to the current monthly global release (file geodatabase,
# ~1.1 GB). Do NOT hardcode a month string: it rolls over every release and
# your DAG will 404 a few weeks later.
WDPA_CURRENT_GLOBAL = "http://wcmc.io/wdpa_current_release"

# Country-scoped downloads come from the CloudFront distribution. The month
# segment is discovered at runtime, never hardcoded.
WDPA_CDN = "https://d1gam3xoknrgr2.cloudfront.net/current"

# ---------------------------------------------------------------- AppEEARS

APPEEARS_BASE = "https://appeears.earthdatacloud.nasa.gov/api"

NDVI_PRODUCT = "MOD13Q1.061"
NDVI_LAYERS = [
    "_250m_16_days_NDVI",
    "_250m_16_days_pixel_reliability",
]

# Point-sample grid spacing in degrees. 0.1 deg over the Thailand box is
# roughly 8,000 points. Each point yields ~23 observations/year, so
# 8,000 x 23 x 14 years is comfortably into the millions of rows.
#
# AppEEARS enforces a per-request limit on point count. Chunk accordingly and
# VERIFY the current limit in the API docs before a large run.
NDVI_GRID_STEP = 0.1
NDVI_POINTS_PER_TASK = 500

# ---------------------------------------------------------------- boundaries

# GADM 4.1 GeoPackage, all admin levels in one file.
GADM_URL = f"https://geodata.ucdavis.edu/gadm/gadm4.1/gpkg/gadm41_{ISO3}.gpkg"

# ---------------------------------------------------------------- filesystem

RAW = Path("./raw")

RAW_FIRMS = RAW / "firms"
RAW_WDPA = RAW / "wdpa"
RAW_NDVI = RAW / "ndvi"
RAW_BOUNDARIES = RAW / "boundaries"

MANIFEST = RAW / "_manifest.jsonl"

# ---------------------------------------------------------------- politeness

PAUSE_SECONDS = 1.0
MAX_RETRIES = 4