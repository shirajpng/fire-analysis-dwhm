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

# NRT sources, extracted by extract_firms_nrt.py. Date bounds always come from
# the API's availability endpoint, never from here. As of Sep 2026:
#   MODIS / SNPP / NOAA-20 NRT start the day after their SP archive ends, and
#   roll forward as SP catches up. NRT that is not captured before then is gone.
#   NOAA-21 NRT goes back to 2024-01-17 and is the only NOAA-21 product.
FIRMS_SENSORS_NRT = {
    "MODIS_NRT": None,
    "VIIRS_SNPP_NRT": None,
    "VIIRS_NOAA20_NRT": None,
    "VIIRS_NOAA21_NRT": None,
}

# NRT windows ending within this many days of the newest available date are
# still filling (late granules), so they are refetched on every run instead of
# being frozen.
FIRMS_NRT_SETTLE_DAYS = 2

# Fixed anchor for NRT window starts, so the same dates always land in the same
# file no matter which day the extractor runs.
FIRMS_NRT_EPOCH = date(2000, 1, 1)

# Days per request. The API enforces [1..5] and returns
#   HTTP 400 "Invalid day range. Expects [1..5]."
# outside it. Note the public docs still describe a maximum of 10; the endpoint
# is stricter than its documentation, so trust this value, not the docs.
FIRMS_DAY_RANGE = 5

# ------------------------------------------------- FIRMS country archive

# Pre-clipped country-year extracts from the FIRMS archive download page.
# Same science-quality detections as the /api/area pull, but clipped by NASA
# to the country border instead of our bounding box. That makes them the
# reference set the spatial clip is validated against, not an ingest source:
# every row here is already in raw/firms/sp under a bbox that also covers
# parts of Myanmar, Laos, Cambodia and Malaysia.
#
# Open download, no MAP_KEY. A missing file returns HTTP 200 with a JSON
# error body, so availability has to be probed by reading the body.
FIRMS_COUNTRY_BASE = "https://firms.modaps.eosdis.nasa.gov/data/country"

# Country token in the archive filename. FIRMS does not always spell a country
# the way REGION_NAME does ("Viet Nam", "Lao People's Democratic Republic"),
# so this is overridable -- but for Thailand the two agree.
FIRMS_COUNTRY_NAME = REGION_NAME

# URL slug -> the sensor name this repo uses everywhere else, so the
# validation join against raw/firms/sp is a straight equality.
#
# NOAA-21 is absent on purpose and not an oversight: there is no country
# archive for it, for the same reason there is no VIIRS_NOAA21_SP.
FIRMS_COUNTRY_SENSORS = {
    "modis": "MODIS_SP",
    "viirs-snpp": "VIIRS_SNPP_SP",
    "viirs-jpss1": "VIIRS_NOAA20_SP",
}

# Earliest year to probe. The upper bound is discovered, never hardcoded: the
# archive lags roughly a year behind real time (2024 is the newest as of
# Sep 2026) and a hardcoded end year silently stops collecting when it rolls.
FIRMS_COUNTRY_FIRST_YEAR = 2000

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

# MODIS MCD12Q1 annual land cover, 500 m. Feeds the land cover SCD Type 2
# dimension: one value per point per year, so a class change between years is
# a new dimension version.
#
#   LC_Type1  IGBP 17-class scheme, the standard one to report
#   LC_Type5  Plant Functional Types, splits cereal from broadleaf cropland
#   LW        land/water mask, lets staging drop sea points without a join
#   QC        product quality flags
#
# Sampled on the same grid as NDVI (NDVI_GRID_STEP), so point ids line up and
# staging can join the two on id instead of a spatial match.
LANDCOVER_PRODUCT = "MCD12Q1.061"
LANDCOVER_LAYERS = ["LC_Type1", "LC_Type5", "LW", "QC"]
LANDCOVER_POINTS_PER_TASK = 500

# ---------------------------------------------------------------- boundaries

# GADM 4.1 GeoPackage, all admin levels in one file.
GADM_URL = f"https://geodata.ucdavis.edu/gadm/gadm4.1/gpkg/gadm41_{ISO3}.gpkg"

# ---------------------------------------------------------------- filesystem

RAW = Path("./raw")

RAW_FIRMS = RAW / "firms"
RAW_FIRMS_COUNTRY = RAW / "firms_country"
RAW_WDPA = RAW / "wdpa"
RAW_NDVI = RAW / "ndvi"
RAW_LANDCOVER = RAW / "landcover"
RAW_BOUNDARIES = RAW / "boundaries"

MANIFEST = RAW / "_manifest.jsonl"

# ---------------------------------------------------------------- politeness

PAUSE_SECONDS = 1.0
MAX_RETRIES = 4