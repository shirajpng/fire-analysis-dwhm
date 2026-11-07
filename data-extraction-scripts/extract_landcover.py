"""
Extract MODIS MCD12Q1 annual land cover from AppEEARS as CSV, incrementally.

This feeds the land cover SCD Type 2 dimension. MCD12Q1 is one classification
per 500 m pixel per year, so a point whose class changes between years (forest
to cropland, say) becomes a new dimension version with real effective dates.

All of the submission, collection and resubmission logic lives in
extract_appeears.py; this file only describes the job. It keeps its own task
names, task cache and output folder, so neither extraction can resume, skip or
overwrite the other. It reuses the NDVI point grid, so point ids (p000000, ...)
are identical across both outputs and staging can join them directly.

MCD12Q1 composites are stamped 1 January and published months after the year
ends, so a batch is only asked again once its newest year is over a year old,
and then at most every LANDCOVER_RECHECK_DAYS until the new year appears.

Usage (same modes as extract_appeears.py):
    python extract_landcover.py --sync      # collect finished, request due ranges
    python extract_landcover.py --dry-run   # show what --sync would request
    python extract_landcover.py --status
    python extract_landcover.py --download | --submit | --wait

Credentials are the same EARTHDATA_USER / EARTHDATA_PASS the NDVI script uses.

Output is small next to NDVI: one row per point per year, under 200k rows for
the full history. It is a dimension source, not a volume source.
"""

from __future__ import annotations

from datetime import date

import config
from extract_appeears import Job, main

LANDCOVER = Job(
    label="landcover",
    product=config.LANDCOVER_PRODUCT,
    layers=tuple(config.LANDCOVER_LAYERS),
    out_dir=config.RAW_LANDCOVER,
    points_per_task=config.LANDCOVER_POINTS_PER_TASK,
    limit_setting="LANDCOVER_POINTS_PER_TASK",
    # Composites are stamped 1 January. Starting at START_DATE itself
    # (2012-01-20) would silently drop the first year, so snap to 1 January.
    first_start=date(config.START_DATE.year, 1, 1),
    new_data_days=config.LANDCOVER_NEW_DATA_DAYS,
    recheck_days=config.LANDCOVER_RECHECK_DAYS,
)


if __name__ == "__main__":
    main(LANDCOVER, __doc__)
