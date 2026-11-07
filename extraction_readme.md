# Extraction layer — ITCS544 term project

Pulls every raw dataset the project needs into a partitioned landing zone, with
provenance recorded for each file.

## Setup

From the repo root:

```bash
pip install -r requirements.txt

cp .env.example .env
$EDITOR .env          # fill in your credentials here
```

That is all. `config.py` loads `.env` on import, searching
`data-extraction-scripts/` and then its parents, so the repo-root `.env` is
found whether a script is run through `run_all.sh` or directly:

```bash
cd data-extraction-scripts
python extract_firms.py --probe MODIS_SP 2012-01-20
```

An explicit `export` still overrides the file, which is handy for one-off tests.

Only FIRMS and AppEEARS need credentials. WDPA and GADM are open downloads.

> **Never put real credentials in README.md, config.py, or any tracked file.**
> This repository is a graded public deliverable. `.env` is gitignored;
> `.env.example` is the committed template and holds placeholders only.
> If a key does reach a commit, rotate it — deleting it in a later commit does
> not remove it from git history.

## Run it automatically (Airflow)

Extraction is the first stage of the Airflow DAG (`dags/airflow_dag.py`,
`fire_dwh_orchestration_dag`), which runs daily at 02:00 inside Docker Compose:

```bash
cp .env.example .env && $EDITOR .env     # credentials, once
docker compose up -d --build
open http://localhost:8080               # Airflow UI, login airflow / airflow
```

The DAG starts unpaused, so the first run begins as soon as the scheduler is
up. Each source is its own task (`extract_boundaries`, `extract_wdpa`,
`extract_ndvi`, `extract_landcover`, `extract_firms_nrt`,
`extract_firms_country`, `extract_firms_archive`, `extraction_inventory`):

- they run one after another, and each runs even if the one before failed,
  so a FIRMS outage does not stop WDPA or NDVI being collected;
- each retries twice, 30 minutes apart;
- staging (`extract_and_stage` onward) waits for **all** of them to succeed. A
  failed source marks the run failed in the UI rather than loading stale data;
  the next day's run resumes where the extractors stopped;
- runs never overlap (`max_active_runs=1`), so a long first backfill is never
  doubled up.

The containers mount `data-extraction-scripts/` at `/opt/airflow/extraction`,
so raw data still lands in `data-extraction-scripts/raw/` on the host and the
same files are shared with manual runs. Credentials reach the tasks as
environment variables from `.env`. On Linux, also set `AIRFLOW_UID=$(id -u)` in
`.env` so the files the tasks write are owned by you.

Daily matters for two sources: FIRMS NRT disappears once science-quality data
catches up, and AppEEARS deletes finished results after about 30 days.

## Run it by hand

```bash
data-extraction-scripts/run_all.sh
```

Same steps as the DAG, for one-off runs and debugging. It works from any
directory and uses `.venv` automatically. A failed step does not stop the run,
but the failures are listed at the end and the script exits non-zero. Do not
run it while the DAG is running: both write the same raw files.

Every run is incremental. The first one takes hours (the FIRMS archive); later
runs fetch only new FIRMS windows, new NRT, new AppEEARS date ranges, new
country-archive years and new WDPA releases.

Or individually from `data-extraction-scripts/`, in roughly this order:

| Script | What it gets | Time |
|---|---|---|
| `extract_boundaries.py` | GADM admin levels 0–3, GeoPackage | seconds |
| `extract_wdpa.py --both` | WDPA country zips + global gdb (~1.9 GB) | minutes |
| `extract_appeears.py --sync` | NDVI: collects finished tasks, requests new ranges | seconds; results arrive on a later run |
| `extract_landcover.py --sync` | MCD12Q1 land cover, same grid and mechanism as NDVI | seconds; results arrive on a later run |
| `extract_firms_nrt.py` | Near-real-time detections, incl. NOAA-21 | minutes |
| `extract_firms_country.py` | Country-clipped archive, validates the spatial clip | ~3 min, no credentials |
| `extract_firms.py` | Fire detections, the fact source | hours first time, minutes after |
| `inventory.py` | Row counts + schema divergence report | seconds |

## AppEEARS: how incremental collection works

The point grid is split into 30 fixed batches. Each batch is requested as a
series of date ranges, one AppEEARS task per range
(`itcs544_ndvi_THA_007_20260830_20261106`). Each `--sync`:

1. refreshes the status of outstanding tasks,
2. downloads every finished one,
3. requests the next range for every batch that is due.

A batch is due once it has nothing outstanding, its newest *downloaded*
observation is at least `NDVI_NEW_DATA_DAYS` old, and its last request at least
`NDVI_RECHECK_DAYS` old (`LANDCOVER_*` for land cover: 365 and 30). Coverage is
read from the dates in the CSVs, not the requested range, because composites
are published weeks late. A task that expires or fails before download is
re-requested automatically. Adjacent ranges can repeat a boundary composite:
dedupe on `(ID, Date)` in staging.

```bash
python extract_appeears.py --dry-run   # what --sync would do, changes nothing
python extract_appeears.py --status    # every task, its range and state
python extract_appeears.py --wait      # block until outstanding tasks are collected
```

Do not change `NDVI_GRID_STEP`, `BBOX` or `*_POINTS_PER_TASK` mid-extraction:
batch numbers would refer to different points, so the scripts refuse to run.

## Output layout

Always `data-extraction-scripts/raw/`, regardless of the directory you run from.

```
raw/
├── _manifest.jsonl              provenance: url, sha256, bytes, rows, timestamp
├── firms/
│   ├── sp/sensor=MODIS_SP/year=2024/2024-01-01.csv
│   └── nrt/...
├── firms_country/               reference only, NOT counted in the total
│   └── sensor=MODIS_SP/year=2024/modis_2024_Thailand.csv
├── wdpa/
│   ├── CITATION_Sep2026.txt     one per release, required in the report
│   ├── WDPA_WDOECM_Sep2026_Public_THA.zip
│   └── WDPA_WDOECM_Sep2026_Public.zip   global gdb, has the geometry
├── ndvi/
│   ├── _tasks.json              every task: batch, range, status, newest observation
│   ├── itcs544_ndvi_THA_000__*.csv                   first range (2012 .. Sep 2026)
│   └── itcs544_ndvi_THA_000_20260830_20261106__*.csv later ranges
├── landcover/
│   ├── _tasks.json              separate cache, never shared with ndvi/
│   └── itcs544_landcover_THA_000[_<range>]__*.csv
└── boundaries/gadm41_THA.gpkg
```

Everything is resumable. Completed FIRMS windows are skipped, windows the API
confirms empty get a zero-byte marker, downloads write to `.part` and rename on
success, and AppEEARS task ids are cached. Killing a run mid-way costs nothing.

Failed FIRMS requests are never marked: they are listed at the end of the run,
the script exits non-zero, and the next run retries them. The newest partial
window of each sensor is held back until the archive covers all of it, so
the archive edge never leaves a permanent gap.

## Changing region

Edit `config.py` only. `REGION_NAME`, `ISO3`, `BBOX`, `START_DATE`. Nothing else
hardcodes a location.

## The number for your proposal

```bash
python inventory.py
```

Prints total rows against the 5M requirement, and lists the distinct CSV schemas
it found. That second part matters: it confirms the MODIS/VIIRS column
divergence (`brightness`/`bright_t31` versus `bright_ti4`/`bright_ti5`, and
numeric versus categorical `confidence`) from real headers rather than from
documentation, which is better evidence for the ETL writeup.

## Caveats, read before debugging

- **Verified working:** boundaries, WDPA country + global. FIRMS and AppEEARS
  remain unverified end-to-end because they need credentials.
- **FIRMS signals errors in the response body, not the status code.** A 200
  response containing "Invalid MAP_KEY" is normal API behaviour.
- **NOAA-21 comes only from NRT.** There is no Standard Processing product, so
  it is absent from `sp/` and extracted by `extract_firms_nrt.py` (NRT back to
  2024-01-17). State this in the report so it does not read as an oversight.
- **NRT disappears from the API once SP catches up.** The API serves MODIS /
  S-NPP / NOAA-20 NRT only for dates SP does not cover yet. Re-run
  `extract_firms_nrt.py` regularly; captured files are never deleted, and they
  are the only way to have NRT and SP versions of the same fires side by side.
- **NRT has no `type` column.** Gas flare / volcano flags for NRT rows have to
  be derived in staging.
- **AppEEARS area requests return raster, not CSV.** That is why this uses a
  point grid. If you switch to area sampling you lose the tabular output.
- **AppEEARS limits coordinates per request.** `NDVI_POINTS_PER_TASK` is set
  conservatively at 500. If submission is rejected, lower it before assuming a
  bug.
- **The NDVI grid at 0.1° gives ~14,500 points and ~4.7M rows over 14 years.**
  Close to the 5M line on its own. Tighten `NDVI_GRID_STEP` to 0.05 if you want
  NDVI to clear the bar independently, but that quadruples task count.
- **WDPA and GADM cannot be redistributed.** Both are in `.gitignore`. The repo
  ships these scripts plus `CITATION.txt`, never the data.
- **WDPA history is resolved.** Protected Planet serves only the current
  release, so `extract_wdpa.py` alone gives a Type 1 snapshot. The month-to-month
  history behind the SCD Type 2 dimension comes from Earth Engine
  (`WCMC/WDPA/YYYYMM`) via `extract_wdpa_history.py`: 93 monthly snapshots,
  2017-07 to 2026-09, in `raw/wdpa_historical/`. The `STATUS_YR` fallback is
  not needed.
- **The country archive is not ingest input.** `extract_firms_country.py`
  fetches the same science-quality detections clipped by NASA to the country
  border. About three quarters of the bbox rows fall outside Thailand, and
  these files are how that clip gets verified rather than asserted. Every row
  is already in `firms/sp`, so `inventory.py` reports them separately and
  leaves them out of the record-count total. Do not let them pad the number.
- **The country archive lags about a year.** 2024 is the newest as of Sep 2026,
  and NOAA-21 has no country product at all — same reason it has no
  `VIIRS_NOAA21_SP`. Availability is probed at runtime, so re-running picks up
  2025 when NASA publishes it without a code change.


How to run individual extractors:
cd data-extraction-scripts
python extract_boundaries.py --inspect
python extract_wdpa.py --both
python extract_appeears.py --sync
python extract_firms.py
python extract_firms_country.py --list
python inventory.py
