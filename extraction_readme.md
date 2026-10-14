# Extraction layer — ITCS544 term project

Pulls every raw dataset the project needs into a partitioned landing zone, with
provenance recorded for each file.

## Setup

```bash
pip install -r requirements.txt

cp .env.example .env
$EDITOR .env          # fill in your credentials here
```

That is all. `config.py` loads `.env` on import, so every script picks up
credentials whether run through `run_all.sh` or directly:

```bash
python extract_firms.py --probe MODIS_SP 2012-01-20
```

An explicit `export` still overrides the file, which is handy for one-off tests.

Only FIRMS and AppEEARS need credentials. WDPA and GADM are open downloads.

> **Never put real credentials in README.md, config.py, or any tracked file.**
> This repository is a graded public deliverable. `.env` is gitignored;
> `.env.example` is the committed template and holds placeholders only.
> If a key does reach a commit, rotate it — deleting it in a later commit does
> not remove it from git history.

## Run

```bash
./run_all.sh
```

Or individually, in roughly this order:

| Script | What it gets | Time |
|---|---|---|
| `extract_boundaries.py` | GADM admin levels 0–2, GeoPackage | seconds |
| `extract_wdpa.py --both` | WDPA country zip + global gdb (~1.7 GB) | minutes |
| `extract_appeears.py --submit` | Queues NDVI tasks | queue now, collect later |
| `extract_landcover.py --submit` | Queues MCD12Q1 land cover tasks (same grid as NDVI) | queue now, collect later |
| `extract_firms_nrt.py` | Near-real-time detections, incl. NOAA-21 | minutes; **re-run weekly** |
| `extract_firms_country.py` | Country-clipped archive, validates the spatial clip | ~3 min, no credentials |
| `extract_firms.py` | Fire detections, the fact source | hours |
| `inventory.py` | Row counts + schema divergence report | seconds |

AppEEARS is asynchronous. Submit early, then:

```bash
python extract_appeears.py --status
python extract_appeears.py --download
```

## Output layout

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
│   ├── _tasks.json              submitted task ids, enables resume
│   └── itcs544_ndvi_THA_000__*.csv
├── landcover/
│   ├── _tasks.json              separate cache, never shared with ndvi/
│   └── itcs544_landcover_THA_000__*.csv
└── boundaries/gadm41_THA.gpkg
```

Everything is resumable. Completed FIRMS windows are skipped, empty windows get
a zero-byte marker, downloads write to `.part` and rename on success, and
AppEEARS task ids are cached. Killing a run mid-way costs nothing.

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
python extract_boundaries.py --inspect
python extract_wdpa.py --both
python extract_appeears.py --submit
python extract_firms.py
python extract_firms_country.py --list
python inventory.py
