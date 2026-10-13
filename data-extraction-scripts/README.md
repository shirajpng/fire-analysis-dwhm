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
├── wdpa/
│   ├── CITATION_Sep2026.txt     one per release, required in the report
│   ├── WDPA_WDOECM_Sep2026_Public_THA.zip
│   └── WDPA_WDOECM_Sep2026_Public.zip   global gdb, has the geometry
├── ndvi/
│   ├── _tasks.json              submitted task ids, enables resume
│   └── itcs544_ndvi_THA_000__*.csv
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
- **NOAA-21 is intentionally absent.** Only an NRT product exists; there is no
  Standard Processing equivalent, so it cannot join an archive build. State this
  in the report so it does not read as an oversight.
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
- **WDPA history is not available here.** Protected Planet serves only the
  current release, so this gets you a Type 1 snapshot. Month-to-month history
  for the SCD Type 2 dimension needs Earth Engine (`WCMC/WDPA/YYYYMM`, from
  July 2017) or the `STATUS_YR` fallback. **This is still unresolved.**


How to run individual extractors:
python extract_boundaries.py --inspect
python extract_wdpa.py --both
python extract_appeears.py --submit
python extract_firms.py
python inventory.py