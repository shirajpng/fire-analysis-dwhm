# ETL NDVI: Staging, Cleaning and Missing-Value Imputation

Vegetation state from MODIS NDVI (MOD13Q1 v061: 16-day composites, 250 m),
sampled by AppEEARS on a 0.1° point grid over the extraction box. This is where
the project's missing-value imputation lives: FIRMS detections are complete, but
31% of the NDVI composites on land are hidden by cloud.

- Loader: `controllers/load_ndvi.py` (incremental, one transaction per file)
- Clean + impute: `controllers/impute_ndvi.py` (one transaction; reruns replace)
- District × month for the star schema: `controllers/load_ndvi_dds.py`
- Tables: `init-scripts/12_ndvi.sql`, `init-scripts/13_ndvi_district_month.sql`
- Extraction: `data-extraction-scripts/extract_appeears.py` ([extraction_readme.md](../extraction_readme.md))

```bash
python controllers/load_ndvi.py      # raw/ndvi/*.csv -> stage.raw_ndvi          ~1.5 min first time
python controllers/impute_ndvi.py    # -> stage.ndvi_clean, stage.ndvi_point     ~3 min
python controllers/load_ndvi_dds.py  # -> dds.fact_ndvi_district_month           ~35 s
```

**Result (run 83, 2026-11-16): 4,979,520 raw rows → 3,108,620 clean rows on 9,143
points; 965,946 of them (31.1%) had no usable value, and 938,242 were imputed.**
On held-out observations the imputation has an RMSE of **0.080** NDVI units,
against 0.095 for linear interpolation and 0.132 for carrying the last value
forward.

## Input

| | |
|---|---:|
| Files (one per AppEEARS task) | 90 |
| Rows | 4,979,520 |
| Rows rejected by the loader | 0 |
| Grid points (0.1°, 160 × 91) | 14,560 |
| Composites, 2012-01-17 to 2026-10-16 | 340 |

A file has 33 columns. The loader keeps the 11 source columns (point id,
coordinates, date, tile, pixel position, NDVI, `VI_Quality`, `pixel_reliability`)
as TEXT in `stage.raw_ndvi`. The other 22 are the two quality columns decoded bit
by bit into text; they are not stored.

**Schema check.** A file is loaded only if its first 11 columns have exactly the
expected names and every further column is a decoded quality column. Any other
header (a renamed column, a new data layer) rejects the whole file into
`stage.raw_ndvi_reject`; a row with the wrong number of fields is rejected on its
own. Loaded files are recorded in `stage.load_file_manifest`, so a rerun skips
unchanged files.

## Rules and counts

| # | Rule | What it does | Rows |
|---|---|---|---:|
| 1 | Type casting | Safe casts (`stage.try_numeric`, `stage.try_date`). A row without a usable point id, date or coordinate is dropped and counted | 0 dropped |
| 2 | Duplicates | One row per point and composite | **−29,120** |
| 3 | Observation class | From `pixel_reliability` (below) | all rows |
| 4 | Point class | Points that never have data are left out | **−1,841,780** |
| 5 | Imputation | Gaps on land points are filled | **938,242 imputed** |
| 6 | Evaluation | Usable observations are hidden, imputed and compared | 133,308 hidden |

4,979,520 − 29,120 − 1,841,780 = **3,108,620** rows in `stage.ndvi_clean`. The job
checks this accounting before it commits.

### Rule 2: duplicates (−29,120)

The extractor requests each batch of points as a series of date ranges, and two
adjacent ranges both return the composite on their boundary. All 29,120 duplicate
pairs have identical NDVI and reliability (`duplicates_conflicting = 0` in the run
log). If NASA reprocesses a composite and the two copies differ, the row from the
newer request is kept.

### Rule 3: observation class

| `pixel_reliability` | Class | Clean rows | Share | Used as observed |
|---:|---|---:|---:|:-:|
| 0 | `good` | 1,255,964 | 40.4% | yes |
| 1 | `marginal` | 886,710 | 28.5% | yes |
| 3 | `cloud` | 919,966 | 29.6% | no |
| −1 (NDVI = −3000) | `fill` | 45,980 | 1.5% | no |
| 2 | `snow_ice` | 0 | | no |
| anything else, or NDVI outside [−0.2, 1] | `invalid` | 0 | | no |

**Why cloudy composites are not used.** A cloudy pixel still has an NDVI value,
but cloud lowers it: the delivered cloudy values average 0.495, against 0.667
for good and 0.684 for marginal ones. The delivered value stays in
`ndvi_observed`; `ndvi` holds the imputed one, which is on average **0.21
higher**.

Missing values are concentrated in the rainy season (land points):

| Thai season | Composites | Missing |
|---|---:|---:|
| Cool (16 Oct – 15 Feb) | 1,005,660 | 10.3% |
| Hot (16 Feb – 15 May) | 815,400 | 14.8% |
| Rainy (16 May – 15 Oct) | 1,259,340 | 56.7% |

### Rule 4: point class

| Class | Meaning | Points | In Thailand |
|---|---|---:|---:|
| `no_data` | Fill value in every composite: sea. No rows in `ndvi_clean` | 5,417 | 1 |
| `sparse` | Fewer than 23 usable composites (one year's worth): water edge. Rows kept, gaps stay NULL | 83 | 7 |
| `land` | Imputed | 9,060 | 4,317 |

Each point gets a `location_code` with the same rules as the fire detections
(`locate_stage.py`: district, Thailand, neighbour country, 1 km coast gap, open
sea), so it joins `dds.dim_location`. 858 of the 928 Thai districts contain at
least one land point.

### Rule 5: imputation

For every land point:

1. **Seasonal mean.** The mean of its usable values in the same composite of the
   year (there are 23 per year) and in the one before and after it, over all
   years: a 48-day window.
2. **Anomaly.** Usable value − seasonal mean.
3. **Fill.** Seasonal mean + the anomaly carried in from the nearest usable
   composite before and after the gap. The two are weighted by closeness, and
   each is multiplied by 0.7 for every composite of distance.

A one-composite gap therefore comes out close to linear interpolation, and a
gap covering the whole rainy season falls back to the point's own seasonal mean.
A gap at the start or end of the series is filled from one side. Observed values
are never changed, and results are clipped to [−0.2, 1].

`gap_length` records how many composites in a row were missing:

| Gap length (composites) | Imputed rows | Share |
|---|---:|---:|
| 1 | 187,432 | 20.0% |
| 2 | 147,754 | 15.7% |
| 3–4 | 218,855 | 23.3% |
| 5–8 | 289,432 | 30.8% |
| 9–16 | 83,250 | 8.9% |
| 17+ | 11,519 | 1.2% |

Example: point `p012577` (Hang Dong, Chiang Mai), rainy season 2023.

| Composite | Class | Delivered | `ndvi` | Imputed |
|---|---|---:|---:|:-:|
| 2023-05-25 | marginal | 0.8284 | 0.8284 | |
| 2023-06-10 | cloud | 0.1994 | 0.7749 | ✔ |
| 2023-06-26 | marginal | 0.6833 | 0.6833 | |
| 2023-07-12 | cloud | 0.5476 | 0.6270 | ✔ |
| 2023-07-28 | cloud | 0.3056 | 0.7548 | ✔ |
| 2023-08-13 | cloud | 0.2922 | 0.8745 | ✔ |
| 2023-08-29 … 2023-11-01 | cloud | 0.04–0.60 | 0.84–0.86 | ✔ |
| 2023-11-17 | marginal | 0.8428 | 0.8428 | |

### Rule 6: evaluation on held-out observations

Hiding random single cells would be too easy a test: real gaps are runs, mostly
in the rainy season. Each point therefore borrows the gap pattern of another
point. 30% of that point's gap runs are laid over this point, and the usable
observations under them are hidden: 133,308 values, 6.2% of the usable ones. The
seasonal means are recomputed without them, so nothing leaks. Four methods are
then scored against the hidden values (RMSE, in NDVI units; the hidden values
have a standard deviation of 0.176):

| Slice | Hidden | Last value carried forward | Linear interpolation | Seasonal mean | **Seasonal anomaly (used)** |
|---|---:|---:|---:|---:|---:|
| **All, weighted to the real gap mix** | 133,308 | 0.1324 | 0.0947 | 0.0855 | **0.0795** |
| Gap of 1 | 20,892 | 0.0844 | 0.0651 | 0.0746 | **0.0624** |
| Gap of 2 | 15,835 | 0.1048 | 0.0774 | 0.0816 | **0.0726** |
| Gap of 3–4 | 23,671 | 0.1243 | 0.0910 | 0.0853 | **0.0800** |
| Gap of 5–8 | 42,237 | 0.1536 | 0.1074 | 0.0911 | **0.0878** |
| Gap of 9–16 | 27,652 | 0.1871 | 0.1261 | 0.0944 | **0.0913** |
| Gap of 17+ | 3,021 | 0.1739 | 0.1512 | 0.0922 | **0.0918** |
| Cool season | 26,677 | 0.1228 | 0.0841 | 0.0725 | **0.0649** |
| Hot season | 26,254 | 0.1007 | 0.0840 | 0.0864 | **0.0738** |
| Rainy season | 80,377 | 0.1607 | 0.1116 | 0.0920 | **0.0894** |

- The method used has the lowest error in every slice: 16% below linear
  interpolation and 40% below carrying the last value forward.
- Linear interpolation is good for short gaps and poor for long ones; the
  seasonal mean is the reverse. The seasonal anomaly follows the better of the
  two at each length.
- Mean absolute error 0.053; bias −0.001. Linear interpolation is biased low
  (−0.009), because it draws a straight line under the rainy-season peak.
- A hidden cell merges with the point's own gaps, so the test has more long gaps
  than the real data. "Weighted to the real gap mix" re-weights the gap classes
  by their share of the real missing values; unweighted, the RMSE is 0.0821.

All rows are in `meta.ndvi_imputation_eval`, per run.

### Choice of parameters

`python controllers/impute_ndvi.py --sweep` scores several damping values on the
same hold-out and writes nothing (RMSE, real gap mix):

| Anomaly kept per composite | 0.0 | 0.5 | 0.6 | **0.7** | 0.8 | 0.9 | 1.0 |
|---|---:|---:|---:|---:|---:|---:|---:|
| RMSE | 0.0855 | 0.0798 | 0.0794 | **0.0795** | 0.0803 | 0.0823 | 0.0864 |

0.0 is the seasonal mean alone; 1.0 interpolates the anomaly without damping.
Anything from 0.5 to 0.8 is within 0.001.

The 48-day window for the seasonal mean was chosen the same way. With one
composite of the year alone the RMSE is 0.0845: in the rainy season a point has
a median of 6 usable values per composite of the year, and 2 or fewer for 15%
of them, too few for a stable mean. A five-composite window gave 0.0801.

## Checks (every run; any failure rolls the job back)

| Check | Result |
|---|---|
| Every raw row is clean, a duplicate, on a no-data point or unusable | 3,108,620 + 29,120 + 1,841,780 + 0 = 4,979,520 |
| One coordinate per point id | max 1 |
| Every point has a location | 14,560 / 14,560 |
| Land points: no missing value left | 0 |
| Imputed rows = missing rows on land points | 938,242 = 938,242 |
| Observed values unchanged | 0 changed |
| All values in [−0.2, 1] | 0 outside |
| Every missing row has a gap length | 0 without |
| Primary key (point, composite) re-created | no duplicate |

**Rerun test.** Two runs gave the same row counts and checksum
(−1538005515105867324460): one on the host (Python 3.13) and one inside the
Airflow scheduler container (Python 3.12). The hold-out uses a fixed seed.

## Limitations

- **A sample, not an area mean.** One 250 m pixel every 0.1° (about 11 km). 70 of
  the 928 Thai districts are too small to contain a land point.
- **Rainy-season values are mostly estimates.** 57% of the rainy-season
  composites are imputed, and their seasonal mean rests on few observations. A
  real drop under cloud (flooding, clearing) is not seen until the next clear
  composite.
- **Marginal observations are used as delivered.** No smoothing (e.g.
  Savitzky-Golay) is applied, so their noise remains.
- **Parameters were chosen on the same hold-out** (one seed) that the table above
  reports. The choice is flat (see the sweep), so the reported error is at most
  slightly optimistic.
- **Recent gaps are filled from one side** and change when later composites
  arrive. The job rebuilds the whole table, so they are corrected then.
- **Composites not delivered yet.** Batches are synced independently, so the
  newest composite can exist for some points only. Such rows are simply absent.
- **Imputation looks both ways.** A gap is filled from the clear composites on
  both sides, and the seasonal mean uses all years. Fine for describing
  vegetation, but an imputed value is not strictly "known at the time"; see the
  anomaly model's limitations ([mining.md](mining.md#limitations-1)).

## District × month: `dds.fact_ndvi_district_month`

`controllers/load_ndvi_dds.py` averages `stage.ndvi_clean` over the land points of
each Thai district, per month. It feeds the anomaly model
([mining.md](mining.md#task-2-anomalous-burning-per-district-and-month)) and
can be joined to anything at district × month grain.

- **Grain:** one row per Thai location of `dim_location` (928 districts and
  `THA_NO_DISTRICT`) and month. `location_key` → `dim_location`; `year_month` is
  YYYYMM, the same as `date_key / 100` on the fire fact.
- **Which month a composite belongs to:** the month its 16-day window **ends**
  in. A month's value never uses a day of a later month, which matters when it
  is used as the state *before* the next month's fires.
- **Districts without a grid point** (70) get the mean of their province, and
  `THA_NO_DISTRICT` the mean of Thailand. `source` says which: `district`,
  `province` or `country`.
- **Measures:** `ndvi_mean`, `imputed_share`, `n_points`, `n_values`.

**Result (run 131, 2026-11-19): 164,433 rows = 929 locations × 177 months
(2012-02 to 2026-10).** 858 locations use their own points (about 5 per
district), 70 their province, 1 Thailand. Checks: every Thai land value is in
exactly one district row (1,467,780), and all means are within [−0.2, 1].

## Tables

`stage.ndvi_clean`: one row per point and composite. `ndvi` is the value to
analyse (observed if usable, else imputed); `is_imputed`, `obs_class` and
`gap_length` say where it came from; `ndvi_observed` is the delivered value.

`stage.ndvi_point`: one row per grid point, with `location_code`, `in_thailand`,
`point_class` and counts.

```sql
-- mean NDVI per province and month, with the share of imputed values
SELECT l.province_name, date_trunc('month', c.composite_date)::date AS month,
       round(avg(c.ndvi), 4) AS ndvi, round(avg(c.is_imputed::int), 2) AS imputed_share
  FROM stage.ndvi_clean c
  JOIN stage.ndvi_point p USING (point_id)
  JOIN dds.dim_location l USING (location_code)
 WHERE p.in_thailand AND p.point_class = 'land'
 GROUP BY 1, 2 ORDER BY 1, 2;
```

## Metrics

- `meta.event_log`: `STAGE_LOAD_NDVI`, `STAGE_NDVI_IMPUTE` (parameters, all
  counts, hold-out RMSE and checksum as JSON in `note`), `LOAD_NDVI_DISTRICT_MONTH`.
- `meta.dq_metrics` steps 701–709 (`src_sensor = 'MOD13Q1'`): input, duplicates,
  no-data rows, missing by cause, imputed, output; step 721: district-months
  that use a province or country mean.
- `meta.ndvi_imputation_eval`: RMSE, MAE and bias per method and slice.
