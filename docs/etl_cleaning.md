# ETL Cleaning, Conforming and Deduplication

Second ETL step: the raw-shaped staging tables (`stage.raw_firms_modis`,
`stage.raw_firms_viirs`, all TEXT) become one typed, conformed, deduplicated table,
`stage.fire_detection_clean`. Job: `controllers/clean_stage.py`. Tables:
`init-scripts/07_clean_dedup.sql`. Run 7, 2026-11-07, 422 s.

**Result: 13,576,562 staged rows → 13,576,064 clean detections + 498 removed.**
Every input row ends up either in the clean table or in `stage.fire_detection_removed`
(with the rule that removed it), and the job checks this before committing.

## How the job runs

All work is SQL inside **one transaction**: truncate the output tables, rebuild them
with `INSERT … SELECT`, re-create the primary key and indexes, write the metrics,
commit. A rerun gives the same result and cannot duplicate rows. If anything fails,
the transaction rolls back and the previous clean table is unchanged. Re-adding the
primary key on `detection_id` is also a final check that deduplication worked:
any duplicate key would make the job fail.

## Rules and counts

| # | Rule | What it does | Rows affected |
|---|---|---|---:|
| 1 | Type casting | Safe casts with `pg_input_is_valid`; a value that fails becomes NULL plus a `cast_fail_<column>` flag | 0 failures |
| 2 | Conform MODIS / VIIRS | One column set for both sensors (below) | all rows |
| 3 | Natural key | `detection_id = md5(satellite \| acq_ts UTC \| round(lat,4) \| round(lon,4))` | all rows |
| A | Exact duplicates | One row kept per `detection_id` | **−498** |
| B | NRT superseded by SP | NRT rows inside the SP date range of their satellite are dropped | **0** |
| 6 | Quality flags | Flagged rows are kept, not deleted (`dq_flags`, `dq_pass`) | 127,247 flagged |

### Rule 1: type casting

Each value is checked before it is cast (`stage.try_numeric`, `stage.try_date`).
An invalid value becomes NULL and the row gets a flag, so one bad value cannot abort
the 13.5M-row load. All values in the current data cast successfully.

`acq_time` is HHMM without leading zeros (`347` = 03:47 UTC). It is zero-padded,
split into hours and minutes, checked (hour < 24, minute < 60) and combined with
`acq_date` into one UTC `timestamptz`, `acq_ts`.

**Bug in the old loader.** `controllers/etl.py` built the timestamp as
`acq_date + (LPAD(acq_time,4,'0') || ':00')::INTERVAL`. Postgres reads `'0347:00'`
as *347 hours*, not 3 h 47 min, so a 03:47 detection landed about two weeks late
(and a 20:00 detection, `'2000:00'`, about 83 days late). The new rule gives
`2022-01-31 03:47:00+00` for `acq_date 2022-01-31, acq_time 347`.

### Rule 2: conform MODIS and VIIRS

| Conformed column | MODIS source | VIIRS source |
|---|---|---|
| `brightness_k` | `brightness` | `bright_ti4` |
| `background_k` | `bright_t31` | `bright_ti5` |
| `confidence_raw` | 0–100 (kept as delivered) | `l` / `n` / `h` (kept as delivered) |
| `confidence_pct` | 0–100 | NULL |
| `confidence_class` | low 0–29 · nominal 30–79 · high 80–100 | `l` low · `n` nominal · `h` high |
| `satellite` | Aqua, Terra | `N` → Suomi-NPP, `N20` → NOAA-20, `N21` → NOAA-21 |

The old 20/50/90 mapping of VIIRS l/n/h (`controllers/etl.py`) was **dropped**.
VIIRS confidence is a category, not a percentage. Turning it into numbers invents
precision, and averaging it with real MODIS percentages gives a meaningless result.
Its `ELSE 50` also turned any unexpected value into "nominal" without a trace.
`confidence_class` makes the two sensors comparable without invented numbers.

| Sensor | low | nominal | high |
|---|---:|---:|---:|
| MODIS | 59,015 | 944,269 | 362,788 |
| VIIRS | 1,332,340 | 10,377,591 | 500,061 |

### Rule 3: natural key `detection_id`

Coordinates are rounded to 4 decimals (~11 m) before hashing. That precision was
chosen from the data: at 3 decimals (~110 m), 44,834 rows would collapse because
neighbouring VIIRS pixels would share a key. At 4 decimals only the 498 true
duplicates do. `detection_id` will be the degenerate dimension on the fact table.

### Rule A: exact duplicates (−498)

When rows share a `detection_id`, the job keeps one, ranked by: SP before NRT,
then higher confidence class, then higher FRP, then `source_file`. The ranking is
deterministic, so reruns keep the same row.

| Sensor | Removed |
|---|---:|
| VIIRS_SNPP_SP | 260 |
| VIIRS_NOAA20_SP | 192 |
| VIIRS_NOAA21_NRT | 46 |
| **Total** | **498** |

All 498 are VIIRS pairs **within the same file**: same satellite and minute, 0–7 m
apart, with slightly different brightness/FRP. These are the same ground spot
sampled twice where adjacent VIIRS scans overlap. Counting both would double-count
the fire. No duplicates were found across files or between SP and NRT.

### Rule B: NRT superseded by SP (0)

SP (science quality) replaces NRT (near real-time) once it is published. For each
satellite, SP coverage is its first-to-last SP `acq_date`; NRT rows for that
satellite inside that range are dropped. The rule uses a date range rather than
matching coordinates, because NRT and SP positions for the same fire can differ
slightly. It uses the range rather than only "days with SP rows", because a day
with no SP detections is still covered by SP.

In the current data there is **no overlap**, so the rule removes 0 rows:

| Satellite | SP range | NRT range |
|---|---|---|
| Aqua / Terra | 2012-01-20 → 2026-06-29 | 2026-07-01 → 2026-09-12 / 09-15 |
| Suomi-NPP | 2012-01-20 → 2026-06-30 | 2026-07-01 → 2026-09-15 |
| NOAA-20 | 2018-04-01 → 2026-06-30 | 2026-07-01 → 2026-09-15 |
| NOAA-21 | none | 2024-01-17 → 2026-09-16 |

The rule matters once FIRMS publishes more SP data and the extraction is rerun.

### Rule 6: quality flags (flag, don't delete)

| Flag | Meaning | Clean rows flagged |
|---|---|---:|
| `type_volcano` | FIRMS type 1 | 0 |
| `type_static_land` | FIRMS type 2, e.g. gas flares, industry | 77,111 |
| `type_offshore` | FIRMS type 3 | 50,136 |
| `coord_impossible` | latitude outside ±90 or longitude outside ±180 | 0 |
| `frp_negative` | FRP < 0 | 0 |
| `missing_critical` | latitude, longitude, timestamp, satellite or brightness missing | 0 |
| `confidence_invalid` | confidence not 0–100 / l / n / h | 0 |
| `cast_fail_*` | value could not be cast | 0 |
| **`dq_pass = false`** | **any flag** | **127,247** |

`dq_pass = true` for 13,448,817 rows. Flagged rows are kept so each analysis can
decide what to exclude (e.g. drop static sources from burned-area statistics but
keep them for QA).

Two notes:
- **Type counts.** In staging there are 77,113 type 2 and 50,137 type 3 rows. Three
  of them were removed by rule A, and each one's kept twin carries the same flag.
- **Negative FRP.** The single "negative" FRP value in the raw data is `-0.0`,
  which is zero, so it is correctly not flagged.

## Limitation: no `type` in NRT

NRT files have no `type` column, so `fire_type` is NULL for all 1,138,956 clean NRT
rows (mostly NOAA-21, 2024–2026). Static sources and offshore detections **cannot
be flagged there**, and some gas flares may be counted as fires in NRT-only periods.
A possible later fix is to flag NRT detections that fall on locations SP has
repeatedly classified as type 2 or 3.

The Thailand border is not applied yet; the data still covers the extraction box
(5–21°N, 97–106°E). Clipping to Thailand with GADM is the next step.

## Metrics

Every run writes before/after counts per rule and sensor to `meta.dq_metrics`
(`src_sensor = 'ALL'` is the total). The run itself is logged in `meta.event_log`.

```sql
SELECT step_order, rule, src_sensor, rows_before, rows_affected, rows_after
FROM meta.dq_metrics
WHERE run_id = (SELECT max(run_id) FROM meta.dq_metrics)
ORDER BY step_order, src_sensor = 'ALL', src_sensor;
```

| Sensor | Input | Rule A | Rule B | Output | dq_pass |
|---|---:|---:|---:|---:|---:|
| MODIS_NRT | 522 | 0 | 0 | 522 | 522 |
| MODIS_SP | 1,365,550 | 0 | 0 | 1,365,550 | 1,339,558 |
| VIIRS_NOAA20_NRT | 3,072 | 0 | 0 | 3,072 | 3,072 |
| VIIRS_NOAA20_SP | 3,870,295 | −192 | 0 | 3,870,103 | 3,835,546 |
| VIIRS_NOAA21_NRT | 1,132,547 | −46 | 0 | 1,132,501 | 1,132,501 |
| VIIRS_SNPP_NRT | 2,861 | 0 | 0 | 2,861 | 2,861 |
| VIIRS_SNPP_SP | 7,201,715 | −260 | 0 | 7,201,455 | 7,134,757 |
| **Total** | **13,576,562** | **−498** | **0** | **13,576,064** | **13,448,817** |
