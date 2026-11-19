# Data Mining

Two mining jobs read the `dds` star schema and write their results back into
`dds.ml_*` tables. Both run after `load_dds.py`, by hand or as the last two
tasks of the Airflow DAG.

| Job | Task | Output | Time |
|---|---|---|---:|
| `controllers/mine_fire_events.py` | 1: cluster detections into fire events (RQ1) | `ml_fire_event`, `ml_fire_event_detection`, `ml_fire_event_silhouette` | ~4 min |
| `controllers/mine_district_anomalies.py` | 2: anomalous burning per district and month (RQ3) | `ml_district_month_anomaly` | ~35 s |

```bash
python controllers/mine_fire_events.py
python controllers/mine_district_anomalies.py
```

Like the ETL jobs, each runs in one transaction, replaces its output, checks
the result before committing (a failed check rolls everything back) and logs
itself in `meta.event_log` (`MINE_FIRE_EVENTS`, `MINE_DISTRICT_ANOMALIES`; the
`note` column holds the parameters and all counts as JSON). Tables are created
by `init-scripts/11_ml_mining.sql`. They need `scikit-learn` and `scipy`
(`requirements.txt`).

Task 1 numbers are from the run of 2026-11-14 (13,593,806 fact rows). Task 2
numbers are from the nightly run of 2026-11-19 (13,595,257 fact rows), with the
NDVI features.

**Rerun test.** The jobs were run by hand (Python 3.13, scipy 1.18) and again
by Airflow after a fresh `load_dds` (Python 3.12, scipy 1.17). Row counts and
checksums of `ml_fire_event`, `ml_fire_event_detection` and
`ml_district_month_anomaly` were identical: the same input gives the same
event ids, roles and scores.

## Task 1: fire events

**Input:** the 4,387,775 detections in Thailand with `dq_pass`. Static sources
(gas flares, industry) and offshore detections are flagged in cleaning and so
are not clustered.

**Method: ST-DBSCAN**, DBSCAN with separate radii for space and time.

- Two detections are neighbours when they are at most **1.5 km** apart on the
  ground and at most **2 days** apart in time.
- A detection with at least **3** neighbours, itself included, is a core point.
  Core points that are neighbours belong to the same event.
- A non-core detection next to a core point joins the event of its nearest core
  point (`role = 'border'`). Everything else is noise: `event_id` is NULL.
- Distances are in metres in UTM zone 47N (EPSG:32647).
- Each Aug–Jul fire year is clustered on its own, so memory stays small. An
  event running across 31 July / 1 August would be split; those two months hold
  0.6% of the fires.

The neighbour search uses a KD-tree over time slabs of 10,000 detections, so
the pair list never has to fit in memory at once. The result was compared with
a brute-force all-pairs DBSCAN on 3,000 detections: same core points, same
events, same border and noise points.

HDBSCAN, named in the proposal, was not used: scikit-learn's implementation
needs time quadratic in the number of points, which rules out years of 400,000
detections. ST-DBSCAN takes 2–3 seconds for such a year.

This step is also the cross-satellite deduplication: one fire seen by several
satellites or on several overpasses becomes one event.

### Result

| | |
|---|---:|
| Detections clustered | 4,387,775 |
| Fire events | **306,689** |
| Detections in an event (core / border) | 3,010,866 (2,933,586 / 77,280) |
| Noise (isolated detections) | 1,376,909 (31.4%) |
| Events seen by more than one satellite | 218,378 (71.2%) |
| Events with a detection in a protected area | 72,318 (23.6%) |
| Events spanning more than one district | 29,731 (9.7%) |
| Detections per event, median / mean / max | 4 / 9.8 / 6,794 |
| Duration, mean / max | 1.2 days / 59 days |

| Event size (detections) | Events | Detections |
|---|---:|---:|
| 3–4 | 162,976 | 545,940 |
| 5–9 | 89,447 | 565,469 |
| 10–49 | 47,485 | 871,091 |
| 50–499 | 6,515 | 732,655 |
| 500+ | 266 | 295,711 |

The five largest events are all month-long burning complexes in or next to
protected areas in the west and north: Lum Nam Pai (March 2026, 6,794
detections over 80 km), Salawin (2019 and 2026), Mae Wong (2026) and Huai Kha
Khaeng (2019, all 5,253 detections inside the sanctuary).

| Fire year | Detections | Events | Noise % | Silhouette |
|---|---:|---:|---:|---:|
| 2011/12 (partial) | 176,892 | 13,817 | 39.7 | 0.492 |
| 2012/13 | 228,576 | 18,302 | 44.3 | 0.564 |
| 2013/14 | 222,744 | 17,315 | 42.8 | 0.525 |
| 2014/15 | 219,615 | 17,774 | 44.3 | 0.602 |
| 2015/16 | 264,601 | 20,635 | 38.1 | 0.534 |
| 2016/17 | 149,950 | 12,394 | 41.6 | 0.606 |
| 2017/18 | 152,489 | 11,136 | 46.5 | 0.514 |
| 2018/19 | 480,182 | 34,123 | 27.4 | 0.459 |
| 2019/20 | 498,843 | 34,337 | 26.4 | 0.481 |
| 2020/21 | 246,392 | 17,634 | 34.3 | 0.512 |
| 2021/22 | 118,474 | 9,021 | 52.7 | 0.685 |
| 2022/23 | 389,718 | 24,487 | 26.1 | 0.397 |
| 2023/24 | 415,226 | 26,586 | 23.9 | 0.473 |
| 2024/25 | 336,031 | 23,255 | 23.0 | 0.518 |
| 2025/26 | 482,417 | 25,584 | 17.9 | 0.268 |
| 2026/27 (partial) | 5,625 | 289 | 72.1 | 0.791 |

### Clustering quality (RQ1)

The silhouette is computed for a random sample of 2,000 clustered detections
per fire year (31,568 in total), in the space where one unit is 1.5 km on the
ground and 2 days in time. All 31,568 values are exact (`is_exact`): the
nearest other event is proven, not approximated. The sample is stored in
`dds.ml_fire_event_silhouette`, so it can be sliced by any dimension.

**Overall mean silhouette: 0.523.**

| Thai season | Sampled | Silhouette | Negative values |
|---|---:|---:|---:|
| Rainy | 995 | 0.834 | 1.3% |
| Cool | 7,795 | 0.673 | 5.4% |
| Hot | 22,778 | 0.458 | 17.4% |

Quality falls as burning gets denser. In the rainy season fires are few and far
apart, and events are well separated. In March and April (0.43) fires in the
north burn next to each other for weeks, events touch, and a detection is often
as close to the next event as to its own. The worst year, 2025/26 (0.268), is
also the year with the largest burning complexes.

```sql
-- silhouette by province
SELECT l.province_name, count(*) AS sampled, round(avg(s.silhouette), 3) AS silhouette
  FROM dds.ml_fire_event_silhouette s
  JOIN dds.fact_active_fire f USING (detection_id, date_key)
  JOIN dds.dim_location l     USING (location_key)
 GROUP BY 1 HAVING count(*) >= 100
 ORDER BY 3;
```

### Choice of parameters

`python controllers/mine_fire_events.py --sweep 2023` clusters one fire year
with 36 parameter sets and writes nothing. Part of its output for 2023/24
(415,226 detections):

| eps km | eps days | min pts | Events | Noise % | Largest event | Max days | Max extent km |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.75 | 2 | 3 | 29,425 | 29.8 | 3,045 | 30.0 | 44.8 |
| 1.0 | 2 | 3 | 28,206 | 27.0 | 4,431 | 30.0 | 55.6 |
| 1.5 | 1 | 3 | 28,450 | 27.3 | 4,186 | 19.0 | 53.7 |
| 1.5 | 2 | 2 | 46,407 | 14.3 | 4,697 | 32.6 | 61.0 |
| **1.5** | **2** | **3** | **26,586** | **23.9** | **4,697** | **32.6** | **61.0** |
| 1.5 | 2 | 5 | 13,421 | 35.2 | 4,693 | 32.6 | 61.0 |
| 1.5 | 5 | 3 | 23,523 | 19.2 | 11,776 | 69.5 | 135.0 |
| 2.0 | 2 | 3 | 25,089 | 21.5 | 5,485 | 36.5 | 73.0 |
| 2.0 | 5 | 3 | 20,962 | 16.3 | 20,488 | 70.5 | 188.9 |

- **1.5 km** covers two MODIS pixels that touch diagonally (1 km pixels, 1.4 km
  centre to centre) and several VIIRS pixels (375 m).
- **2 days** bridges one day without a detection (cloud, or no overpass). With 5
  days the largest event more than doubles to 135 km across: separate fires get
  chained together.
- **min pts** hardly changes the large events; it decides whether pairs count
  as events. With 3, an event needs three detections.

### Limitations

- **Noise falls as satellites are added.** With one VIIRS satellite (until 2018)
  38–47% of the detections are isolated; with three (from 2024) about 20%. The
  same small fire gives one detection in 2014 and three in 2025. Event counts
  are therefore not comparable across these periods.
- **Thailand only.** Detections across the border are not clustered, so an event
  on the border is cut at it.
- **Large events are chains.** In dense burning, DBSCAN joins everything that
  touches. The month-long 50–80 km events are burning complexes, not single
  fires.
- **`frp_sum_mw`** adds every detection, so a fire seen on ten overpasses
  counts ten times. Use `frp_max_mw` or `frp_mean_mw` to compare events.

### Tables

`dds.ml_fire_event`: one row per event. `event_id` is the fire year × 1,000,000
plus the event's position in start order, so the same input gives the same ids.
`location_key` is the district with most of its detections,
`protected_area_key` the park version with most of its in-park detections (0 =
none). `start_date_key` joins `dim_date`.

`dds.ml_fire_event_detection`: one row per clustered detection, with `event_id`
(NULL for noise) and `role`. Joins the fact on `(detection_id, date_key)`.

```sql
-- events and detections per fire year and province
SELECT d.fire_year_label, l.province_name, count(*) AS events, sum(e.n_detections) AS detections
  FROM dds.ml_fire_event e
  JOIN dds.dim_date d     ON d.date_key = e.start_date_key
  JOIN dds.dim_location l USING (location_key)
 GROUP BY 1, 2 ORDER BY 1, 3 DESC;
```

## Task 2: anomalous burning per district and month

**Question (RQ3):** which district-months burn unlike what that district's own
history predicts?

**Input: Suomi-NPP VIIRS only**, in Thailand, `dq_pass`. It is the one satellite
with a record over the whole period. NOAA-20 joined in 2018 and NOAA-21 in
2024, so counts pooled over all satellites rise for that reason alone, and
every recent month would look anomalous.

**Vegetation:** mean NDVI per district and month from
`dds.fact_ndvi_district_month` ([etl_ndvi.md](etl_ndvi.md)), with cloud gaps
imputed. Run `load_ndvi_dds.py` before this job.

**Grid:** all 929 Thai districts × 153 complete months (2014-02 to 2026-10) =
142,137 rows, holding 2,031,696 detections. Months without a detection count
as 0. Left out: the first month (it starts on the 20th), the current month,
and the first two years, which have no history to compare with.

**Features** (14), per district and month:

| Feature | Meaning |
|---|---|
| `log_n`, `log_n_pa`, `log_frp_sum`, `log_frp_max` | log(1 + x) of the detection count, the count inside a Designated / Inscribed protected area, the FRP sum and the FRP maximum |
| `pa_share`, `night_share` | shares of the count |
| `expected_log_n` | mean of `log_n` over the earlier years, same district and calendar month |
| `dev_log_n`, `dev_log_n_pa`, `dev_log_frp_sum` | value minus its mean over those earlier years |
| `z_n` | `dev_log_n` / max(standard deviation over those years, 0.5) |
| `lag1_log_n` | `log_n` of the month before |
| `ndvi_lag1` | mean NDVI of the district in the month before: the vegetation state before the month's fires |
| `dev_ndvi_lag1` | `ndvi_lag1` minus its mean over the earlier years, same calendar month (negative = drier or barer than usual) |

"Earlier years" never include the month itself or anything later: every month
is compared only with what was known before it.

NDVI is taken from the month before, not the month itself, because burning
lowers NDVI: the month's own value is partly a result of its fires. A composite
counts for the month its 16-day window ends in, so the month before contains no
day of the scored month. If the month before has no NDVI yet (composites are
published some weeks late), the district's usual value for that calendar month
is used with a deviation of 0; this happened for 0 rows in the current run.

**Model:** Isolation Forest, 200 trees, fixed seed. It is fitted on the months
before August 2022 (94,758 rows) and scores every month. `is_anomaly` uses the
threshold that flags 2% of the training months. The 47,379 rows from August
2022 on are out of sample (`split = 'test'`).

### Result

| | Train (2014-02 – 2022-07) | Test (2022-08 – 2026-10) |
|---|---:|---:|
| District-months | 94,758 | 47,379 |
| Flagged | 1,896 (2.0%) | 942 (2.0%) |
| – more burning than expected (`direction = 'high'`) | 1,250 | 709 |
| – less burning than expected (`'low'`) | 646 | 233 |

The share flagged out of sample (1.99%) is close to the 2% the threshold was set
for, so the model has not drifted on the newer years.

The most anomalous district-months:

| Month | District, province | Detections | In protected area | Expected | NDVI month before | vs usual | Split |
|---|---|---:|---:|---:|---:|---:|---|
| 2026-04 | Fak Tha, Uttaradit | 711 | 652 | 25 | 0.596 | +0.083 | test |
| 2023-04 | Phu Kradung, Loei | 284 | 270 | 8 | 0.602 | +0.037 | test |
| 2020-02 | Ban Rai, Uthai Thani | 1,936 | 1,647 | 327 | 0.598 | −0.131 | train |
| 2026-04 | Nong Bua Daeng, Chaiyaphum | 726 | 593 | 53 | 0.598 | +0.026 | test |
| 2026-04 | Klong Lan, Kamphaeng Phet | 1,316 | 1,284 | 88 | 0.659 | +0.048 | test |
| 2020-02 | Phu Kradung, Loei | 523 | 426 | 104 | 0.473 | −0.165 | train |
| 2019-04 | Mae Sariang, Mae Hong Son | 2,435 | 1,948 | 349 | 0.614 | +0.030 | train |
| 2020-01 | Sam Ngao, Tak | 1,714 | 1,411 | 85 | 0.670 | −0.061 | train |

"Expected" is exp(`expected_log_n`) − 1. All of the top rows are surges inside
protected areas: for 2,766 of the 2,838 flagged rows, the feature furthest from
normal (`top_feature`) is one of the three protected-area features, and for 5
it is `dev_ndvi_lag1`.

Flags per fire year follow the known bad fire years: 2015/16 (323), 2018/19
(291), 2019/20 (389), 2022/23 (274) and 2025/26 (266) are the highest. In the
quiet years 2016/17, 2017/18, 2020/21 and 2021/22 most flags are `low`.

```sql
-- the 20 most anomalous district-months that were not used for training
SELECT a.year_month, l.province_name, l.district_name, a.n_detections, a.n_in_pa,
       round(exp(a.expected_log_n) - 1) AS expected, a.direction, a.top_feature
  FROM dds.ml_district_month_anomaly a
  JOIN dds.dim_location l USING (location_key)
 WHERE a.split = 'test'
 ORDER BY a.anomaly_rank
 LIMIT 20;
```

### Effect of the NDVI features

The model was fitted twice on the same data (run of 2026-11-19), with the 12
fire features only and with the 2 NDVI features added. Same seed and threshold.

**Is pre-fire vegetation related to unusual burning?** Yes, weakly. Districts
that were drier or barer than usual the month before burn more than their own
history predicts. Burning season (January–April), 47,379 district-months, split
into fifths by `dev_ndvi_lag1`:

| NDVI month before, vs usual | Mean deviation | Months above expected burning | Burning vs expected (geometric mean) |
|---|---:|---:|---:|
| Driest fifth | −0.065 | 45.2% | 0.97× |
| 2 | −0.017 | 41.6% | 0.93× |
| 3 | +0.003 | 38.2% | 0.85× |
| 4 | +0.022 | 36.5% | 0.77× |
| Greenest fifth | +0.063 | 34.0% | 0.72× |

Spearman correlation between `dev_ndvi_lag1` and `dev_log_n`: −0.12 over
January–April (−0.17 in January and February, −0.08 in March and April), and
−0.26 in May, at the end of the season. Most of it is between years: dry years
burn more everywhere. Comparing districts within the same month, the correlation
is only −0.05.

**What changes in the result:**

| | Without NDVI | With NDVI |
|---|---:|---:|
| Flagged, test months | 909 (1.92%) | 942 (1.99%) |
| Flagged `high` / `low`, all months | 1,744 / 1,061 | 1,959 / 879 |
| Flagged `high` in the driest fifth (Jan–Apr) | 340 | 415 |

- 2,500 district-months are flagged by both; 305 only without NDVI and 338 only
  with it. Rank correlation of the two scores: 0.964. Of the 100 most anomalous
  months, 76 are the same.
- The 305 dropped are mostly `low` (224) and small (median 8 detections). The
  338 added are mostly `high` (296), larger (median 90 detections), and on
  average follow a drier-than-usual month (−0.010 against +0.012).
- In short, NDVI moves the model's attention from "less burning than usual",
  which is rarely the question, toward surges that follow dry vegetation. The
  most anomalous months are still the protected-area surges.
- Agreement with the stand-in rule below is unchanged (ROC-AUC 0.966 without,
  0.965 with). This is no evidence either way, since the rule uses only fire
  counts.

### Evaluation: not done yet

**There are no ground-truth labels**, so precision, recall, ROC-AUC and PR
curves cannot be measured yet. Where the labels come from is still an open
decision from the proposal.

The job prints one stand-in: on the test months it compares the score with a
simple rule, `z_n >= 3` and at least 10 detections (67 district-months, 0.14%).
ROC-AUC is 0.965 and average precision 0.124; 31 of the 67 are flagged. Since
`z_n` is one of the features, this only shows that the forest ranks the
rule's months near the top. It is not accuracy.

### Limitations

- **Not a forecast.** The model scores a month after it has happened, against
  the district's history. It does not predict next month.
- **Vegetation is sampled and partly imputed.** District NDVI comes from about 5
  grid points per district; 70 of the 928 districts have none and get their
  province's value. In the burning season 10–15% of the values behind it are imputed.
- **Small look-ahead in imputed NDVI.** A cloud gap is filled from the clear
  composites on both sides, and the seasonal mean uses all years. An imputed
  value for the month before can therefore draw on a later composite. In the
  dry months the model uses this is rare, but the NDVI features are not
  strictly "known before" the month.
- **Vegetation state is not a cause.** The NDVI relation is weak (−0.12) and
  mostly between years; it does not show that drier vegetation causes burning
  in a given district.
- **Protected-area features dominate.** Districts with large parks produce most
  flags. That fits the project's question, but a surge of farm burning outside
  parks scores lower.
- **Suomi-NPP only**: 55% of the Thai `dq_pass` detections (2,407,401 of 4,387,775). From July 2026 its data
  is Near Real-Time, which has no `type` column, so static sources are not
  flagged there.
- **Fire events are not features.** Event counts depend on the number of
  satellites (see task 1).

## Superseded

- `dags/ml_mining.py` (removed): written for the old schema; it clustered in
  space only and scored single detections.
- `dds.ml_fire_anomalies` (05): its output table. Empty, unused, left in place.
