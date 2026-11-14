# OLAP Queries

Five OLAP queries against the `dds` star schema, each in its own file in `olap/`,
plus a benchmark runner.

- Queries: `olap/q1_*.sql` … `olap/q5_*.sql` (each file states its question,
  OLAP operation, dimensions and filter in a header comment)
- Runner: `controllers/olap_benchmark.py` (read-only; logs `OLAP_BENCHMARK` in
  `meta.event_log`)
- Output: `olap_query_results/q1.csv` … `q5u.csv` (results),
  `olap_query_results/plans/*.txt` (`EXPLAIN (ANALYZE, BUFFERS)`),
  `olap_query_results/benchmark.csv` (timings)

```bash
python controllers/olap_benchmark.py            # all queries, 1 first + 5 warm runs each
python controllers/olap_benchmark.py --only q2  # one query
```

## The five queries

| # | File | OLAP operation | Question | Design feature shown |
|---|---|---|---|---|
| Q1 | `q1_rollup_fire_year_location.sql` | Roll-up / drill-down (`ROLLUP`) | Thai fires per fire year → province → district | Location hierarchy (`dim_location`), Aug–Jul fire year (`dim_date`) |
| Q2 | `q2_protected_area_share_by_season.sql` | Slice / dice | Share of Thai fires inside Designated/Inscribed vs only Proposed protected areas, per fire year and Thai season (RQ2) | SCD Type 2 protected areas, matched point in time |
| Q3 | `q3_top_parks_bridge.sql` | Roll-up over a many-to-many dimension | The 10 parks with most fires, `count(*)` vs `sum(allocation_weight)` | Bridge table, allocation weights, SCD2 versions rolled up per site |
| Q4 | `q4_sensor_pivot.sql` | Pivot (`FILTER`) | Confidence mix, day/night split and FRP per satellite and product | Junk dimension (`dim_detection_flag`), `dim_satellite_instrument` |
| Q5 | `q5_province_burning_season.sql` | Slice + drill-down | Chiang Mai, 2024 burning season, by month and district | Partition pruning (year filter on `date_key`) |

Q1, Q4 and Q5 use the standard Thai filter `dim_location.in_thailand AND
dim_detection_flag.dq_pass`. Q2 uses the official RQ2 definition (in Thailand,
flagged rows included) so its totals reconcile with
[etl_protected_area.md](etl_protected_area.md#rq2-share-of-thai-fires-inside-protected-areas-per-year).
Q3 counts every fire in the bridge (the bridge only holds fires in Thailand).

`q5_province_burning_season_unpruned.sql` is not a sixth query: it is Q5 with the
year filtered on `dim_date.year_number` instead of the fact's partition key, used
only to measure what partition pruning saves.

## Benchmark

**Run of 2026-11-13** on a fresh full build: 13,593,697 fact rows. That is
17,633 more than the build in the README, because this extraction ran later
and has more NRT days. Machine: MacBook with 8 GB RAM; Docker Desktop VM with
4 GB and 8 CPUs; Postgres defaults (`shared_buffers` 128 MB,
`max_parallel_workers_per_gather` 2, JIT on); Airflow stopped. Each query ran
once, then 5 more times, with all rows fetched.

| Query | Result rows | First run (s) | Warm median (s) | Warm min–max (s) | Fact partitions scanned | Blocks from shared_buffers / read from outside |
|---|---:|---:|---:|---:|---:|---|
| Q1 roll-up | 15,149 | 24.8 | **17.9** | 13.4–26.7 | 20 | 15,798 / 314,289 |
| Q2 protected-area dice | 64 | 10.0 | **10.5** | 9.7–13.7 | 20 | 15,847 / 314,261 |
| Q3 bridge | 10 | 3.9 | **2.1** | 1.9–2.4 | — (bridge) | 1,125 / 13,489 |
| Q4 sensor pivot | 9 | 12.8 | **11.8** | 10.5–15.0 | 20 | 15,747 / 314,322 |
| Q5 Chiang Mai 2024 (pruned) | 97 | 1.4 | **0.38** | 0.33–0.85 | **1** | 2,908 / 32,782 |
| Q5 unpruned (year on `dim_date`) | 97 | 7.3 | **8.6** | 7.0–10.0 | 20 | 16,032 / 314,449 |

"Partitions scanned" counts every partition the plan executed, including the
empty 2027–2030 and DEFAULT partitions. A block is 8 kB.

**Partition pruning: Q5 is 22.7× faster** (0.38 s vs 8.6 s, warm median) when
the year is filtered on `date_key`. It reads 1 partition instead of 20, and
about 36,000 blocks instead of 330,000. Both versions return identical rows
(checked). In practice: put date filters on `f.date_key`, not only on
`dim_date` columns.

**What limits the full-table queries (Q1, Q2, Q4).** Each reads about 314,000
blocks (2.4 GB) from outside `shared_buffers`, because the 4.15 GB fact does not
fit in a 128 MB buffer cache or in the 4 GB VM. Warm runs are therefore not much
faster than first runs, and the spread is wide (up to 2×) as the operating
system's file cache is evicted between queries. These times are bounded by I/O.
Things worth trying, not done yet:
- a larger `shared_buffers` and more parallel workers on a machine with more memory
- a summary table or materialized view at district × day grain; Q1, Q2 and Q4
  only need counts and sums

"First run" is cold only for Postgres's own cache: the VM's file cache may
already hold part of the data.

## Checks (every run)

| Check | Result |
|---|---|
| Q1 grand total = sum of district rows = Thai `dq_pass` fires in the fact | 4,387,727 = 4,387,727 = 4,387,727 |
| Q2 grand total = Thai fires in the fact | 4,426,788 = 4,426,788 |
| Q2 designated + proposed-only = Thai fires with a park key | 1,279,526 = 1,279,526 |
| Q3 allocation weights over all parks sum to the number of fires in a park | 1,279,526 = 1,279,526 |
| Q5 pruned = unpruned | 97 identical rows |
| Q5 returns rows (`'Chiang Mai'` spelled as in `dim_location`) | 97 rows |

## Results worth reporting

**Q2 (RQ2).** 25.3% of Thai fires fall inside a Designated or Inscribed protected
area, and another 3.62% fall only inside a Proposed one. This matches the
official RQ2 table to the decimal; counts differ by the extra NRT days. By fire
year, the designated share ranges from 12.7% (2021/22) to 32.4% (2022/23); the
2026/27 row is a partial year. All 64 rows are in `olap_query_results/q2.csv`.

**Q3 (bridge).** The top park, the Thungyai-Huai Kha Khaeng World Heritage Site
(70,915 fires), overlaps the two wildlife sanctuaries it is made of. Huai Kha
Khaeng (42,178 fires) and Thungyai Naresuan (35,157) are both in the top five.
Counting rows per park, the same fires are counted two or three times. With
allocation weights the Heritage Site drops to 35,647. Only 379 of its fires are
assigned to it as primary park, because the primary rule prefers the smallest
park. Parks without overlaps (Lum Nam Pai, Salawin, Chiang Dao, …) have equal
count and allocation.

**Q4 (sensors).** VIIRS (375 m; three satellites) has about 11× as many
detections as MODIS (1 km; two satellites), 4,034,591 against 353,136, at much
lower FRP per pixel. For example, S-NPP
daytime averages 7.2 MW against 28.4 MW for Aqua. MODIS is mostly daytime:
94% for Aqua, 64% for Terra, whose second pass is at about 22:30. The
`high` confidence class is 16–19% for MODIS but only about 2% for VIIRS, so
confidence classes cannot be compared directly across sensors.

**Q5 (Chiang Mai, 2024).** 36,228 detections in January–April, of which 54%
(19,523) are inside a protected area. March (17,681) and April (14,461) hold 89%
of them.
