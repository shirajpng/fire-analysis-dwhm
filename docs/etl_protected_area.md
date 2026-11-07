# ETL Protected Areas: WDPA with SCD Type 2

Fourth ETL step: Thai protected areas from the World Database on Protected Areas
(WDPA) become a **Slowly Changing Dimension Type 2**, `dds.dim_protected_area`,
built from 91 monthly attribute snapshots. Every fire detection gets the protected-
area version that was valid **on its acquisition date** (point-in-time join).

- WDPA load and SCD2 build: `controllers/load_wdpa.py`
- Point-in-time join: `controllers/locate_protected_area.py`
- Tables: `init-scripts/09_wdpa_scd2.sql`
- Runs: 16 (WDPA/SCD2, 2 s) and 17 (join, 101 s), 2026-11-07

**Result: 1,118,834 of 4,418,597 Thai fire detections (25.3%) fall inside a
Designated or Inscribed protected area; another 160,242 (3.6%) only in a Proposed
one.**

Data: UNEP-WCMC and IUCN (2026), Protected Planet: The World Database on
Protected Areas (WDPA), September 2026, www.protectedplanet.net.

## Inputs and profile

| Input | Content |
|---|---|
| `raw/wdpa/WDPA_WDOECM_Sep2026_Public_THA.zip` | current release, file geodatabase: 234 polygons, 7 point-only records |
| `raw/wdpa_historical/wdpa_THA_YYYYMM.csv` | 93 monthly attribute snapshots, 2017-07 to 2026-09, no geometry (Earth Engine `WCMC/WDPA/<yyyymm>/polygons`) |

**Key: `SITE_ID`.** In every snapshot row with IDs, `WDPAID = WDPA_PID`, with no
parcel suffixes. In the current release `SITE_ID = SITE_PID` for all 241 records.
So `SITE_ID`, `WDPAID` and `WDPA_PID` are the same 1:1 key under different names.
`WDPAID` is sometimes written in scientific notation (`5.55512221E8`). It has at
most 9 significant digits, so it converts to an integer exactly; the loader reads
the plain `WDPA_PID` and checks it against `WDPAID`.

**PA vs OECM.** The file is called WDPA_WDOECM, but Thailand has **no OECM records**:
all 241 are `SITE_TYPE = 'PA'`. The loader keeps `site_type` so OECMs would be
carried along if they appear.

**Point-only records (7).** Three UNESCO biosphere reserves and four Ramsar
wetlands (4–421 km²) have only a point, no boundary. They are in the dimension
(`has_polygon = false`) but cannot be used in the spatial join. Drawing circles
from `REP_AREA` was rejected because it would invent boundaries.

**Partial snapshots (2).** 2019-11 has 28 rows and 2025-05 has 16, against 209–231
in neighbouring months. A snapshot is used only if it has at least 90% of the
previous complete snapshot's rows; otherwise ~200 parks would falsely "disappear"
and reappear. **91 of 93 snapshots are used.** 18 calendar months have no snapshot.

**Parks appearing and disappearing** (238 polygon parks in the history):
- 209 parks are present from the first snapshot.
- 29 appear later, 21 of them in 2021-02. Those 21 are not new parks: their
  designation years are 1979–2018, so the WDPA catalogued them late.
- 4 marine national parks disappear after 2020-09 and are not in the current
  release: Mu Ko Phayam (Designated), and Had Khanom–Mu Koh Taletai, Than
  Sadeth–Koh Phangan and Mu Ko Ra–Ko Pra Thong (Proposed).
- No park has a gap inside its lifetime.

## Renamed WDPA columns (for the extraction script)

From the December 2025 WDPA release on, several attribute names changed. The
history extractor (`data-extraction-scripts/extract_wdpa_history.py`) still asks
Earth Engine for the old names (`SCD_FIELDS`), so these columns come back
**empty** in the snapshots 2025-12, 2026-01, 2026-07, 2026-08 and 2026-09:

| Old name (snapshot CSVs, `SCD_FIELDS`) | New name (current release) | Effect in our snapshots |
|---|---|---|
| `WDPAID` | `SITE_ID` | empty from 2025-12 |
| `WDPA_PID` | `SITE_PID` | empty from 2025-12 |
| `MARINE` (0/1/2) | `REALM` (Terrestrial/Coastal/Marine) | empty from 2025-12 |
| `PARENT_ISO3` | `PRNT_ISO3` | empty in **all** snapshots |

Suggested fix for the extractor: select the new names for snapshots from 2025-12
on (or both and coalesce), and re-export the five affected months.

Until then, `load_wdpa.py` recovers the key: for snapshot rows without an ID it
matches `NAME + DESIG_ENG` to the current release. That pair is unique (`NAME`
alone is not: "Salawin" and "Mu Ko Similan" occur twice). **All 1,166 ID-less rows
matched**, with no ambiguity. Each row records how its key was found
(`stage.wdpa_snapshot.site_id_source = 'id' | 'name_match'`). The loader stops
with an error if a row cannot be matched.

## SCD Type 2 design

**Which attribute changes create a new version** (91 snapshots, 238 parks):

| Attribute | Change events | Parks | Treatment |
|---|---:|---:|---|
| STATUS | 0 | 0 | tracked (Type 2) |
| GOV_TYPE | 0 | 0 | tracked |
| DESIG_TYPE, STATUS_YR | 0 | 0 | tracked |
| IUCN_CAT | 1 | 1 | tracked: Mu Ko Ang Thong, Not Reported → VI (2021-02) |
| DESIG_ENG | 12 | 12 | tracked, **wording normalised first**: the 12 changes are relabels only ("Ramsar Site, Wetland of International Importance" → "Wetland of International Importance (Ramsar Site)", "World Heritage Site" → "… (natural or mixed)"), so they create 0 versions |
| REP_AREA | 250 | 227 | tracked, compared at 0.01 km²: real area revisions (median 2.3%, 53 parks > 10%) |
| NAME | 87 | 85 | Type 1 (latest value): mostly romanisation, e.g. Phu Kradueng → Phu Kradung |
| GIS_AREA | 251 | 230 | Type 1: recomputed area, median 0.56% |
| MANG_AUTH | 21 | 21 | Type 1: values replaced by "Not Reported" |
| VERIF, REALM | 1 / renamed | | Type 1 |

Versioning the Type 1 attributes would add ~350 versions that only record
spelling, recalculation or data getting worse, not any change to the park.

**How versions are built** (SQL, gaps-and-islands with window functions):
1. **Hash** the tracked attributes per park and snapshot.
2. **New version** when the hash differs from the park's previous complete
   snapshot, or when the park was missing from the previous complete snapshot.
3. **Dates:**
   - `effective_from` = 1st of the snapshot month in which the version is first seen.
   - `effective_to` (exclusive) = the next version's `effective_from`; or the first
     complete snapshot without the park (removed: 2021-02-01 for the 4 marine
     parks); or `9999-12-31` for current versions (`is_current = true`).
   - `version_no` counts per park.
4. **Backdating (valid time vs. transaction time).** A snapshot shows when *the
   WDPA* recorded something, not when the park was protected. So each park's
   **first version is backdated to January 1 of its `STATUS_YR`** (designation
   year). This also answers fires before the first snapshot (2017-07):
   - 200 parks were designated before 2012, so they cover all our fire data.
   - 7 were designated 2012–2017-06, and 10 in 2018 or later.
   - A fire before a park's designation year is not counted in that park.
   - Proposed parks have no year (`STATUS_YR = 0`), so their first version starts
     at the first snapshot (2017-07-01).
5. **Point-only sites:** one current version, from their designation year.
6. **Special members:**
   - key **0** "Outside protected area": a Thai detection in no valid park version.
   - key **−1** "Not assessed (outside Thailand)": the WDPA file covers Thailand
     only, so a fire in Laos is not "outside a protected area"; we don't know.
7. **Checks before commit:** no overlapping versions per park, no empty date
   ranges, at most one current version per park.

**Result: 245 sites, 489 versions + 2 members = 491 rows.** All 244 later versions
come from REP_AREA (2021-02: 21, 2023-04: 221, 2024-08: 2), plus the one IUCN
change. Example, Khao Sam Roi Yot (site 939):

| version | effective_from | effective_to | rep_area km² |
|---:|---|---|---:|
| 1 | 1966-01-01 | 2021-02-01 | 98.08 |
| 2 | 2021-02-01 | 2023-04-01 | 22.85 |
| 3 | 2023-04-01 | 9999-12-31 | 158.48 |

**Stable keys.** The dimension is `MERGE`d on `(site_id, version_no)`, so
`protected_area_key` does not change on reruns (verified: identical keys after a
second run). Fact rows can safely reference it.

**Changes to the existing table.** `dds.dim_protected_area` (03) was **altered**
in 09, not replaced: it was empty, and `dds.fact_active_fire` already references
it with a foreign key.
- `effective_timestamp`/`expiry_timestamp` → `effective_from`/`effective_to`
  (`DATE`, `effective_to` exclusive and NOT NULL).
- Added: `version_no`, `status`, `status_yr`, `desig_type`, `site_type`, `realm`,
  `name`, `gis_area`, `mang_auth`, `has_polygon`, `record_source`, `tracked_hash`,
  `run_id`, and a unique constraint on `(site_id, version_no)`.
- `site_id` widened to `BIGINT`.
- **`geom` dropped** (see limitations). Polygons live in `stage.wdpa_polygon`.

**Legacy `init-scripts/04_spatial_join.sql` is superseded** by
`controllers/locate_stage.py` (admin areas) and `controllers/locate_protected_area.py`
(protected areas). It uses `dim_protected_area.geom` and `idx_dds_pa_geom`, which
09 removes. It still runs on a fresh database, because init scripts run in order
and 04 comes before 09, but it would fail if run later. **It should be removed in
the cleanup**, together with the old `stg_*` tables and `dds.dim_location_spatial`.

## Point-in-time spatial join

1. **Candidates:** the 4,418,597 in-Thailand detections × 2,672 park polygon pieces
   (`ST_Subdivide(256)`, GIST index), using `ST_Intersects`.
2. **Point in time:** each candidate keeps only the park version with
   `effective_from ≤ acq_date < effective_to`.
3. **Bridge** `stage.fire_detection_pa_bridge`: every (detection, park version)
   membership, **1,363,736 rows**. Use it for "in any protected area" questions
   without double counting.
4. **Primary** `stage.fire_detection_protected_area`: exactly one key per clean
   detection (13,576,064 rows), for the fact table. Rule when a detection lies in
   more than one park:
   1. Designated / Inscribed before Proposed.
   2. National designation before International. Ramsar sites, World Heritage
      Sites and biosphere reserves lie on top of the national parks and wildlife
      sanctuaries that manage the land.
   3. Smallest area: the most specific of nested parks.
   4. Lowest `site_id`, so the choice is deterministic.

| Detections | Count |
|---|---:|
| In exactly 1 park | 1,194,442 |
| In 2 parks | 84,608 |
| In 3 parks | 26 |
| **In 2+ parks** | **84,634** (6.6% of detections in a park) |

| Overlap type (detections in 2+ parks) | Count |
|---|---:|
| International + National, all Designated | 82,759 |
| International + National, one Proposed | 1,638 |
| National + National, one Proposed | 173 |
| National + National, both Designated | 64 |

Almost all overlaps are a World Heritage Site or Ramsar site on top of the
national park or sanctuary it contains:

| Park 1 | Park 2 | Shared fires | Overlap (% of smaller) |
|---|---|---:|---:|
| Huai Kha Khaeng (WS) | Thungyai–Huai Kha Khaeng (WHS) | 35,640 | 92 |
| Thungyai Naresuan (WS) | Thungyai–Huai Kha Khaeng (WHS) | 34,612 | 99 |
| Thap Lan (NP) | Dong Phayayen–Khao Yai (WHS) | 5,291 | 100 |
| Khao Yai (NP) | Dong Phayayen–Khao Yai (WHS) | 2,396 | 100 |
| Chaloem Prakiat Thai Prachan (NP) | Kaeng Krachan Forest Complex (WHS) | 1,637 | 65 |

**Point-in-time effect:** 64,277 memberships were excluded because the fire is
inside a park's current polygon but before that park version existed.
- **6,363** are real: fires 2012–2024 in 11 parks before their designation year.
- **57,914** are fires before 2017-07 in 24 *Proposed* parks, whose first version
  starts at the first snapshot (they have no designation year). This only
  affects the separate "proposed" figure (see the RQ2 note).

**Benchmark:** the spatial join (4,418,597 points × 2,672 pieces) takes **16.2 s**;
the whole job takes 101–108 s, most of it choosing the primary park and writing
metrics. The job sets `enable_parallel_hash = off`. With it on, the first run
(run 14) failed with "could not resize shared memory segment … No space left on
device": a parallel hash join keeps one shared hash table in `/dev/shm`, which is
only 64 MB in the default Docker container. With it off, workers still scan in
parallel, and each builds its own hash table in ordinary memory.

## Results

### RQ2: share of Thai fires inside protected areas, per year

Primary assignment, all sensors pooled, in-Thailand detections only.

| Year | Thai fires | In Designated/Inscribed PA | % | Only in Proposed PA | % |
|---|---:|---:|---:|---:|---:|
| 2012 | 194,037 | 38,549 | 19.9 | 0 | 0.00 |
| 2013 | 227,763 | 40,939 | 18.0 | 0 | 0.00 |
| 2014 | 229,213 | 46,291 | 20.2 | 0 | 0.00 |
| 2015 | 217,872 | 40,712 | 18.7 | 0 | 0.00 |
| 2016 | 265,948 | 66,889 | 25.2 | 0 | 0.00 |
| 2017 | 150,794 | 31,663 | 21.0 | 48 | 0.03 |
| 2018 | 176,281 | 26,125 | 14.8 | 5,480 | 3.11 |
| 2019 | 508,804 | 134,301 | 26.4 | 25,235 | 4.96 |
| 2020 | 472,284 | 147,179 | 31.2 | 24,732 | 5.24 |
| 2021 | 242,929 | 57,210 | 23.6 | 8,552 | 3.52 |
| 2022 | 117,388 | 15,313 | 13.0 | 3,162 | 2.69 |
| 2023 | 396,264 | 127,122 | 32.1 | 26,226 | 6.62 |
| 2024 | 431,650 | 124,423 | 28.8 | 22,030 | 5.10 |
| 2025 | 329,952 | 70,924 | 21.5 | 14,568 | 4.42 |
| 2026 | 457,418 | 151,194 | 33.1 | 30,209 | 6.60 |
| **All** | **4,418,597** | **1,118,834** | **25.3** | **160,242** | **3.63** |

Reading notes:
- **Headline:** Designated/Inscribed only. Proposed parks are reported separately.
- **"Proposed" before 2018 is an artefact of the data.** Proposed parks only exist
  in the dimension from 2017-07-01 (first snapshot), so fires in them before
  2017-07 are not counted. Compare the proposed share only from 2018 on.
- **Pooled sensors.** One fire is often seen by several sensors (MODIS, Suomi-NPP,
  NOAA-20, NOAA-21), and the sensor mix changes over time: NOAA-20 from 2018, and
  2024–2026 adds NOAA-21 NRT. The *share* is meaningful, but the absolute counts
  are detections, not distinct fires.
- **2026** runs to mid-September only (SP to 2026-06-30, NRT after).

### Top 10 parks by fire count (primary assignment, 2012–2026)

| # | Park | Designation | Area km² | Fires | % of PA fires | Fires / km² |
|---:|---|---|---:|---:|---:|---:|
| 1 | Lum Nam Pai | Wildlife Sanctuary | 1,201 | 42,185 | 3.3 | 35.1 |
| 2 | Huai Kha Khaeng | Wildlife Sanctuary | 2,815 | 42,178 | 3.3 | 15.0 |
| 3 | Khuen Si Nakarin | National Park | 1,847 | 36,443 | 2.8 | 19.7 |
| 4 | Thungyai Naresuan | Wildlife Sanctuary | 3,695 | 35,152 | 2.7 | 9.5 |
| 5 | Salawin | National Park | 742 | 34,946 | 2.7 | 47.1 |
| 6 | Salawin | Wildlife Sanctuary | 880 | 33,400 | 2.6 | 38.0 |
| 7 | Chiang Dao | Wildlife Sanctuary | 1,664 | 33,161 | 2.6 | 19.9 |
| 8 | Mae Ping | National Park | 1,046 | 31,311 | 2.4 | 29.9 |
| 9 | Mae Tuen | Wildlife Sanctuary | 1,239 | 30,729 | 2.4 | 24.8 |
| 10 | Doi Phukha | National Park | 1,901 | 28,608 | 2.2 | 15.1 |

All ten are in the north and west (the dry-season burning belt along the Myanmar
border). Huai Kha Khaeng and Thungyai Naresuan rank 2 and 4 because the rule
gives fires to the national sanctuary rather than the World Heritage Site
covering both.

## Limitations

1. **Geometry is current-only.** The snapshots have attributes but no geometry, so
   every version uses the September 2026 boundary. A park whose boundary moved is
   evaluated with today's outline for all years. That is why `geom` was removed
   from the dimension: on historical versions it would suggest boundaries we
   don't have.
2. **7 point-only sites** (3 biosphere reserves, 4 Ramsar wetlands) and the **4
   removed marine parks** have no polygon, so no fire can be assigned to them.
3. **Date precision.** A change takes effect on the 1st of the snapshot month in
   which it is first seen, which is the latest possible date. Where snapshots are
   missing (e.g. 2020-10 to 2021-01), the real date may be up to 4 months earlier.
4. **Backdating** assumes the attributes in a park's first snapshot were also true
   back to its designation year.
5. **Proposed parks before 2017-07** are not represented (see the RQ2 note).
6. **Outside Thailand** (key −1): WDPA protection in neighbouring countries is not
   assessed.

## Metrics

| Table | Content |
|---|---|
| `meta.dq_metrics` run 16 (steps 201–213) | snapshots used/skipped, keys recovered by name, sites, versions, removed sites |
| `meta.dq_metrics` run 17 (steps 301–305) | detections in Designated, in Proposed only, outside PA, not assessed, in 2+ parks, per sensor |
| `meta.event_log` | runs 13/16 (WDPA, 2 s), 14 (failed, `/dev/shm`), 15/17 (join: spatial join 16.2 s) |

```sql
-- RQ2: share of Thai fires in Designated/Inscribed protected areas, per year
SELECT extract(year FROM c.acq_date)::int AS year, count(*) AS thai_fires,
       round(100.0 * count(*) FILTER (WHERE d.status IN ('Designated', 'Inscribed')) / count(*), 1) AS pct_designated,
       round(100.0 * count(*) FILTER (WHERE d.status = 'Proposed') / count(*), 2) AS pct_proposed_only
FROM stage.fire_detection_protected_area a
JOIN stage.fire_detection_clean c USING (detection_id)
JOIN dds.dim_protected_area d USING (protected_area_key)
WHERE a.protected_area_key <> -1
GROUP BY 1 ORDER BY 1;
```
