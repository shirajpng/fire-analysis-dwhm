# ETL Location: GADM Admin Hierarchy and Thailand Clip

Third ETL step: every clean detection (`stage.fire_detection_clean`, 13,576,064 rows)
is linked to the Thai admin hierarchy (district → province → country) from GADM 4.1,
and gets an `in_thailand` flag. Fires outside Thailand are **kept and flagged, not
deleted**. Run 11, 2026-11-07.

- Reference load: `controllers/load_reference.py`
- Spatial join and validation: `controllers/locate_stage.py`
- Benchmark: `controllers/bench_spatial_join.py`
- Tables: `init-scripts/08_gadm_location.sql`

**Result: 4,418,597 detections (32.5%) are in Thailand, 9,157,467 outside.** The
extraction box (5–21°N, 97–106°E) also covers large parts of Myanmar, Laos,
Cambodia, Vietnam and Malaysia.

## Step 1: GADM boundaries

| Layer | Source | Rows |
|---|---|---:|
| Country (level 0) | Thailand + MMR, LAO, KHM, MYS, VNM | 6 |
| Province (level 1) | Thailand | 77 |
| District (level 2) | Thailand (845 districts, 81 minor districts, 2 water bodies) | 928 |
| District pieces | `ST_Subdivide(geom, 256)` | 12,824 |
| Country pieces | `ST_Subdivide(geom, 256)` | 9,112 |

- **Download:** the neighbour files are fetched by `data-extraction-scripts/extract_boundaries.py`
  (`config.NEIGHBOUR_ISO3`) and recorded in `raw/_manifest.jsonl`, like the Thailand
  file, so Airflow can reproduce them.
- **Loading:** geopandas reads the polygons and psycopg2 `COPY` loads them as hex EWKB.
  No ogr2ogr or extra packages are needed.
- **Keys are GADM GIDs, not names:** 11 district names occur in more than one province.
- **Water bodies:** the two 'Water body' districts (Songkhla Lake, in Songkhla and
  Phatthalung) count as Thailand. They contain 2 detections.

## Step 2: spatial join and classification

Each detection gets exactly one location; the first rule that matches wins:

| # | Rule | location_code | Detections |
|---|---|---|---:|
| 1 | Point in a Thai district piece (`ST_Intersects`) | GID_2 of the district | **4,418,597** |
| 2 | In the Thailand country polygon but no district | `THA_NO_DISTRICT` | 0 |
| 3 | In a neighbour's country polygon | `OUT_NEIGHBOUR_<ISO3>` | 9,103,394 |
| | · Laos | | 3,556,924 |
| | · Cambodia | | 2,756,018 |
| | · Myanmar | | 2,330,080 |
| | · Vietnam | | 414,837 |
| | · Malaysia | | 45,535 |
| 4 | In no country polygon, flagged `type_offshore` (expected) | `OUT_OFFSHORE_FLAGGED` | 45,756 |
| 5 | In no polygon, ≤ 1 km from Thailand: coastline/border miss | `OUT_GAP_THAILAND` | 58 |
| 6 | In no polygon, ≤ 1 km from a neighbour | `OUT_GAP_NEIGHBOUR` | 273 |
| 7 | Anything else | `OUT_OPEN_SEA` | 7,986 |
| | **Total** | | **13,576,064** |

Why it is done this way:
- **Province and country come from the district's GID**, not from separate joins.
  That keeps the hierarchy consistent: a point can't land in a district of one
  province and in another province.
- **`ST_Intersects`, not `ST_Contains`:** `ST_Contains` is false for a point on an
  edge, so points on the internal cut lines of subdivided pieces would be lost. A
  point on a border shared by two districts goes to the lowest GID_2, so reruns
  give the same result.
- **1 km gap threshold:** the largest pixel size (MODIS). GADM's coastline is
  generalised, so a coastal fire can fall just outside the polygon. Only 58
  detections are such Thai coastline/border misses.
- **`OUT_OPEN_SEA` (7,986):** mostly NRT rows (NOAA-21), which have no `type` column,
  so offshore gas flares there can't be identified as `type_offshore` (see the item 2
  limitation). Reruns of this step must follow every rerun of the cleaning step; the
  table stores `clean_run_id` for that check.

In Thailand per sensor:

| Sensor | Detections | In Thailand | % |
|---|---:|---:|---:|
| MODIS_SP | 1,365,550 | 359,958 | 26.4 |
| VIIRS_SNPP_SP | 7,201,455 | 2,421,438 | 33.6 |
| VIIRS_NOAA20_SP | 3,870,103 | 1,267,658 | 32.8 |
| VIIRS_NOAA21_NRT | 1,132,501 | 367,409 | 32.4 |
| MODIS_NRT | 522 | 119 | 22.8 |
| VIIRS_SNPP_NRT | 2,861 | 1,047 | 36.6 |
| VIIRS_NOAA20_NRT | 3,072 | 968 | 31.5 |
| **Total** | **13,576,064** | **4,418,597** | **32.5** |

## Spatial join benchmark

| Measurement | Value |
|---|---:|
| Full spatial join, 13,576,064 points (district join 204.9 s + country join for the 9.16M non-district points 206.0 s) | **410.9 s** |
| Whole location job incl. classification, validation and metrics | 524 s |
| One year (2020, 1,389,622 points), whole district polygons | 35.7 s |
| Same year, `ST_Subdivide` pieces (≤ 256 vertices) | 21.7 s |
| **Speed-up from subdividing** | **1.6×** |

Both methods put exactly the same 472,284 points (2020) in a district. The
speed-up is smaller than often reported for `ST_Subdivide`. Two likely reasons,
not tested separately: PostGIS caches a prepared version of a polygon that is
tested repeatedly, which helps whole polygons; and about two thirds of the points
are outside Thailand, where the GIST bounding-box test rejects them cheaply with
either method.

## Step 3: validation against NASA's Thailand archive

NASA publishes a FIRMS archive already clipped to Thailand
(`raw/firms_country`, 3,855,606 rows). Compared like for like: SP sensors only,
from our first SP date per sensor (2012-01-20; NOAA-20 2018-04-01) to 2024-12-31,
flagged rows included. Detections are matched row by row on the same
`detection_id` key. "Ours incl. dups" adds back the rule A duplicates (item 2)
that fall in Thailand, because NASA's archive still contains them.

| Sensor | Years | NASA | Ours | Ours incl. dups | Diff | Diff % | Matched | NASA-Thai, ours outside | NASA only | Ours only |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| MODIS_SP | 2012–2024 | 320,881 | 319,425 | 319,425 | −1,456 | −0.45 | 317,125 | 3,756 | 0 | 2,300 |
| VIIRS_NOAA20_SP | 2018–2024 | 1,014,648 | 1,009,748 | 1,009,803 | −4,845 | −0.48 | 1,002,302 | 12,291 | 0 | 7,446 |
| VIIRS_SNPP_SP | 2012–2024 | 2,188,802 | 2,178,457 | 2,178,532 | −10,270 | −0.47 | 2,163,495 | 25,229 | 0 | 14,962 |
| **Total** | | **3,524,331** | **3,507,630** | **3,507,760** | **−16,571** | **−0.47** | **3,482,922** | **41,276** | **0** | **24,708** |

Per year, the difference lies between −0.11% and −0.84% for every sensor (full
table: `meta.location_validation`, run 11).

**What the differences are:**

1. **NASA only = 0.** Every detection in NASA's archive is also in our data. The
   extraction and cleaning lost nothing; all differences come from the boundary.
2. **Different borders.** NASA uses its own Thailand boundary, not GADM's, and the
   two disagree along the border:
   - 41,276 NASA-Thailand detections fall outside our GADM Thailand: 36,488 in
     Myanmar, 2,314 in Laos, 2,044 in Cambodia, 381 in Malaysia, and 49 offshore,
     coastal gap or sea.
   - 24,708 of our Thailand detections are not in NASA's archive.
   - Both groups hug the border: median distance to the GADM border is 844 m and
     755 m, and 99% are within 2.7 km and 6.2 km.
   - Most are on the Thai–Myanmar border, the longest one, which runs through
     mountains and rivers where boundary datasets often differ by a pixel or two.
3. **Two areas further inside.** 453 of our Thailand detections that NASA leaves
   out are more than 5 km from the border:
   - 414 in Chat Trakan (Phitsanulok) and Na Haeo (Loei), on the Thai–Lao border,
     where Chat Trakan includes the disputed Ban Rom Klao area.
   - 39 on islands in Songkhla Lake (Pak Phayun, Krasae Sinthu), which NASA's
     boundary appears to treat as water.
4. **Net effect: −0.47%.** With GADM we count slightly fewer Thai fires than NASA.
   GADM is kept because it provides the province/district hierarchy that NASA's
   country clip doesn't.

**MODIS 6.2 vs 6.03 (2012–2017).** NASA's country archive labels its MODIS rows
for 2012–2017 as version `6.2`, while our SP data says `6.03`. From 2018 on both
say `6.03`, then `61.03`. Before the comparison this looked like a different
processing collection, which would make MODIS 2012–2017 not like for like. The
row-level match shows otherwise: all 168,421 NASA MODIS rows for 2012–2017 match
one of our detections on satellite, minute and position, with **identical FRP,
brightness and confidence**. The difference is only in the version label;
2012–2017 are compared like for like.

## Step 4: location dimension (design change)

The existing `dds.dim_location_spatial` (03) has **one row per point**
(lat/lon/geom + province and district names). We have 13,574,639 distinct
locations for 13,576,064 detections, so that dimension would be as large as the
fact table: no compression, and every query would pay for a 13.5M-row join.

New table: **`dds.dim_location`, at district grain**:

| | Members |
|---|---:|
| Thai districts (incl. 2 water-body districts), each with its province and country | 928 |
| `THA_NO_DISTRICT` (Thailand, district unknown) | 1 |
| `OUT_NEIGHBOUR_MMR` / `LAO` / `KHM` / `MYS` / `VNM` | 5 |
| `OUT_OFFSHORE_FLAGGED`, `OUT_GAP_THAILAND`, `OUT_GAP_NEIGHBOUR`, `OUT_OPEN_SEA` | 4 |
| **Total** | **938** |

- **Columns:** district GID / name / Thai name / type, province GID / name / Thai
  name, country code / name, `in_thailand`, `outside_class`.
- **Roll-up:** district → province → country is a `GROUP BY` on the dimension.
- **Drill-down:** to the individual fire goes through the fact row, which keeps
  the point (`latitude`, `longitude`, `geom`) and `detection_id` (the degenerate
  dimension).
- **Stable keys:** `location_key` is upserted on `location_code`, so it doesn't
  change on reruns and fact rows can safely reference it.
- **Fact mapping:** `stage.fire_detection_location.location_code` maps each
  detection to its member when the fact table is loaded.
- **Old table:** `dds.dim_location_spatial` is empty and kept for now, to be
  dropped in a later cleanup together with the old `stg_*` tables.

## Metrics

- `meta.dq_metrics` (run 11): one `loc_*` row per location class and sensor.
- `meta.location_validation` (run 11): the validation table per sensor and year.
- `meta.event_log`: runs 9 (reference load, 33 s), 10 (benchmark) and 11
  (location job: spatial join 410.9 s, total 524 s).

```sql
SELECT src_sensor, year, nasa_rows, ours_incl_rule_a_dups, diff_incl_dups,
       matched_ids, matched_ours_outside, nasa_only_ids, ours_only_ids
FROM meta.location_validation
WHERE run_id = (SELECT max(run_id) FROM meta.location_validation)
ORDER BY src_sensor, year;
```
