"""
Clean, deduplicate and gap-fill the staged NDVI samples (missing-value imputation).

    stage.raw_ndvi  (all TEXT, from load_ndvi.py)
        -> stage.ndvi_point            one row per grid point: location, data class, counts
        -> stage.ndvi_clean            one row per point x 16-day composite; `ndvi` has no gaps
        -> meta.ndvi_imputation_eval   error of each method on held-out observations
        -> meta.dq_metrics             before/after counts per rule (steps 701-709)

Tables: init-scripts/12_ndvi.sql. Docs: docs/etl_ndvi.md. Rebuilt in ONE
transaction (truncate + insert): a rerun gives the same result, and a failed
check rolls everything back. Needs the GADM reference tables (load_reference.py).

Rules:
  1  safe type casting     a row without a usable point id / date / coordinate is dropped and counted
  2  duplicates            one row per (point, composite); adjacent AppEEARS requests repeat
                           their boundary composite. The row from the newer request is kept.
  3  observation class     from pixel_reliability: good 0, marginal 1, snow_ice 2, cloud 3,
                           fill -1 (or NDVI = -3000); anything else, or NDVI outside
                           [-0.2, 1], is invalid. Only good and marginal are used as observed.
  4  point class           no_data: fill in every composite (sea), left out of ndvi_clean
                           sparse:  fewer than --min-valid usable composites, not imputed
                           land:    imputed
  5  imputation            seasonal anomaly, see below
  6  evaluation            usable observations are hidden, imputed and compared

Imputation (rule 5). For every land point:
    seasonal mean   mean of its usable values in the same composite-of-year (23 per year)
                    and the one before and after it, over all years: a 48-day window.
                    One composite-of-year alone often has too few usable values in the
                    rainy season (2 or fewer for 15% of the points).
    anomaly         usable value - seasonal mean
    fill            seasonal mean + anomaly carried in from the nearest usable composite
                    before and after the gap, weighted by closeness, each multiplied by
                    PHI ** (composites away)
So a one-composite gap is close to linear interpolation, and a long rainy-season gap
falls back to the point's own seasonal mean. Observed values are never changed.

Evaluation (rule 6). Random single cells would be too easy: real gaps are runs,
mostly in the rainy season. Each point therefore borrows the gap pattern of another
point: 30% of that point's gap runs are laid over this point, and the usable
observations under them are hidden. The seasonal means are recomputed without the
hidden values. Four methods are scored: locf (last value carried forward), linear
interpolation in time, seasonal_mean, and seasonal_anomaly (the one used).

Usage:
    python controllers/impute_ndvi.py
    python controllers/impute_ndvi.py --sweep     # hold-out error for several PHI; writes nothing
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
import time

import numpy as np
import pandas as pd
import psycopg2

import db_limits

DB = dict(
    host=os.getenv("DWH_HOST", "localhost"),
    port=int(os.getenv("DWH_PORT", "5432")),
    dbname=os.getenv("DWH_DB", "fire_dwh"),
    user=os.getenv("DWH_USER", "dwh_admin"),
    password=os.getenv("DWH_PASSWORD", "admin123"),
)

PHI = 0.7                 # anomaly kept per composite of distance (chosen with --sweep, docs/etl_ndvi.md)
MIN_VALID = 23            # usable composites a point needs to be imputed: one year's worth
SLOTS_PER_YEAR = 23       # MOD13Q1 composites start on day of year 1, 17, 33, ...
SEASON_WINDOW = 1         # composites-of-year pooled on each side for the seasonal mean
NDVI_MIN, NDVI_MAX = -0.2, 1.0
FILL_VALUE = -3000
SEED = 42
EVAL_RUN_SHARE = 0.3      # share of the donor point's gap runs laid over a point
GAP_BINS = [0, 1, 2, 4, 8, 16, 10_000]
GAP_LABELS = ["gap 1", "gap 2", "gap 3-4", "gap 5-8", "gap 9-16", "gap 17+"]
CLASSES = ["good", "marginal", "snow_ice", "cloud", "fill", "invalid"]
USED = "seasonal_anomaly"

# 1 km gap test at the coast, as in locate_stage.py.
GAP_M = 1000
GAP_DEG = 0.01


# ---------------------------------------------------------------- SQL: rules 1-4

def prepare_sql(min_valid: int) -> list[tuple[str, str]]:
    return [
        ("session settings", db_limits.session_sql(maintenance=True)),

        ("rules 1-3: cast, rank duplicates, classify", f"""
            CREATE TEMP TABLE t_ndvi ON COMMIT DROP AS
            WITH c AS (
                SELECT nullif(id, '')                       AS point_id,
                       stage.try_date(date)                 AS composite_date,
                       stage.try_numeric(latitude)          AS latitude,
                       stage.try_numeric(longitude)         AS longitude,
                       modis_tile,
                       stage.try_numeric(ndvi)              AS ndvi_num,
                       stage.try_numeric(pixel_reliability) AS rel_num,
                       stage.try_numeric(vi_quality)        AS viq_num,
                       source_file
                  FROM stage.raw_ndvi),
            k AS (
                SELECT c.*,
                       (point_id IS NOT NULL AND composite_date IS NOT NULL
                        AND latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180) AS keyed,
                       -- the newest composite in a file tells which request is the newer one
                       max(composite_date) OVER (PARTITION BY source_file) AS file_last_date
                  FROM c)
            SELECT k.*,
                   row_number() OVER (PARTITION BY point_id, composite_date
                                      ORDER BY file_last_date DESC, source_file) AS dup_rank,
                   (min(ndvi_num) OVER pd IS DISTINCT FROM max(ndvi_num) OVER pd
                    OR min(rel_num) OVER pd IS DISTINCT FROM max(rel_num) OVER pd) AS dup_conflict,
                   CASE WHEN rel_num = -1 OR ndvi_num = {FILL_VALUE}           THEN 'fill'
                        WHEN ndvi_num IS NULL OR rel_num IS NULL
                             OR ndvi_num NOT BETWEEN {NDVI_MIN} AND {NDVI_MAX} THEN 'invalid'
                        WHEN rel_num = 0 THEN 'good'
                        WHEN rel_num = 1 THEN 'marginal'
                        WHEN rel_num = 2 THEN 'snow_ice'
                        WHEN rel_num = 3 THEN 'cloud'
                        ELSE 'invalid' END AS obs_class
              FROM k
            WINDOW pd AS (PARTITION BY point_id, composite_date);
        """),

        ("rule 4: points", f"""
            CREATE TEMP TABLE t_point ON COMMIT DROP AS
            SELECT point_id,
                   min(latitude) AS latitude, min(longitude) AS longitude, min(modis_tile) AS modis_tile,
                   count(DISTINCT (latitude, longitude)) AS n_coords,
                   count(*) AS n_composites,
                   count(*) FILTER (WHERE obs_class IN ('good', 'marginal')) AS n_valid,
                   count(*) FILTER (WHERE obs_class = 'cloud') AS n_cloud,
                   count(*) FILTER (WHERE obs_class = 'fill')  AS n_fill,
                   CASE WHEN count(*) FILTER (WHERE obs_class <> 'fill') = 0 THEN 'no_data'
                        WHEN count(*) FILTER (WHERE obs_class IN ('good', 'marginal')) < {min_valid} THEN 'sparse'
                        ELSE 'land' END AS point_class
              FROM t_ndvi
             WHERE keyed AND dup_rank = 1
             GROUP BY point_id;
        """),

        # Rows kept for ndvi_clean, with the matrix position (pi, ti) of each.
        ("rows of points with data", f"""
            CREATE TEMP TABLE t_keep ON COMMIT DROP AS
            SELECT n.point_id, n.composite_date,
                   (dense_rank() OVER (ORDER BY n.point_id) - 1)::int       AS pi,
                   (dense_rank() OVER (ORDER BY n.composite_date) - 1)::int AS ti,
                   CASE WHEN n.obs_class NOT IN ('fill', 'invalid') THEN n.ndvi_num END AS ndvi_observed,
                   CASE WHEN n.rel_num BETWEEN -1 AND 3 AND n.rel_num = trunc(n.rel_num)
                        THEN n.rel_num::smallint END AS pixel_reliability,
                   CASE WHEN n.viq_num BETWEEN 0 AND 65535 AND n.viq_num = trunc(n.viq_num)
                        THEN n.viq_num::int END AS vi_quality,
                   n.obs_class,
                   n.obs_class IN ('good', 'marginal') AS is_valid,
                   p.point_class,
                   n.source_file
              FROM t_ndvi n
              JOIN t_point p USING (point_id)
             WHERE n.keyed AND n.dup_rank = 1 AND p.point_class <> 'no_data';
        """),
    ]


def location_sql(run_id: int) -> str:
    """Rules of locate_stage.py (district, Thailand, neighbour, 1 km coast gap, open sea)."""
    return f"""
        INSERT INTO stage.ndvi_point
               (point_id, latitude, longitude, geom, modis_tile, location_code, in_thailand,
                point_class, n_composites, n_valid, n_cloud, n_fill, n_imputed, run_id)
        SELECT x.point_id, x.latitude, x.longitude, x.geom, x.modis_tile, x.location_code, dl.in_thailand,
               x.point_class, x.n_composites, x.n_valid, x.n_cloud, x.n_fill,
               coalesce(i.n_imputed, 0), {run_id}
          FROM (
            SELECT p.*, g.geom,
                   CASE
                     WHEN d.gid_2 IS NOT NULL THEN d.gid_2
                     WHEN k.gid_0 = 'THA'     THEN 'THA_NO_DISTRICT'
                     WHEN k.gid_0 IS NOT NULL THEN 'OUT_NEIGHBOUR_' || k.gid_0
                     WHEN EXISTS (SELECT 1 FROM stage.gadm_country_piece c
                                   WHERE c.gid_0 = 'THA'
                                     AND ST_DWithin(c.geom, g.geom, {GAP_DEG})
                                     AND ST_DWithin(c.geom::geography, g.geom::geography, {GAP_M}))
                                              THEN 'OUT_GAP_THAILAND'
                     WHEN EXISTS (SELECT 1 FROM stage.gadm_country_piece c
                                   WHERE c.gid_0 <> 'THA'
                                     AND ST_DWithin(c.geom, g.geom, {GAP_DEG})
                                     AND ST_DWithin(c.geom::geography, g.geom::geography, {GAP_M}))
                                              THEN 'OUT_GAP_NEIGHBOUR'
                     ELSE 'OUT_OPEN_SEA'
                   END AS location_code
              FROM t_point p
             CROSS JOIN LATERAL (SELECT ST_SetSRID(ST_MakePoint(p.longitude, p.latitude), 4326) AS geom) g
              LEFT JOIN LATERAL (SELECT c.gid_2 FROM stage.gadm_district_piece c
                                  WHERE ST_Intersects(c.geom, g.geom)
                                  ORDER BY c.gid_2 LIMIT 1) d ON true
              LEFT JOIN LATERAL (SELECT c.gid_0 FROM stage.gadm_country_piece c
                                  WHERE d.gid_2 IS NULL AND ST_Intersects(c.geom, g.geom)
                                  ORDER BY (c.gid_0 <> 'THA'), c.gid_0 LIMIT 1) k ON true
          ) x
          -- in_thailand comes from the dimension, as for the fire detections
          JOIN dds.dim_location dl ON dl.location_code = x.location_code
          LEFT JOIN (SELECT point_id, count(*) AS n_imputed
                       FROM stage.ndvi_clean WHERE is_imputed GROUP BY 1) i USING (point_id);
    """


# ---------------------------------------------------------------- numpy: rules 5-6

def runs_of(mask_row: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Start and end (exclusive) of every run of True."""
    edges = np.flatnonzero(np.diff(np.concatenate(([0], mask_row.astype(np.int8), [0]))))
    return edges[::2], edges[1::2]


def run_lengths(mask: np.ndarray) -> np.ndarray:
    """For every True cell, the length of the run of True it is in."""
    out = np.zeros(mask.shape, dtype=np.int32)
    for p in range(mask.shape[0]):
        for a, b in zip(*runs_of(mask[p])):
            out[p, a:b] = b - a
    return out


def seasonal_mean(Y: np.ndarray, slot: np.ndarray) -> np.ndarray:
    """Per point, the mean of the usable values in each composite-of-year and its
    SEASON_WINDOW neighbours on each side (around the year end); returned per cell."""
    total = np.zeros((Y.shape[0], SLOTS_PER_YEAR))
    count = np.zeros((Y.shape[0], SLOTS_PER_YEAR))
    for s in range(SLOTS_PER_YEAR):
        cols = Y[:, slot == s]
        total[:, s] = np.nansum(cols, axis=1)
        count[:, s] = (~np.isnan(cols)).sum(axis=1)
    shifts = range(-SEASON_WINDOW, SEASON_WINDOW + 1)
    total = sum(np.roll(total, k, axis=1) for k in shifts)
    count = sum(np.roll(count, k, axis=1) for k in shifts)
    C = np.where(count > 0, total / np.maximum(count, 1), np.nan)
    # Still nothing usable in the whole window (always cloudy): interpolate between the
    # nearest composites-of-year that have a mean, around the year end.
    for p in np.flatnonzero(np.isnan(C).any(axis=1) & ~np.isnan(C).all(axis=1)):
        ok = ~np.isnan(C[p])
        xs = np.flatnonzero(ok)
        C[p] = np.interp(np.arange(SLOTS_PER_YEAR),
                         np.concatenate((xs - SLOTS_PER_YEAR, xs, xs + SLOTS_PER_YEAR)), np.tile(C[p, ok], 3))
    return C[:, slot]


def fill_seasonal_anomaly(Y: np.ndarray, slot: np.ndarray, phi: float) -> np.ndarray:
    """Rule 5. Returns a full matrix; usable cells keep their value."""
    clim = seasonal_mean(Y, slot)
    anomaly = Y - clim
    steps = np.arange(Y.shape[1])
    out = np.full(Y.shape, np.nan)
    for p in range(Y.shape[0]):
        ok = ~np.isnan(anomaly[p])
        if not ok.any():
            continue
        x, a = steps[ok], anomaly[p, ok]
        j = np.searchsorted(x, steps)                    # first usable composite at or after each step
        left, right = np.clip(j - 1, 0, len(x) - 1), np.clip(j, 0, len(x) - 1)
        d_left = np.where(j > 0, steps - x[left], np.inf)
        d_right = np.where(j < len(x), x[right] - steps, np.inf)
        with np.errstate(invalid="ignore"):
            w_right = np.where(np.isinf(d_left), 1.0,
                               np.where(np.isinf(d_right), 0.0, d_left / (d_left + d_right)))
        carried = ((1 - w_right) * a[left] * phi ** np.where(np.isinf(d_left), 0, d_left)
                   + w_right * a[right] * phi ** np.where(np.isinf(d_right), 0, d_right))
        out[p] = clim[p] + carried
    out = np.clip(out, NDVI_MIN, NDVI_MAX)
    return np.where(np.isnan(Y), out, Y)


def fill_linear(Y: np.ndarray) -> np.ndarray:
    steps = np.arange(Y.shape[1])
    out = Y.copy()
    for p in range(Y.shape[0]):
        ok = ~np.isnan(Y[p])
        if ok.any():
            out[p] = np.interp(steps, steps[ok], Y[p, ok])
    return out


def fill_locf(Y: np.ndarray) -> np.ndarray:
    """Last usable value carried forward; before the first one, the first is carried back."""
    T = Y.shape[1]
    ok = ~np.isnan(Y)
    last = np.maximum.accumulate(np.where(ok, np.arange(T), -1), axis=1)
    nxt = np.minimum.accumulate(np.where(ok, np.arange(T), T)[:, ::-1], axis=1)[:, ::-1]
    src = np.where(last >= 0, last, np.minimum(nxt, T - 1))
    return np.take_along_axis(Y, src, axis=1)


def holdout(valid: np.ndarray, rng: np.random.Generator) -> np.ndarray:
    """Cells to hide: each point gets a share of another point's gap runs laid over it."""
    P = valid.shape[0]
    order = rng.permutation(P)
    donor = np.empty(P, dtype=int)
    donor[order] = np.roll(order, -1)
    mask = np.zeros(valid.shape, dtype=bool)
    for p in range(P):
        starts, ends = runs_of(~valid[donor[p]])
        take = rng.random(len(starts)) < EVAL_RUN_SHARE
        for a, b in zip(starts[take], ends[take]):
            mask[p, a:b] = True
    return mask & valid


def evaluate(Y: np.ndarray, slot: np.ndarray, seasons: np.ndarray, phis: list[float]) -> tuple[pd.DataFrame, dict]:
    """Hide usable observations, impute, compare. One row per method and slice."""
    valid = ~np.isnan(Y)
    mask = holdout(valid, np.random.default_rng(SEED))
    Yt = np.where(mask, np.nan, Y)
    truth = Y[mask]
    gap = pd.cut(run_lengths(np.isnan(Yt))[mask], GAP_BINS, labels=GAP_LABELS).astype(str)
    season = np.broadcast_to(seasons, Y.shape)[mask]

    # Hidden cells sit in longer gaps than real ones (they merge with the point's own gaps).
    # all_real_gap_mix weights the gap classes by their share of the real missing cells.
    real = pd.cut(run_lengths(~valid)[~valid], GAP_BINS, labels=GAP_LABELS).astype(str)
    real_share = pd.Series(real).value_counts(normalize=True)

    methods = {"locf": fill_locf(Yt), "linear": fill_linear(Yt), "seasonal_mean": seasonal_mean(Yt, slot)}
    if len(phis) == 1:
        methods[USED] = fill_seasonal_anomaly(Yt, slot, phis[0])
    else:
        methods.update({f"{USED} phi={phi}": fill_seasonal_anomaly(Yt, slot, phi) for phi in phis})

    rows = []
    for name, filled in methods.items():
        err = filled[mask] - truth
        ok = ~np.isnan(err)                      # a point with nothing usable left cannot be filled
        slices = [("all", ok)]
        slices += [(g, ok & (gap == g)) for g in GAP_LABELS]
        slices += [(f"season {s}", ok & (season == s)) for s in sorted(set(season))]
        mse_by_gap = {}
        for label, sel in slices:
            if not sel.any():
                continue
            e = err[sel]
            rows.append(dict(method=name, slice=label, n_heldout=int(sel.sum()),
                             rmse=float(np.sqrt(np.mean(e ** 2))), mae=float(np.abs(e).mean()),
                             bias=float(e.mean())))
            if label in GAP_LABELS:
                mse_by_gap[label] = (np.mean(e ** 2), np.abs(e).mean(), e.mean())
        w = real_share.reindex(list(mse_by_gap)).fillna(0)
        w = w / w.sum()
        rows.append(dict(method=name, slice="all_real_gap_mix", n_heldout=int(ok.sum()),
                         rmse=float(np.sqrt(sum(w[g] * v[0] for g, v in mse_by_gap.items()))),
                         mae=float(sum(w[g] * v[1] for g, v in mse_by_gap.items())),
                         bias=float(sum(w[g] * v[2] for g, v in mse_by_gap.items()))))
    info = dict(heldout=int(mask.sum()), heldout_share_of_valid=round(float(mask.sum() / valid.sum()), 4),
                heldout_std=round(float(truth.std()), 4))
    return pd.DataFrame(rows), info


# ---------------------------------------------------------------- main

def load_matrix(cur) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """t_keep as point x composite matrices: Y (usable values, NaN elsewhere), delivered values,
    class, plus the land rows and the composite-of-year and Thai season of every column.
    A cell with class -1 has no row (a composite not delivered yet for that point)."""
    cur.execute("""SELECT k.ti, k.composite_date, coalesce(d.thai_season, 'unknown')
                     FROM (SELECT DISTINCT ti, composite_date FROM t_keep) k
                     LEFT JOIN dds.dim_date d ON d.full_date = k.composite_date
                    ORDER BY k.ti""")
    date_rows = cur.fetchall()
    dates = pd.to_datetime([r[1] for r in date_rows])
    slot = np.minimum((dates.day_of_year.to_numpy() - 1) // 16, SLOTS_PER_YEAR - 1)
    seasons = np.array([r[2] for r in date_rows])

    buf = io.StringIO()
    cur.copy_expert(f"""COPY (SELECT pi, ti, ndvi_observed,
                                    array_position(ARRAY{CLASSES}, obs_class) - 1,
                                    (point_class = 'land')::int
                               FROM t_keep) TO STDOUT WITH (FORMAT csv)""", buf)
    buf.seek(0)
    df = pd.read_csv(buf, header=None, names=["pi", "ti", "v", "cls", "land"],
                     dtype={"pi": np.int32, "ti": np.int32, "v": np.float64, "cls": np.int8, "land": np.int8})
    P, T = int(df.pi.max()) + 1, len(dates)
    pi, ti = df.pi.to_numpy(), df.ti.to_numpy()
    delivered = np.full((P, T), np.nan)
    cls = np.full((P, T), -1, dtype=np.int8)
    delivered[pi, ti] = df.v.to_numpy()
    cls[pi, ti] = df.cls.to_numpy()
    usable = (cls == CLASSES.index("good")) | (cls == CLASSES.index("marginal"))
    Y = np.where(usable, delivered, np.nan)
    land = np.zeros(P, dtype=bool)
    land[pi[df.land.to_numpy() == 1]] = True
    return Y, delivered, cls, land, slot, seasons


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--phi", type=float, default=PHI,
                    help=f"share of the anomaly kept per composite of distance (default {PHI})")
    ap.add_argument("--min-valid", type=int, default=MIN_VALID,
                    help=f"usable composites a point needs to be imputed (default {MIN_VALID})")
    ap.add_argument("--sweep", action="store_true",
                    help="print the hold-out error for several values of phi and write nothing")
    args = ap.parse_args()
    if not 0 <= args.phi <= 1:
        ap.error("--phi must be between 0 and 1")
    params = dict(phi=args.phi, min_valid=args.min_valid, seed=SEED, eval_run_share=EVAL_RUN_SHARE)

    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    cur.execute("SELECT (SELECT count(*) FROM stage.raw_ndvi), (SELECT count(*) FROM stage.gadm_district_piece)")
    n_raw, n_pieces = cur.fetchone()
    conn.rollback()
    if not n_raw:
        raise SystemExit("stage.raw_ndvi is empty: run controllers/load_ndvi.py first")
    if not n_pieces:
        raise SystemExit("stage.gadm_district_piece is empty: run controllers/load_reference.py first")

    if args.sweep:
        for _, sql in prepare_sql(args.min_valid):
            cur.execute(sql)
        Y, _, _, land, slot, seasons = load_matrix(cur)
        conn.rollback()                               # temp tables only; nothing written
        conn.close()
        table, info = evaluate(Y[land], slot, seasons, [0.0, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0])
        print(f"hold-out: {info}\nRMSE by method and slice:")
        print(table.pivot(index="method", columns="slice", values="rmse")
                   [["all_real_gap_mix", "all"] + GAP_LABELS].round(4).to_string())
        return 0

    cur.execute("""INSERT INTO meta.event_log (event_type, event_category, status, note)
                   VALUES ('STAGE_NDVI_IMPUTE', 'STAGE', 'RUNNING', %s) RETURNING log_key""",
                (json.dumps(params),))
    run_id = cur.fetchone()[0]
    conn.commit()
    print(f"run {run_id}: {params} ({db_limits.describe()})")
    t0 = time.time()

    def step(name: str, t: float) -> None:
        print(f"  {name:<46} {time.time() - t:6.1f}s")

    try:
        for name, sql in prepare_sql(args.min_valid):
            t = time.time()
            cur.execute(sql)
            step(name, t)

        cur.execute("""SELECT count(*), count(*) FILTER (WHERE NOT keyed),
                              count(*) FILTER (WHERE keyed AND dup_rank > 1),
                              count(*) FILTER (WHERE keyed AND dup_rank > 1 AND dup_conflict)
                         FROM t_ndvi""")
        n_in, n_unkeyed, n_dup, n_dup_conflict = cur.fetchone()
        cur.execute("""SELECT count(*), coalesce(sum(n_composites), 0), coalesce(max(n_coords), 0)
                         FROM t_point WHERE point_class = 'no_data'""")
        n_nodata_points, n_nodata_rows, _ = cur.fetchone()
        n_nodata_rows = int(n_nodata_rows)               # sum() comes back as Decimal
        cur.execute("SELECT count(*), coalesce(max(n_coords), 0) FROM t_point")
        n_points, max_coords = cur.fetchone()

        # ---- rule 5: impute
        t = time.time()
        Y, delivered, cls, land, slot, seasons = load_matrix(cur)
        filled = np.full(Y.shape, np.nan)
        filled[land] = fill_seasonal_anomaly(Y[land], slot, args.phi)
        missing = (cls >= 0) & np.isnan(Y)
        gap_len = run_lengths(np.isnan(Y))
        step(f"rule 5: impute {int(land.sum()):,} points x {Y.shape[1]} composites", t)

        # ---- rule 6: evaluate on the land points
        t = time.time()
        table, eval_info = evaluate(Y[land], slot, seasons, [args.phi])
        step(f"rule 6: evaluate on {eval_info['heldout']:,} hidden observations", t)

        # Cloudy composites read low: what the delivered value was against what replaces it.
        cloudy = (cls == CLASSES.index("cloud")) & land[:, None]
        cloud_shift = float(np.nanmean(delivered[cloudy] - filled[cloudy])) if cloudy.any() else 0.0

        # ---- write
        t = time.time()
        pi, ti = np.nonzero(missing)
        fill_df = pd.DataFrame({"pi": pi, "ti": ti, "ndvi": np.round(filled[pi, ti], 4), "gap_length": gap_len[pi, ti]})
        buf = io.StringIO()
        fill_df.to_csv(buf, index=False, header=False, float_format="%.4f")
        buf.seek(0)
        cur.execute("CREATE TEMP TABLE t_fill (pi int, ti int, ndvi numeric(6,4), gap_length smallint) ON COMMIT DROP")
        cur.copy_expert("COPY t_fill (pi, ti, ndvi, gap_length) FROM STDIN WITH (FORMAT csv)", buf)
        cur.execute(f"""
            ANALYZE t_fill;
            TRUNCATE stage.ndvi_clean, stage.ndvi_point;
            ALTER TABLE stage.ndvi_clean DROP CONSTRAINT IF EXISTS ndvi_clean_pkey;
            DROP INDEX IF EXISTS stage.idx_ndvi_clean_date;
            INSERT INTO stage.ndvi_clean
                   (point_id, composite_date, ndvi_observed, pixel_reliability, vi_quality, obs_class,
                    is_valid, ndvi, is_imputed, gap_length, source_file, run_id)
            SELECT k.point_id, k.composite_date, k.ndvi_observed, k.pixel_reliability, k.vi_quality,
                   k.obs_class, k.is_valid,
                   CASE WHEN k.is_valid THEN k.ndvi_observed ELSE f.ndvi END,
                   (NOT k.is_valid AND f.ndvi IS NOT NULL), f.gap_length, k.source_file, {run_id}
              FROM t_keep k
              LEFT JOIN t_fill f USING (pi, ti)
             ORDER BY k.point_id, k.composite_date;
            -- Re-adding the primary key also proves rule 2 left one row per point and composite.
            ALTER TABLE stage.ndvi_clean ADD CONSTRAINT ndvi_clean_pkey PRIMARY KEY (point_id, composite_date);
            CREATE INDEX idx_ndvi_clean_date ON stage.ndvi_clean (composite_date);
            ANALYZE stage.ndvi_clean;
        """)
        cur.execute(location_sql(run_id))
        cur.execute("ANALYZE stage.ndvi_point")
        step("write ndvi_clean, ndvi_point", t)

        table.insert(0, "run_id", run_id)
        buf = io.StringIO()
        table.to_csv(buf, index=False, header=False, float_format="%.6f")
        buf.seek(0)
        cur.copy_expert("COPY meta.ndvi_imputation_eval (run_id, method, slice, n_heldout, rmse, mae, bias) "
                        "FROM STDIN WITH (FORMAT csv)", buf)

        # ---- checks: any failure rolls the whole job back
        cur.execute("""SELECT count(*),
                              count(*) FILTER (WHERE is_valid),
                              count(*) FILTER (WHERE is_imputed),
                              count(*) FILTER (WHERE ndvi IS NULL),
                              count(*) FILTER (WHERE is_valid AND ndvi IS DISTINCT FROM ndvi_observed),
                              count(*) FILTER (WHERE ndvi NOT BETWEEN %s AND %s),
                              count(*) FILTER (WHERE NOT is_valid AND gap_length IS NULL),
                              count(*) FILTER (WHERE obs_class = 'fill'),
                              count(*) FILTER (WHERE obs_class = 'cloud'),
                              count(*) FILTER (WHERE obs_class IN ('snow_ice', 'invalid')),
                              sum(hashtextextended(point_id || '|' || composite_date || '|'
                                                   || coalesce(ndvi::text, '') || '|' || is_imputed, 0)::numeric)
                         FROM stage.ndvi_clean""", (NDVI_MIN, NDVI_MAX))
        (n_clean, n_valid, n_imputed, n_null, n_changed, n_range, n_nogap,
         n_fill, n_cloud, n_other, checksum) = cur.fetchone()
        cur.execute("""SELECT count(*) FROM stage.ndvi_clean c JOIN stage.ndvi_point p USING (point_id)
                        WHERE c.ndvi IS NULL AND p.point_class = 'land'""")
        n_null_land = cur.fetchone()[0]
        cur.execute("""SELECT count(*), count(*) FILTER (WHERE point_class = 'land'),
                              count(*) FILTER (WHERE point_class = 'sparse'),
                              count(*) FILTER (WHERE in_thailand AND point_class = 'land')
                         FROM stage.ndvi_point""")
        n_point_rows, n_land, n_sparse, n_land_thailand = cur.fetchone()
        n_missing = n_clean - n_valid
        checks = [
            ("every raw row is clean, a duplicate, on a no-data point or unusable",
             n_clean + n_dup + n_nodata_rows + n_unkeyed == n_in == n_raw,
             f"{n_clean:,} + {n_dup:,} + {n_nodata_rows:,} + {n_unkeyed:,} = {n_in:,} / {n_raw:,} raw"),
            ("one coordinate per point id", max_coords <= 1, f"max {max_coords}"),
            ("every point has a location", n_point_rows == n_points, f"{n_point_rows:,} / {n_points:,}"),
            ("land points: no missing value left", n_null_land == 0, f"{n_null_land:,} missing"),
            ("imputed = missing on land points", n_imputed == int((missing & land[:, None]).sum()) == n_missing - n_null,
             f"{n_imputed:,} / {int((missing & land[:, None]).sum()):,} / {n_missing - n_null:,}"),
            ("observed values unchanged", n_changed == 0, f"{n_changed:,} changed"),
            (f"all values in [{NDVI_MIN}, {NDVI_MAX}]", n_range == 0, f"{n_range:,} outside"),
            ("every missing row has a gap length", n_nogap == 0, f"{n_nogap:,} without"),
            ("land points in the table = land points imputed", n_land == int(land.sum()), f"{n_land:,} / {int(land.sum()):,}"),
        ]
        print()
        for name, ok, detail in checks:
            print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {detail}")
        if not all(ok for _, ok, _ in checks):
            raise RuntimeError("checks failed: " + "; ".join(n for n, ok, _ in checks if not ok))

        n_dedup = n_in - n_unkeyed - n_dup
        metrics = [
            (701, "ndvi_input",                    n_in,                   0,             n_in),
            (702, "ndvi_unusable_key",             n_in,                   n_unkeyed,     n_in - n_unkeyed),
            (703, "ndvi_duplicate_point_composite", n_in - n_unkeyed,      n_dup,         n_dedup),
            (704, "ndvi_no_data_point_rows",       n_dedup,                n_nodata_rows, n_clean),
            (705, "ndvi_missing_fill",             n_clean,                n_fill,        n_clean),
            (706, "ndvi_missing_cloud",            n_clean,                n_cloud,       n_clean),
            (707, "ndvi_missing_snow_or_invalid",  n_clean,                n_other,       n_clean),
            (708, "ndvi_imputed (before = missing, after = still missing)", n_missing, n_imputed, n_null),
            (709, "ndvi_output (affected = imputed)", n_clean,             n_imputed,     n_clean),
        ]
        cur.executemany("""INSERT INTO meta.dq_metrics (run_id, step_order, rule, src_sensor,
                                                        rows_before, rows_affected, rows_after)
                           VALUES (%s, %s, %s, 'MOD13Q1', %s, %s, %s)""",
                        [(run_id, *m) for m in metrics])

        headline = table[table.slice == "all_real_gap_mix"].set_index("method").rmse.round(4).to_dict()
        note = dict(params, raw_rows=n_raw, unusable_key=n_unkeyed, duplicates=n_dup,
                    duplicates_conflicting=n_dup_conflict, points=n_points, no_data_points=n_nodata_points,
                    no_data_rows=n_nodata_rows, land_points=n_land, sparse_points=n_sparse,
                    land_points_in_thailand=n_land_thailand, composites=int(Y.shape[1]),
                    clean_rows=n_clean, valid=n_valid, missing=n_missing, missing_fill=n_fill,
                    missing_cloud=n_cloud, missing_other=n_other, imputed=n_imputed, still_missing=n_null,
                    cloud_delivered_minus_imputed=round(cloud_shift, 4), evaluation=eval_info,
                    rmse_real_gap_mix=headline, checksum=str(checksum), seconds=round(time.time() - t0))
        cur.execute("UPDATE meta.event_log SET status = 'SUCCESS', rows_processed = %s, note = %s WHERE log_key = %s",
                    (n_clean, json.dumps(note), run_id))
        conn.commit()
    except BaseException as exc:
        conn.rollback()
        cur.execute("UPDATE meta.event_log SET status = 'FAILED', note = %s WHERE log_key = %s",
                    (f"{type(exc).__name__}: {exc}"[:2000], run_id))
        conn.commit()
        raise
    finally:
        conn.close()

    print(f"\nrun {run_id} SUCCESS: {n_raw:,} raw -> {n_clean:,} clean rows on {n_land + n_sparse:,} points; "
          f"{n_missing:,} missing ({100 * n_missing / n_clean:.1f}%), {n_imputed:,} imputed, {n_null:,} left "
          f"on {n_sparse} sparse points")
    print(f"hold-out RMSE (real gap mix): {headline}")
    print(f"checksum {checksum}; {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
