"""
Mining task 2 (RQ3): anomalous burning per district and month (Isolation Forest).

    dds.fact_active_fire (in Thailand, dq_pass, Suomi-NPP VIIRS)
    dds.fact_ndvi_district_month (vegetation state, from load_ndvi_dds.py)
        -> dds.ml_district_month_anomaly   one scored row per district x month

Only Suomi-NPP is counted. It is the one satellite with a record over the whole
period (from 2012-01-20); NOAA-20 joined in 2018 and NOAA-21 in 2024, so counts
pooled over all satellites rise for that reason alone and every recent month
would look anomalous.

Grid: every Thai district x every complete month (months with no detection
count as 0). Dropped: the first month (starts on the 20th), the month of the
latest detection unless it is complete, and months with fewer than
--min-history earlier years of the same calendar month.

Features, per district x month:
    log_n, log_n_pa, log_frp_sum, log_frp_max   log1p of count, count in a Designated /
                                                Inscribed protected area, FRP sum, FRP max
    pa_share, night_share                       shares of the count (0 when the count is 0)
    expected_log_n                              mean of log_n over the earlier years, same
                                                district and calendar month
    dev_log_n, dev_log_n_pa, dev_log_frp_sum    value - mean over those earlier years
    z_n                                         dev_log_n / max(std over those years, 0.5)
    lag1_log_n                                  log_n of the month before
    ndvi_lag1                                   mean NDVI of the district in the month before:
                                                the vegetation state before the month's fires
    dev_ndvi_lag1                               ndvi_lag1 - its mean over the earlier years, same
                                                calendar month (negative = drier than usual)

"Earlier years" never include the month itself or anything after it, so a
month is always compared with what was known before it.

NDVI is taken from the month before, not from the month itself, because burning
lowers NDVI: the month's own value is partly a result of its fires. NDVI
composites are published some weeks late. If the month before has no NDVI yet,
ndvi_lag1 is the district's mean for that calendar month and dev_ndvi_lag1 is 0
(counted as ndvi_fallback_rows in meta.event_log).

The forest is fitted on the months before --train-until and scores every
month; is_anomaly uses the threshold that flags --contamination of the
training months. Months from --train-until on are out of sample.

There are no ground-truth labels for anomalous burning. The printed ROC-AUC and
average precision compare the score with a simple rule (z_n >= 3 and at least
10 detections) on the test months. z_n is itself a feature, so this shows
agreement with the rule, not accuracy.

Rebuilt in ONE transaction (truncate + insert). Rerun after load_dds.py and
load_ndvi_dds.py.

Usage:
    python controllers/mine_district_anomalies.py
    python controllers/mine_district_anomalies.py --contamination 0.01 --train-until 202108
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
from sklearn.ensemble import IsolationForest
from sklearn.metrics import average_precision_score, roc_auc_score

DB = dict(
    host=os.getenv("DWH_HOST", "localhost"),
    port=int(os.getenv("DWH_PORT", "5432")),
    dbname=os.getenv("DWH_DB", "fire_dwh"),
    user=os.getenv("DWH_USER", "dwh_admin"),
    password=os.getenv("DWH_PASSWORD", "admin123"),
)

SATELLITE = "N"          # Suomi-NPP (dim_satellite_instrument.satellite_code)
SEED = 42
FEATURES = ["log_n", "log_n_pa", "log_frp_sum", "log_frp_max", "pa_share", "night_share",
            "expected_log_n", "dev_log_n", "dev_log_n_pa", "dev_log_frp_sum", "z_n", "lag1_log_n",
            "ndvi_lag1", "dev_ndvi_lag1"]

AGG_SQL = """
SELECT f.location_key, f.date_key / 100 AS year_month,
       count(*) AS n_detections,
       count(*) FILTER (WHERE p.status IN ('Designated', 'Inscribed')) AS n_in_pa,
       coalesce(sum(f.frp_mw), 0)::float8 AS frp_sum_mw,
       coalesce(max(f.frp_mw), 0)::float8 AS frp_max_mw,
       count(*) FILTER (WHERE q.daynight = 'N') AS n_night
  FROM dds.fact_active_fire f
  JOIN dds.dim_location l             USING (location_key)
  JOIN dds.dim_detection_flag q       USING (detection_flag_key)
  JOIN dds.dim_satellite_instrument s USING (satellite_key)
  JOIN dds.dim_protected_area p       USING (protected_area_key)
 WHERE l.in_thailand AND q.dq_pass AND s.satellite_code = %s
 GROUP BY 1, 2
"""


def build_grid(cur) -> tuple[pd.DataFrame, dict]:
    cur.execute(AGG_SQL, (SATELLITE,))
    agg = pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])
    if agg.empty:
        raise SystemExit("no Thai Suomi-NPP detections in dds.fact_active_fire: run controllers/load_dds.py first")
    cur.execute("""SELECT min(f.date_key), max(f.date_key)
                     FROM dds.fact_active_fire f JOIN dds.dim_satellite_instrument s USING (satellite_key)
                    WHERE s.satellite_code = %s""", (SATELLITE,))
    first_key, last_key = cur.fetchone()
    cur.execute("SELECT location_key FROM dds.dim_location WHERE in_thailand ORDER BY 1")
    districts = [r[0] for r in cur.fetchall()]

    first = pd.Timestamp(str(first_key))
    last = pd.Timestamp(str(last_key))
    start = first if first.day == 1 else first + pd.offsets.MonthBegin(1)     # first complete month
    end = last if last.is_month_end else last - pd.offsets.MonthEnd(1)        # last complete month
    months = pd.period_range(start, end, freq="M")
    grid = pd.MultiIndex.from_product([districts, months.year * 100 + months.month],
                                      names=["location_key", "year_month"]).to_frame(index=False)
    df = grid.merge(agg, how="left", on=["location_key", "year_month"]).fillna(0)
    info = dict(first_date=int(first_key), last_date=int(last_key), districts=len(districts), months=len(months),
                detections=int(df.n_detections.sum()), detections_all_months=int(agg.n_detections.sum()))
    return df, info


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["location_key", "year_month"]).reset_index(drop=True)
    df["month"] = df.year_month % 100
    n = df.n_detections.clip(lower=1)
    df["log_n"] = np.log1p(df.n_detections)
    df["log_n_pa"] = np.log1p(df.n_in_pa)
    df["log_frp_sum"] = np.log1p(df.frp_sum_mw)
    df["log_frp_max"] = np.log1p(df.frp_max_mw)
    df["pa_share"] = df.n_in_pa / n
    df["night_share"] = df.n_night / n

    # Same district and calendar month, earlier years only (shift excludes the row itself).
    by = df.groupby(["location_key", "month"], sort=False)
    df["n_prior_years"] = by.cumcount()
    for col in ("log_n", "log_n_pa", "log_frp_sum"):
        prior_sum = by[col].cumsum() - df[col]
        df[f"expected_{col}"] = prior_sum / df.n_prior_years.where(df.n_prior_years > 0)
        df[f"dev_{col}"] = df[col] - df[f"expected_{col}"]
    prior_sq = by["log_n"].transform(lambda s: (s ** 2).cumsum()) - df.log_n ** 2
    k = df.n_prior_years.where(df.n_prior_years > 1)
    var = (prior_sq - k * df.expected_log_n ** 2) / (k - 1)                    # sample variance of the earlier years
    df["std_log_n"] = np.sqrt(var.clip(lower=0))
    df["z_n"] = df.dev_log_n / df.std_log_n.clip(lower=0.5)
    df["lag1_log_n"] = df.groupby("location_key", sort=False)["log_n"].shift(1)
    return df


def add_ndvi_features(cur, df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """ndvi_lag1 and dev_ndvi_lag1: the district's NDVI in the month before each row's month."""
    cur.execute("SELECT location_key, year_month, ndvi_mean::float8 FROM dds.fact_ndvi_district_month")
    ndvi = pd.DataFrame(cur.fetchall(), columns=["location_key", "year_month", "ndvi"])
    if ndvi.empty:
        raise SystemExit("dds.fact_ndvi_district_month is empty: run controllers/load_ndvi_dds.py first")
    ndvi = ndvi.sort_values(["location_key", "year_month"]).reset_index(drop=True)
    ndvi["month"] = ndvi.year_month % 100

    # Same district and calendar month, earlier years only (as for the fire counts).
    by = ndvi.groupby(["location_key", "month"], sort=False)
    prior_years = by.cumcount()
    expected = (by.ndvi.cumsum() - ndvi.ndvi) / prior_years.where(prior_years > 0)
    ndvi["dev_ndvi_lag1"] = ndvi.ndvi - expected

    # A month's NDVI is the feature of the month after it.
    after = pd.to_datetime(ndvi.year_month.astype(str), format="%Y%m") + pd.offsets.MonthBegin(1)
    lagged = ndvi.assign(year_month=after.dt.year * 100 + after.dt.month).rename(columns={"ndvi": "ndvi_lag1"})
    df = df.merge(lagged[["location_key", "year_month", "ndvi_lag1", "dev_ndvi_lag1"]],
                  how="left", on=["location_key", "year_month"])

    # No NDVI for the month before (not published yet), or no earlier year to compare with:
    # the district's mean for that calendar month, and no deviation.
    fallback = df.ndvi_lag1.isna() | df.dev_ndvi_lag1.isna()
    if fallback.any():
        normal = ndvi.groupby(["location_key", "month"]).ndvi.mean().rename("ndvi_normal").reset_index()
        month_before = (df.year_month % 100 + 10) % 12 + 1
        normal = df.assign(month=month_before)[["location_key", "month"]].merge(
            normal, how="left", on=["location_key", "month"]).ndvi_normal.to_numpy()
        df["ndvi_lag1"] = df.ndvi_lag1.where(df.ndvi_lag1.notna(), normal)
        df["dev_ndvi_lag1"] = df.dev_ndvi_lag1.where(~fallback, 0.0)
    info = dict(ndvi_first_month=int(ndvi.year_month.min()), ndvi_last_month=int(ndvi.year_month.max()),
                ndvi_fallback_rows=int(fallback.sum()))
    return df, info


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--contamination", type=float, default=0.02,
                    help="share of training months flagged as anomalies (default 0.02)")
    ap.add_argument("--train-until", type=int, default=202208, metavar="YYYYMM",
                    help="fit on months before this one; later months are out of sample (default 202208)")
    ap.add_argument("--min-history", type=int, default=2,
                    help="earlier years of the same calendar month a month needs (default 2)")
    args = ap.parse_args()
    if args.min_history < 2:
        ap.error("--min-history must be at least 2 (a standard deviation needs two years)")
    params = dict(contamination=args.contamination, train_until=args.train_until,
                  min_history=args.min_history, satellite=SATELLITE)

    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    cur.execute("""INSERT INTO meta.event_log (event_type, event_category, status, note)
                   VALUES ('MINE_DISTRICT_ANOMALIES', 'MINING', 'RUNNING', %s) RETURNING log_key""",
                (json.dumps(params),))
    run_id = cur.fetchone()[0]
    conn.commit()
    print(f"run {run_id}: {params}")

    t0 = time.perf_counter()
    try:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        grid, info = build_grid(cur)
        df = add_features(grid)
        df = df[df.n_prior_years >= args.min_history].copy()
        df, ndvi_info = add_ndvi_features(cur, df)
        info.update(ndvi_info)
        train = df.year_month < args.train_until
        if not train.any() or train.all():
            raise SystemExit(f"--train-until {args.train_until} leaves no training or no test months "
                             f"({df.year_month.min()}..{df.year_month.max()})")

        X = df[FEATURES].to_numpy()
        forest = IsolationForest(n_estimators=200, contamination=args.contamination,
                                 random_state=SEED, n_jobs=-1).fit(X[train.to_numpy()])
        df["anomaly_score"] = forest.decision_function(X)
        df["is_anomaly"] = df.anomaly_score < 0
        df["anomaly_rank"] = df.anomaly_score.rank(method="first").astype(int)
        df["direction"] = np.where(df.dev_log_n >= 0, "high", "low")
        df["split"] = np.where(train, "train", "test")

        # Feature furthest from its training mean, in training standard deviations.
        # (Not median / IQR: most months have no fire, so the IQR of the shares is 0.)
        mean, std = X[train.to_numpy()].mean(axis=0), X[train.to_numpy()].std(axis=0)
        df["top_feature"] = np.array(FEATURES)[np.abs((X - mean) / np.where(std > 0, std, 1.0)).argmax(axis=1)]

        month = pd.to_datetime(df.year_month.astype(str), format="%Y%m")
        df["month_start"] = month.dt.date
        df["fire_year_start"] = month.dt.year - (month.dt.month < 8).astype(int)
        df["run_id"] = run_id
        for col in ("n_detections", "n_in_pa"):
            df[col] = df[col].astype(int)

        cur.execute("TRUNCATE dds.ml_district_month_anomaly")
        columns = ["location_key", "year_month", "month_start", "fire_year_start", "n_detections", "n_in_pa",
                   "pa_share", "frp_sum_mw", "frp_max_mw", "night_share", "n_prior_years", "expected_log_n",
                   "dev_log_n", "z_n", "ndvi_lag1", "dev_ndvi_lag1", "anomaly_score", "is_anomaly",
                   "anomaly_rank", "direction", "top_feature", "split", "run_id"]
        buf = io.StringIO()
        df[columns].to_csv(buf, index=False, header=False, float_format="%.6g")
        buf.seek(0)
        cur.copy_expert(f"COPY dds.ml_district_month_anomaly ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv)", buf)

        # ---- agreement with the simple rule, on the out-of-sample months
        test = df[~train]
        rule = (test.z_n >= 3) & (test.n_detections >= 10)
        agreement = dict(rule_months=int(rule.sum()))
        if 0 < rule.sum() < len(test):
            agreement.update(roc_auc=round(float(roc_auc_score(rule, -test.anomaly_score)), 4),
                             average_precision=round(float(average_precision_score(rule, -test.anomaly_score)), 4),
                             rule_share=round(float(rule.mean()), 4),
                             flagged_and_rule=int((rule & test.is_anomaly).sum()))

        # ---- checks: any failure rolls the whole job back
        cur.execute("""SELECT count(*), coalesce(sum(n_detections), 0), count(*) FILTER (WHERE is_anomaly),
                              count(*) FILTER (WHERE is_anomaly <> (anomaly_score < 0)),
                              count(DISTINCT anomaly_rank)
                         FROM dds.ml_district_month_anomaly""")
        rows, det_sum, flagged, bad_flag, ranks = cur.fetchone()
        cur.execute("""SELECT count(*) FROM dds.fact_active_fire f
                         JOIN dds.dim_location l USING (location_key)
                         JOIN dds.dim_detection_flag q USING (detection_flag_key)
                         JOIN dds.dim_satellite_instrument s USING (satellite_key)
                        WHERE l.in_thailand AND q.dq_pass AND s.satellite_code = %s
                          AND f.date_key / 100 IN (SELECT DISTINCT year_month FROM dds.ml_district_month_anomaly)""",
                    (SATELLITE,))
        fact_sum = cur.fetchone()[0]
        train_share = float(df.is_anomaly[train].mean())
        checks = [
            ("rows = districts x scored months", rows == len(df) == info["districts"] * df.year_month.nunique(),
             f"{rows:,} = {info['districts']} x {df.year_month.nunique()}"),
            ("detections in the table = Suomi-NPP Thai dq_pass detections of those months in the fact",
             det_sum == fact_sum, f"{det_sum:,} / {fact_sum:,}"),
            ("no missing feature value", not np.isnan(X).any(), f"{int(np.isnan(X).sum())} missing"),
            ("is_anomaly <=> score < 0; ranks unique", bad_flag == 0 and ranks == rows, f"{bad_flag} / {ranks:,}"),
            ("share of training months flagged = contamination",
             abs(train_share - args.contamination) < 0.002, f"{train_share:.4f} / {args.contamination}"),
        ]
        print()
        for name, ok, detail in checks:
            print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {detail}")
        if not all(ok for _, ok, _ in checks):
            raise RuntimeError("checks failed: " + "; ".join(n for n, ok, _ in checks if not ok))

        cur.execute("""INSERT INTO meta.dq_metrics (run_id, step_order, rule, src_sensor,
                                                    rows_before, rows_affected, rows_after)
                       VALUES (%s, 601, 'district-month anomalies: scored months (affected = flagged)',
                               'VIIRS_SNPP', %s, %s, %s)""", (run_id, rows, flagged, rows - flagged))
        cur.execute("ANALYZE dds.ml_district_month_anomaly")
        flagged_test = int(test.is_anomaly.sum())
        note = dict(params, **info, scored_months=int(df.year_month.nunique()), rows=rows,
                    first_scored=int(df.year_month.min()), last_scored=int(df.year_month.max()),
                    train_rows=int(train.sum()), test_rows=len(test), flagged=flagged,
                    flagged_test=flagged_test, flagged_test_share=round(flagged_test / len(test), 4),
                    flagged_high=int((df.is_anomaly & (df.direction == "high")).sum()),
                    rule_agreement_test=agreement, seconds=round(time.perf_counter() - t0))
        cur.execute("UPDATE meta.event_log SET status = 'SUCCESS', rows_processed = %s, note = %s WHERE log_key = %s",
                    (rows, json.dumps(note), run_id))
        conn.commit()
    except BaseException as exc:
        conn.rollback()
        cur.execute("UPDATE meta.event_log SET status = 'FAILED', note = %s WHERE log_key = %s",
                    (f"{type(exc).__name__}: {exc}"[:2000], run_id))
        conn.commit()
        raise
    finally:
        conn.close()

    print(f"\n{rows:,} district-months scored ({note['first_scored']}..{note['last_scored']}); "
          f"{flagged:,} flagged, {note['flagged_high']:,} of them above expectation; "
          f"test months: {flagged_test:,} of {len(test):,} flagged ({100 * note['flagged_test_share']:.1f}%)")
    print(f"agreement with the z >= 3 rule on test months: {agreement}")
    print(f"{time.perf_counter() - t0:.0f} s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
