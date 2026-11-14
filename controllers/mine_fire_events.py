"""
Mining task 1: cluster fire detections into fire events (ST-DBSCAN).

    dds.fact_active_fire (in Thailand, dq_pass)
        -> dds.ml_fire_event              one row per event
        -> dds.ml_fire_event_detection    event and role of every detection
        -> dds.ml_fire_event_silhouette   silhouette of a sample, per fire year

Two detections are neighbours when they are at most --eps-km apart on the
ground AND at most --eps-days apart in time. A detection with at least
--min-pts neighbours (itself included) is a core point; core points that are
neighbours belong to the same event; a non-core detection next to a core
point joins the event of its nearest core point (border); everything else is
noise (an isolated detection). This is DBSCAN with separate space and time
radii (ST-DBSCAN), so one fire seen by several satellites, or on several
overpasses, becomes one event.

Each Aug-Jul fire year is clustered on its own: the boundary lies in the rainy
season, and an event that runs across 31 July / 1 August is split in two.
Positions are projected to UTM zone 47N (EPSG:32647) for distances in metres.

Silhouette: for a random sample of clustered detections per fire year, in the
space where one unit is eps-km on the ground and eps-days in time. A value is
exact when the nearest other event is proven (see silhouette()); otherwise it
is an upper bound and is_exact is false.

Rebuilt in ONE transaction (truncate + insert); reruns never duplicate rows and
give the same event ids for the same input. Rerun after load_dds.py.

Usage:
    python controllers/mine_fire_events.py
    python controllers/mine_fire_events.py --sweep 2023    # parameter table for fire year 2023/24, writes nothing
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
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components
from scipy.spatial import cKDTree

DB = dict(
    host=os.getenv("DWH_HOST", "localhost"),
    port=int(os.getenv("DWH_PORT", "5432")),
    dbname=os.getenv("DWH_DB", "fire_dwh"),
    user=os.getenv("DWH_USER", "dwh_admin"),
    password=os.getenv("DWH_PASSWORD", "admin123"),
)

EPS_KM, EPS_DAYS, MIN_PTS = 1.5, 2.0, 3
SLAB = 10_000            # detections per slab of the neighbour search (bounds memory)
SIL_SAMPLE = 2_000       # silhouette sample per fire year
SIL_MAX_K = 4_096        # neighbours looked at before a silhouette value is declared inexact
SEED = 42

# One fire year of Thai dq_pass detections, in time order.
YEAR_SQL = """
COPY (
    SELECT f.detection_id, f.date_key,
           ST_X(p.g) AS x, ST_Y(p.g) AS y,
           extract(epoch FROM f.acq_ts)::bigint AS epoch,
           f.longitude, f.latitude, f.frp_mw,
           f.location_key, f.protected_area_key, s.satellite_code
      FROM dds.fact_active_fire f
      JOIN dds.dim_location l           USING (location_key)
      JOIN dds.dim_detection_flag q     USING (detection_flag_key)
      JOIN dds.dim_satellite_instrument s USING (satellite_key)
     CROSS JOIN LATERAL (SELECT ST_Transform(f.geom, 32647) AS g) p
     WHERE f.date_key BETWEEN {lo} AND {hi}
       AND l.in_thailand AND q.dq_pass
     ORDER BY f.acq_ts, f.detection_id
) TO STDOUT WITH (FORMAT csv, HEADER)
"""


def read_year(cur, fire_year: int) -> pd.DataFrame:
    buf = io.BytesIO()
    cur.copy_expert(YEAR_SQL.format(lo=fire_year * 10000 + 801, hi=(fire_year + 1) * 10000 + 731), buf)
    buf.seek(0)
    return pd.read_csv(buf, dtype={"detection_id": str, "satellite_code": str})


def slab_pairs(pts: np.ndarray, t: np.ndarray):
    """
    Yield every neighbour pair (i, j), i < j, once, with its squared ground
    distance in eps units. pts is (x, y, t) scaled so that both radii are 1,
    sorted by time. Each slab searches its own points plus the later points
    still within the time radius, and keeps the pairs that start in the slab.
    """
    n = len(pts)
    for lo in range(0, n, SLAB):
        hi = min(lo + SLAB, n)
        end = int(np.searchsorted(t, t[hi - 1] + 1.0, side="right"))
        pairs = cKDTree(pts[lo:end]).query_pairs(1.0, p=np.inf, output_type="ndarray")
        if not len(pairs):
            continue
        i, j = pairs[:, 0], pairs[:, 1]
        d2 = ((pts[lo + i, :2] - pts[lo + j, :2]) ** 2).sum(axis=1)
        keep = (i < hi - lo) & (d2 <= 1.0)       # box -> circle on the ground
        yield lo + i[keep], lo + j[keep], d2[keep]


def st_dbscan(x, y, t_days, eps_km, eps_days, min_pts):
    """Return (label, role, scaled points): label -1 = noise, role 0 noise / 1 border / 2 core."""
    n = len(x)
    pts = np.column_stack([x / (eps_km * 1000.0), y / (eps_km * 1000.0), t_days / eps_days])
    t = pts[:, 2]

    degree = np.ones(n, dtype=np.int64)          # every point counts itself
    for i, j, _ in slab_pairs(pts, t):
        degree += np.bincount(i, minlength=n) + np.bincount(j, minlength=n)
    core = degree >= min_pts

    # Second pass: core-core pairs link events; core-other pairs are border candidates.
    links_a, links_b = [], []
    best_d2 = np.full(n, np.inf)
    best_core = np.full(n, -1, dtype=np.int64)
    for i, j, d2 in slab_pairs(pts, t):
        ci, cj = core[i], core[j]
        both = ci & cj
        if both.any():
            # Keep one spanning star per connected group of this slab, not every pair.
            a, b = i[both], j[both]
            nodes, inv = np.unique(np.concatenate([a, b]), return_inverse=True)
            g = coo_matrix((np.ones(len(a), dtype=np.int8), (inv[:len(a)], inv[len(a):])),
                           shape=(len(nodes), len(nodes)))
            _, comp = connected_components(g, directed=False)
            first = np.full(comp.max() + 1, -1, dtype=np.int64)
            first[comp[::-1]] = nodes[::-1]       # lowest node of each group
            links_a.append(nodes)
            links_b.append(first[comp])
        for border, c, mask in ((i, j, ~ci & cj), (j, i, ci & ~cj)):
            if mask.any():
                b_, c_, d_ = border[mask], c[mask], d2[mask]
                order = np.lexsort((c_, d_, b_))            # per border: nearest core, then lowest index
                b_, c_, d_ = b_[order], c_[order], d_[order]
                firsts = np.r_[True, b_[1:] != b_[:-1]]
                b_, c_, d_ = b_[firsts], c_[firsts], d_[firsts]
                better = (d_ < best_d2[b_]) | ((d_ == best_d2[b_]) & (c_ < best_core[b_]))
                best_d2[b_[better]], best_core[b_[better]] = d_[better], c_[better]

    label = np.full(n, -1, dtype=np.int64)
    if core.any():
        a = np.concatenate(links_a) if links_a else np.empty(0, dtype=np.int64)
        b = np.concatenate(links_b) if links_b else np.empty(0, dtype=np.int64)
        g = coo_matrix((np.ones(len(a), dtype=np.int8), (a, b)), shape=(n, n))
        _, comp = connected_components(g, directed=False)
        label[core] = comp[core]
        border = ~core & (best_core >= 0)
        label[border] = comp[best_core[border]]
    role = np.where(core, 2, np.where(label >= 0, 1, 0))
    return label, role, pts


def silhouette(pts: np.ndarray, label: np.ndarray, rng) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Silhouette of a random sample of clustered points: (index, value, exact).

    b(i), the mean distance to the nearest other event, is searched among the
    events of the k nearest points. An event not seen there has every point,
    and so its mean, at least as far as the k-th neighbour; once the best b is
    not above that distance it is proven. k grows up to SIL_MAX_K.
    """
    idx = np.flatnonzero(label >= 0)
    if len(idx) == 0:
        return idx, np.empty(0), np.empty(0, dtype=bool)
    sample = np.sort(rng.choice(idx, size=min(SIL_SAMPLE, len(idx)), replace=False))
    order = idx[np.argsort(label[idx], kind="stable")]
    labels_sorted = label[order]
    tree = cKDTree(pts[idx])

    def members(lab):
        lo, hi = np.searchsorted(labels_sorted, [lab, lab + 1])
        return pts[order[lo:hi]]

    values, exact = np.zeros(len(sample)), np.zeros(len(sample), dtype=bool)
    for s, i in enumerate(sample):
        own = members(label[i])
        a = np.linalg.norm(own - pts[i], axis=1).sum() / (len(own) - 1)
        b, seen, k = np.inf, set(), 64
        while True:
            k = min(k, len(idx))
            dist, nb = tree.query(pts[i], k=k)
            dist, nb = np.atleast_1d(dist), np.atleast_1d(nb)
            for lab in np.unique(label[idx[nb]]):
                if lab != label[i] and lab not in seen:
                    seen.add(lab)
                    b = min(b, np.linalg.norm(members(lab) - pts[i], axis=1).mean())
            if b <= dist[-1] or k == len(idx):
                exact[s] = True
                break
            if k >= SIL_MAX_K:
                break
            k *= 4
        if not np.isfinite(b):
            values[s] = np.nan                    # no other event within reach: dropped
        else:
            values[s] = (b - a) / max(a, b)
    ok = ~np.isnan(values)
    return sample[ok], values[ok], exact[ok]


def summarise(df: pd.DataFrame, fire_year: int) -> pd.DataFrame:
    """One row per event from the member detections (df['event_seq'] >= 0)."""
    m = df[df["event_seq"] >= 0]
    g = m.groupby("event_seq", sort=True)
    ev = g.agg(
        start_epoch=("epoch", "min"), end_epoch=("epoch", "max"),
        start_date_key=("date_key", "min"), end_date_key=("date_key", "max"),
        n_detections=("epoch", "size"), n_core=("is_core", "sum"),
        n_satellites=("satellite_code", "nunique"),
        frp_max_mw=("frp_mw", "max"), frp_mean_mw=("frp_mw", "mean"), frp_sum_mw=("frp_mw", "sum"),
        longitude=("longitude", "mean"), latitude=("latitude", "mean"),
        x_min=("x", "min"), x_max=("x", "max"), y_min=("y", "min"), y_max=("y", "max"),
        n_districts=("location_key", "nunique"),
        n_in_protected_area=("in_pa", "sum"),
    )
    ev["n_overpasses"] = m.drop_duplicates(["event_seq", "satellite_code", "epoch"]).groupby("event_seq").size()
    ev["extent_km"] = np.hypot(ev.x_max - ev.x_min, ev.y_max - ev.y_min) / 1000.0
    ev["duration_days"] = (ev.end_epoch - ev.start_epoch) / 86400.0

    def modal(frame, col):       # most frequent value per event; lowest key on ties
        c = frame.groupby(["event_seq", col]).size().reset_index(name="n")
        return c.sort_values(["event_seq", "n", col], ascending=[True, False, True]) \
                .drop_duplicates("event_seq").set_index("event_seq")[col]

    ev["location_key"] = modal(m, "location_key")
    ev["protected_area_key"] = modal(m[m["in_pa"]], "protected_area_key").reindex(ev.index).fillna(0).astype(int)
    if len(ev) >= 1_000_000:
        raise RuntimeError(f"fire year {fire_year}: {len(ev):,} events do not fit the event_id numbering")
    ev["event_id"] = fire_year * 1_000_000 + ev.index + 1
    ev["fire_year_start"] = fire_year
    return ev.reset_index(drop=True)


def cluster_year(df: pd.DataFrame, fire_year: int, eps_km, eps_days, min_pts):
    """Cluster one fire year; adds event_seq (start order, -1 noise) and role to df."""
    label, role, pts = st_dbscan(df["x"].to_numpy(), df["y"].to_numpy(),
                                 df["epoch"].to_numpy() / 86400.0, eps_km, eps_days, min_pts)
    # Number events in start order (df is sorted by time, then detection_id).
    seq = np.full(len(df), -1, dtype=np.int64)
    clustered = label >= 0
    if clustered.any():
        _, first_pos, inv = np.unique(label[clustered], return_index=True, return_inverse=True)
        seq[clustered] = np.argsort(np.argsort(first_pos))[inv]
    df["event_seq"], df["role"] = seq, role
    df["is_core"] = role == 2
    df["in_pa"] = df["protected_area_key"] > 0
    return seq, pts


def copy_rows(cur, table: str, columns: list[str], frame: pd.DataFrame) -> None:
    buf = io.StringIO()
    frame[columns].to_csv(buf, index=False, header=False, na_rep="")
    buf.seek(0)
    cur.copy_expert(f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv)", buf)


def fire_years(cur) -> list[int]:
    cur.execute("SELECT min(date_key), max(date_key) FROM dds.fact_active_fire")
    lo, hi = cur.fetchone()
    if lo is None:
        raise SystemExit("dds.fact_active_fire is empty: run controllers/load_dds.py first")
    year = lambda k: k // 10000 - (1 if (k // 100) % 100 < 8 else 0)
    return list(range(year(lo), year(hi) + 1))


def sweep(cur, fire_year: int) -> int:
    df = read_year(cur, fire_year)
    print(f"fire year {fire_year}/{(fire_year + 1) % 100:02d}: {len(df):,} detections\n")
    print(f"{'eps_km':>6} {'eps_days':>8} {'min_pts':>7} {'events':>8} {'noise %':>8} "
          f"{'median n':>8} {'largest n':>9} {'max days':>8} {'max km':>7}")
    for eps_km in (0.75, 1.0, 1.5, 2.0):
        for eps_days in (1.0, 2.0, 5.0):
            for min_pts in (2, 3, 5):
                d = df.copy()
                cluster_year(d, fire_year, eps_km, eps_days, min_pts)
                ev = summarise(d, fire_year)
                print(f"{eps_km:>6} {eps_days:>8} {min_pts:>7} {len(ev):>8,} "
                      f"{100 * (d.event_seq < 0).mean():>8.1f} {ev.n_detections.median():>8.0f} "
                      f"{ev.n_detections.max():>9,} {ev.duration_days.max():>8.1f} {ev.extent_km.max():>7.1f}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--eps-km", type=float, default=EPS_KM, help=f"ground radius in km (default {EPS_KM})")
    ap.add_argument("--eps-days", type=float, default=EPS_DAYS, help=f"time radius in days (default {EPS_DAYS})")
    ap.add_argument("--min-pts", type=int, default=MIN_PTS, help=f"neighbours for a core point, itself included (default {MIN_PTS})")
    ap.add_argument("--sweep", type=int, metavar="FIRE_YEAR", help="print a parameter table for one fire year and exit")
    args = ap.parse_args()

    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    if args.sweep is not None:
        return sweep(cur, args.sweep)

    params = dict(eps_km=args.eps_km, eps_days=args.eps_days, min_pts=args.min_pts)
    cur.execute("""INSERT INTO meta.event_log (event_type, event_category, status, note)
                   VALUES ('MINE_FIRE_EVENTS', 'MINING', 'RUNNING', %s) RETURNING log_key""",
                (json.dumps(params),))
    run_id = cur.fetchone()[0]
    conn.commit()
    print(f"run {run_id}: {params}")

    t0 = time.perf_counter()
    try:
        # One snapshot for the whole job: the check against the fact at the end
        # must see the same rows the clustering read.
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        cur.execute("TRUNCATE dds.ml_fire_event, dds.ml_fire_event_detection, dds.ml_fire_event_silhouette")
        rng = np.random.default_rng(SEED)
        totals = dict(detections=0, events=0, noise=0, core=0, border=0)
        sil_all, sil_exact, per_year = [], 0, {}

        for fy in fire_years(cur):
            t1 = time.perf_counter()
            df = read_year(cur, fy)
            if df.empty:
                continue
            seq, pts = cluster_year(df, fy, **params)
            ev = summarise(df, fy)

            df["event_id"] = pd.array(np.where(seq >= 0, fy * 1_000_000 + seq + 1, -1), dtype="Int64")
            df.loc[seq < 0, "event_id"] = pd.NA
            df["role_name"] = np.array(["noise", "border", "core"])[df["role"].to_numpy()]
            df["run_id"] = run_id
            copy_rows(cur, "dds.ml_fire_event_detection",
                      ["detection_id", "date_key", "event_id", "role", "run_id"],
                      df.assign(role=df["role_name"]))

            if len(ev):
                ev["run_id"] = run_id
                ev["start_ts"] = pd.to_datetime(ev.start_epoch, unit="s", utc=True)
                ev["end_ts"] = pd.to_datetime(ev.end_epoch, unit="s", utc=True)
                ev["geom"] = "SRID=4326;POINT(" + ev.longitude.round(6).astype(str) + " " + ev.latitude.round(6).astype(str) + ")"
                copy_rows(cur, "dds.ml_fire_event",
                          ["event_id", "fire_year_start", "start_ts", "end_ts", "start_date_key", "end_date_key",
                           "duration_days", "n_detections", "n_core", "n_overpasses", "n_satellites",
                           "frp_max_mw", "frp_mean_mw", "frp_sum_mw", "longitude", "latitude", "geom",
                           "extent_km", "location_key", "n_districts", "protected_area_key",
                           "n_in_protected_area", "run_id"], ev)

            s_idx, s_val, s_exact = silhouette(pts, seq, rng)
            if len(s_idx):
                sil = df.iloc[s_idx][["detection_id", "date_key", "event_id"]].copy()
                sil["silhouette"], sil["is_exact"], sil["run_id"] = s_val.round(4), s_exact, run_id
                copy_rows(cur, "dds.ml_fire_event_silhouette",
                          ["detection_id", "date_key", "event_id", "silhouette", "is_exact", "run_id"], sil)
                sil_all.append(s_val)
                sil_exact += int(s_exact.sum())

            role = df["role"].to_numpy()
            totals["detections"] += len(df)
            totals["events"] += len(ev)
            totals["noise"] += int((role == 0).sum())
            totals["border"] += int((role == 1).sum())
            totals["core"] += int((role == 2).sum())
            per_year[fy] = dict(detections=len(df), events=len(ev),
                                silhouette=round(float(s_val.mean()), 3) if len(s_val) else None)
            print(f"  {fy}/{(fy + 1) % 100:02d}: {len(df):>9,} detections -> {len(ev):>7,} events, "
                  f"{100 * (role == 0).mean():4.1f}% noise, silhouette "
                  f"{s_val.mean() if len(s_val) else float('nan'):.3f}  ({time.perf_counter() - t1:.0f} s)")

        # ---- checks: any failure rolls the whole job back
        cur.execute("""SELECT count(*) FROM dds.fact_active_fire f
                         JOIN dds.dim_location l USING (location_key)
                         JOIN dds.dim_detection_flag q USING (detection_flag_key)
                        WHERE l.in_thailand AND q.dq_pass""")
        fact_rows = cur.fetchone()[0]
        cur.execute("""SELECT count(*), count(event_id),
                              count(*) FILTER (WHERE (role = 'noise') <> (event_id IS NULL))
                         FROM dds.ml_fire_event_detection""")
        det_rows, det_clustered, role_mismatch = cur.fetchone()
        cur.execute("""SELECT count(*), coalesce(sum(n_detections), 0), coalesce(min(n_detections), 0),
                              coalesce(min(n_core), 0),
                              count(*) FILTER (WHERE end_date_key / 10000 - (end_date_key / 100 % 100 < 8)::int
                                                     <> fire_year_start)
                         FROM dds.ml_fire_event""")
        ev_rows, ev_members, ev_min, core_min, cross_year = cur.fetchone()
        cur.execute("""SELECT count(*) FROM (
                           SELECT event_id, count(*) AS n FROM dds.ml_fire_event_detection
                            WHERE event_id IS NOT NULL GROUP BY 1) d
                         FULL JOIN dds.ml_fire_event e USING (event_id)
                        WHERE d.n IS DISTINCT FROM e.n_detections""")
        member_diff = cur.fetchone()[0]
        checks = [
            ("detections clustered = Thai dq_pass detections in the fact", det_rows == fact_rows == totals["detections"],
             f"{det_rows:,} / {fact_rows:,}"),
            ("sum of event sizes = detections with an event", ev_members == det_clustered, f"{ev_members:,} / {det_clustered:,}"),
            ("members per event = n_detections, no event without members", member_diff == 0, f"{member_diff} differing"),
            ("noise <=> no event", role_mismatch == 0, f"{role_mismatch} rows"),
            (f"every event has >= {args.min_pts} detections and a core point",
             ev_rows == 0 or (ev_min >= args.min_pts and core_min >= 1), f"min {ev_min}, min core {core_min}"),
            ("no event ends outside its fire year", cross_year == 0, f"{cross_year} events"),
        ]
        print()
        for name, ok, detail in checks:
            print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {detail}")
        if not all(ok for _, ok, _ in checks):
            raise RuntimeError("checks failed: " + "; ".join(n for n, ok, _ in checks if not ok))

        cur.execute("""INSERT INTO meta.dq_metrics (run_id, step_order, rule, src_sensor,
                                                    rows_before, rows_affected, rows_after)
                       VALUES (%s, 501, 'fire events: detections -> events (affected = noise)', 'ALL', %s, %s, %s)""",
                    (run_id, totals["detections"], totals["noise"], totals["events"]))
        cur.execute("ANALYZE dds.ml_fire_event")
        cur.execute("ANALYZE dds.ml_fire_event_detection")
        sil = np.concatenate(sil_all) if sil_all else np.empty(0)
        note = dict(params, **totals, seconds=round(time.perf_counter() - t0),
                    silhouette_mean=round(float(sil.mean()), 4) if len(sil) else None,
                    silhouette_sampled=len(sil), silhouette_exact=sil_exact, per_fire_year=per_year)
        cur.execute("UPDATE meta.event_log SET status = 'SUCCESS', rows_processed = %s, note = %s WHERE log_key = %s",
                    (totals["detections"], json.dumps(note), run_id))
        conn.commit()
    except BaseException as exc:
        conn.rollback()
        cur.execute("UPDATE meta.event_log SET status = 'FAILED', note = %s WHERE log_key = %s",
                    (f"{type(exc).__name__}: {exc}"[:2000], run_id))
        conn.commit()
        raise
    finally:
        conn.close()

    print(f"\n{totals['detections']:,} detections -> {totals['events']:,} events "
          f"({totals['core']:,} core, {totals['border']:,} border, {totals['noise']:,} noise); "
          f"silhouette {sil.mean():.3f} over {len(sil):,} sampled ({sil_exact:,} exact); "
          f"{time.perf_counter() - t0:.0f} s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
