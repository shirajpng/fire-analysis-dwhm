"""
Run and benchmark the five OLAP queries in olap/ against the dds star schema.

    olap/q1_rollup_fire_year_location.sql        roll-up / drill-down, location hierarchy
    olap/q2_protected_area_share_by_season.sql   slice / dice, protected areas (RQ2)
    olap/q3_top_parks_bridge.sql                 many-to-many through the bridge
    olap/q4_sensor_pivot.sql                     pivot, sensors x confidence x day/night
    olap/q5_province_burning_season.sql          slice + drill-down, partition pruning
    olap/q5_province_burning_season_unpruned.sql same query without pruning (benchmark)

For every query:
  1. one first run, then --runs warm runs (wall clock, all rows fetched)
  2. EXPLAIN (ANALYZE, BUFFERS) once: server time, buffers read from the
     Postgres cache (hit) or from outside it (read), fact partitions scanned;
     the text plan is saved to olap_query_results/plans/<id>.txt
  3. the result is saved to olap_query_results/<id>.csv

Then it checks the results against each other and the fact table (any failure
exits with status 1), writes olap_query_results/benchmark.csv and logs an
OLAP_BENCHMARK row in meta.event_log. Read-only otherwise.

"First run" is only cold for Postgres's own cache (shared_buffers, 128 MB):
the operating system's file cache inside the Docker VM may already hold the
data. The shared_read column shows how much came from outside shared_buffers.

Usage:
    python controllers/olap_benchmark.py              # all queries, 5 warm runs
    python controllers/olap_benchmark.py --runs 10
    python controllers/olap_benchmark.py --only q3    # one query (checks that need others are skipped)
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
import sys
import time
from pathlib import Path

import psycopg2

DB = dict(
    host=os.getenv("DWH_HOST", "localhost"),
    port=int(os.getenv("DWH_PORT", "5432")),
    dbname=os.getenv("DWH_DB", "fire_dwh"),
    user=os.getenv("DWH_USER", "dwh_admin"),
    password=os.getenv("DWH_PASSWORD", "admin123"),
)

ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = ROOT / "olap"
OUT_DIR = ROOT / "olap_query_results"

# (id, file, title)
QUERIES = [
    ("q1",  "q1_rollup_fire_year_location.sql",        "Roll-up: fire year > province > district"),
    ("q2",  "q2_protected_area_share_by_season.sql",   "Dice: protected-area share by fire year and season"),
    ("q3",  "q3_top_parks_bridge.sql",                 "Bridge: top 10 parks, count vs allocated"),
    ("q4",  "q4_sensor_pivot.sql",                     "Pivot: sensor x confidence x day/night"),
    ("q5",  "q5_province_burning_season.sql",          "Slice: Chiang Mai 2024 burning season (pruned)"),
    ("q5u", "q5_province_burning_season_unpruned.sql", "Slice: same, year on dim_date (unpruned)"),
]


def load_sql(name: str) -> str:
    return (SQL_DIR / name).read_text().strip().rstrip(";")


def run_query(cur, sql: str):
    t = time.perf_counter()
    cur.execute(sql)
    rows = cur.fetchall()
    return (time.perf_counter() - t) * 1000, [d[0] for d in cur.description], rows


def walk(node):
    yield node
    for child in node.get("Plans", []):
        yield from walk(child)


def explain(cur, sql: str) -> dict:
    cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql)
    doc = cur.fetchone()[0][0]
    plan = doc["Plan"]
    partitions = {n["Relation Name"] for n in walk(plan)
                  if n.get("Relation Name", "").startswith("fact_active_fire_")
                  and n.get("Actual Loops", 0) > 0}
    cur.execute("EXPLAIN (ANALYZE, BUFFERS) " + sql)
    text = "\n".join(r[0] for r in cur.fetchall())
    return dict(server_ms=round(doc["Execution Time"] + doc["Planning Time"], 1),
                shared_hit=plan.get("Shared Hit Blocks", 0),
                shared_read=plan.get("Shared Read Blocks", 0),
                partitions_scanned=len(partitions),
                plan_text=text)


def save_csv(path: Path, columns, rows) -> None:
    with path.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(columns)
        w.writerows(rows)


def scalar(cur, sql: str):
    cur.execute(sql)
    return cur.fetchone()[0]


def checks(cur, res: dict) -> list[tuple[str, bool, str]]:
    """Cross-check the query results; each entry is (check, passed, detail)."""
    out = []
    if "q1" in res:
        cols, rows = res["q1"]
        i_level, i_fires = cols.index("level"), cols.index("fires")
        total = next(r[i_fires] for r in rows if r[i_level] == "all")
        districts = sum(r[i_fires] for r in rows if r[i_level] == "district")
        expected = scalar(cur, """SELECT count(*) FROM dds.fact_active_fire f
                                    JOIN dds.dim_location l USING (location_key)
                                    JOIN dds.dim_detection_flag q USING (detection_flag_key)
                                   WHERE l.in_thailand AND q.dq_pass""")
        out.append(("Q1 grand total = sum of district rows = Thai dq_pass fires",
                     total == districts == expected, f"{total:,} / {districts:,} / {expected:,}"))
    if "q2" in res:
        cols, rows = res["q2"]
        r = next(r for r in rows if r[0] == "all" and r[1] == "all")
        expected = scalar(cur, """SELECT count(*) FROM dds.fact_active_fire f
                                    JOIN dds.dim_location l USING (location_key) WHERE l.in_thailand""")
        out.append(("Q2 grand total = Thai fires in the fact",
                     r[cols.index("thai_fires")] == expected, f"{r[cols.index('thai_fires')]:,} / {expected:,}"))
        in_pa = scalar(cur, """SELECT count(*) FROM dds.fact_active_fire f
                                 JOIN dds.dim_location l USING (location_key)
                                WHERE l.in_thailand AND f.protected_area_key > 0""")
        both = r[cols.index("in_designated_pa")] + r[cols.index("only_in_proposed_pa")]
        out.append(("Q2 designated + proposed = Thai fires with a park key",
                     both == in_pa, f"{both:,} / {in_pa:,}"))
    if "q3" in res:
        cur.execute("""SELECT round(sum(allocation_weight)), count(DISTINCT detection_id)
                         FROM dds.bridge_fire_protected_area""")
        alloc, distinct = cur.fetchone()
        out.append(("Q3 allocation weights over all parks sum to the number of fires",
                     alloc == distinct, f"{alloc:,} / {distinct:,}"))
    if "q5" in res and "q5u" in res:
        out.append(("Q5 pruned and unpruned return identical rows",
                     res["q5"] == res["q5u"], f"{len(res['q5'][1])} rows each"))
        cols, rows = res["q5"]
        out.append(("Q5 returns rows (province name matches dim_location)",
                     len(rows) > 0, f"{len(rows)} rows"))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--runs", type=int, default=5, help="warm runs per query (default 5)")
    ap.add_argument("--only", help="run one query id (q1..q5, q5u)")
    args = ap.parse_args()

    queries = [q for q in QUERIES if not args.only or q[0] == args.only]
    if not queries:
        ap.error(f"unknown query id {args.only!r}")
    (OUT_DIR / "plans").mkdir(parents=True, exist_ok=True)

    conn = psycopg2.connect(**DB)
    conn.autocommit = True          # read-only; no snapshot held between queries
    cur = conn.cursor()
    cur.execute("SELECT sum(c.reltuples)::bigint FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
                "WHERE i.inhparent = 'dds.fact_active_fire'::regclass")
    fact_rows = cur.fetchone()[0]
    print(f"fact_active_fire: ~{fact_rows:,} rows; {args.runs} warm runs per query\n")

    summary, results = [], {}
    for qid, fname, title in queries:
        sql = load_sql(fname)
        first_ms, cols, rows = run_query(cur, sql)
        warm = [run_query(cur, sql)[0] for _ in range(args.runs)]
        ex = explain(cur, sql)
        results[qid] = (cols, rows)
        save_csv(OUT_DIR / f"{qid}.csv", cols, rows)
        (OUT_DIR / "plans" / f"{qid}.txt").write_text(f"-- {title}\n-- olap/{fname}\n\n{ex['plan_text']}\n")
        summary.append(dict(query=qid, title=title, result_rows=len(rows),
                            first_run_ms=round(first_ms, 1),
                            warm_median_ms=round(statistics.median(warm), 1) if warm else None,
                            warm_min_ms=round(min(warm), 1) if warm else None,
                            warm_max_ms=round(max(warm), 1) if warm else None,
                            server_ms=ex["server_ms"], shared_hit=ex["shared_hit"],
                            shared_read=ex["shared_read"], partitions_scanned=ex["partitions_scanned"]))
        s = summary[-1]
        print(f"{qid:<4} {title:<52} rows={s['result_rows']:>6}  first={s['first_run_ms']:>8.1f} ms  "
              f"warm median={s['warm_median_ms'] or 0:>8.1f} ms  partitions={s['partitions_scanned']}")

    results_check = checks(cur, results)
    print()
    for name, ok, detail in results_check:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {detail}")
    passed = all(ok for _, ok, _ in results_check)

    if not args.only:
        with (OUT_DIR / "benchmark.csv").open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(summary[0]))
            w.writeheader()
            w.writerows(summary)

    by_id = {s["query"]: s for s in summary}
    if "q5" in by_id and "q5u" in by_id and by_id["q5"]["warm_median_ms"]:
        print(f"\npartition pruning: {by_id['q5u']['warm_median_ms'] / by_id['q5']['warm_median_ms']:.1f}x faster "
              f"({by_id['q5']['partitions_scanned']} vs {by_id['q5u']['partitions_scanned']} partitions)")

    note = json.dumps({"runs": args.runs, "fact_rows": fact_rows,
                       "checks_passed": sum(ok for _, ok, _ in results_check),
                       "checks_total": len(results_check),
                       "median_ms": {s["query"]: s["warm_median_ms"] for s in summary}})
    cur.execute("""INSERT INTO meta.event_log (event_type, event_category, status, rows_processed, note)
                   VALUES ('OLAP_BENCHMARK', 'BENCHMARK', %s, %s, %s)""",
                ("SUCCESS" if passed else "FAILED", sum(s["result_rows"] for s in summary), note))
    conn.close()
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
