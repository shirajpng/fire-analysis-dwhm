"""
Count everything in the raw landing zone and report against the 5M requirement.

This is the script that produces the number your proposal has to state. Run it
after extraction and paste the output straight into the report.

It also flags the MODIS-versus-VIIRS column divergence, which is the schema
reconciliation problem you are claiming as an ETL deliverable -- better to see
it confirmed with real headers than to assert it from documentation.

Usage:
    python inventory.py
    python inventory.py --markdown > ../inventory.md
"""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path

import config

REQUIREMENT = 5_000_000


def count_rows(path: Path) -> int:
    """Row count excluding the header. Fast line count, no parsing."""
    if path.stat().st_size == 0:
        return 0
    with open(path, "rb") as fh:
        n = sum(1 for _ in fh)
    return max(0, n - 1)


def header_of(path: Path) -> tuple[str, ...]:
    if path.stat().st_size == 0:
        return ()
    with open(path, newline="") as fh:
        try:
            return tuple(next(csv.reader(fh)))
        except StopIteration:
            return ()


def scan(root: Path, exclude: Path | None = None) -> tuple[dict, dict, int]:
    by_group: dict[str, int] = defaultdict(int)
    files_by_group: dict[str, int] = defaultdict(int)
    headers: dict[tuple, list[str]] = defaultdict(list)
    total = 0

    for path in sorted(root.rglob("*.csv")):
        # The country archive is a duplicate of rows already counted under
        # firms/sp, held only to validate the spatial clip. Counting it would
        # add ~3.9M rows to the figure that goes in the report, none of them
        # new detections.
        if exclude and exclude in path.parents:
            continue

        rows = count_rows(path)
        total += rows

        # Group by the sensor=... partition if present, else by parent dir.
        group = next((p.split("=", 1)[1] for p in path.parts if p.startswith("sensor=")),
                     path.parent.name)
        by_group[group] += rows
        files_by_group[group] += 1

        hdr = header_of(path)
        if hdr:
            headers[hdr].append(group)

    return dict(by_group), {"files": dict(files_by_group), "headers": headers}, total


def report(as_markdown: bool) -> None:
    if not config.RAW.exists():
        raise SystemExit(f"{config.RAW} does not exist. Run the extractors first.")

    # Report which sources landed at all, so an empty table is never ambiguous.
    print("\nSource presence:")
    for label, folder, pattern in [
        ("FIRMS (sp)",   config.RAW_FIRMS / "sp", "*.csv"),
        ("FIRMS (nrt)",  config.RAW_FIRMS / "nrt", "*.csv"),
        ("FIRMS (ctry)", config.RAW_FIRMS_COUNTRY, "*.csv"),
        ("NDVI",         config.RAW_NDVI, "*.csv"),
        ("Land cover",   config.RAW_LANDCOVER, "*.csv"),
        ("WDPA",         config.RAW_WDPA, "*.zip"),
        ("Boundaries",   config.RAW_BOUNDARIES, "*.gpkg"),
    ]:
        if not folder.exists():
            print(f"  {label:<14} MISSING   ({folder} does not exist)")
        else:
            n = len(list(folder.rglob(pattern)))
            state = "ok" if n else "EMPTY"
            print(f"  {label:<14} {state:<9} {n} file(s)")

    by_group, meta, total = scan(config.RAW, exclude=config.RAW_FIRMS_COUNTRY)
    files = meta["files"]
    headers = meta["headers"]

    if config.RAW_FIRMS_COUNTRY.exists():
        ref_by_group, ref_meta, ref_total = scan(config.RAW_FIRMS_COUNTRY)
        ref_files = ref_meta["files"]
    else:
        ref_by_group, ref_files, ref_total = {}, {}, 0

    if as_markdown:
        print(f"# Raw data inventory — {config.REGION_NAME}\n")
        print("| Source | Files | Rows |")
        print("|---|---:|---:|")
        for group in sorted(by_group):
            print(f"| {group} | {files[group]:,} | {by_group[group]:,} |")
        print(f"| **TOTAL** | **{sum(files.values()):,}** | **{total:,}** |")
        print()
        status = "MEETS" if total >= REQUIREMENT else "BELOW"
        print(f"Requirement is {REQUIREMENT:,} records. Current total **{status}** it.\n")
        if ref_total:
            print(f"Excluded from the total: {ref_total:,} rows of FIRMS "
                  f"country-archive reference data across "
                  f"{sum(ref_files.values()):,} files, held to validate the "
                  f"spatial clip. Those rows are already in `firms/sp`.\n")
    else:
        print(f"\nRaw inventory — {config.REGION_NAME}")
        print("-" * 52)
        for group in sorted(by_group):
            print(f"  {group:<28} {files[group]:>5} files  {by_group[group]:>12,} rows")
        print("-" * 52)
        print(f"  {'TOTAL':<28} {sum(files.values()):>5} files  {total:>12,} rows")
        print()

        if total >= REQUIREMENT:
            print(f"  OK: {total:,} >= {REQUIREMENT:,} required.")
            print("  Put this number in the proposal.")
        else:
            short = REQUIREMENT - total
            print(f"  SHORT by {short:,} rows ({total / REQUIREMENT:.1%} of requirement).")
            print("  Options: widen the date range, widen the bounding box,")
            print("  or add the NDVI point grid rows to the count.")

    # Reference data, deliberately outside the total above.
    if ref_total:
        print(f"\nReference — FIRMS country archive (NOT counted above)")
        print("-" * 52)
        for group in sorted(ref_by_group):
            print(f"  {group:<28} {ref_files[group]:>5} files  "
                  f"{ref_by_group[group]:>12,} rows")
        print("-" * 52)
        print(f"  {'REFERENCE TOTAL':<28} {sum(ref_files.values()):>5} files  "
              f"{ref_total:>12,} rows")
        print()
        print("  NASA-clipped to the country border. Every row is already in")
        print("  firms/sp under a wider bbox, so these are excluded from the")
        print("  count above. Use them to verify the admin clip, not to pad it.")

    # Schema divergence
    if len(headers) > 1:
        print("\nDistinct CSV schemas found (this is your reconciliation problem):\n")
        for i, (hdr, groups) in enumerate(headers.items(), 1):
            uniq = sorted(set(groups))
            print(f"  Schema {i} — used by: {', '.join(uniq)}")
            print(f"    {', '.join(hdr)}\n")

        all_cols = [set(h) for h in headers]
        common = set.intersection(*all_cols)
        divergent = set.union(*all_cols) - common
        print(f"  Common to all schemas ({len(common)}): {', '.join(sorted(common))}")
        print(f"  Divergent ({len(divergent)}): {', '.join(sorted(divergent))}")
        print("\n  The divergent set is what your conformed staging table must harmonise.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--markdown", action="store_true")
    args = ap.parse_args()
    report(args.markdown)