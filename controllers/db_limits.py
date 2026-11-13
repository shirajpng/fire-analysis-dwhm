"""
Memory and parallelism settings for the heavy ETL jobs, from the environment.

The defaults are the values the jobs were tuned with (run by hand, nothing
else using the Docker VM's memory). Under Airflow, the scheduler and webserver
share the VM; with 8 workers x 256 MB the Postgres backend for locate_stage.py
was killed by the out-of-memory killer on a 4 GB VM. docker-compose.yml
therefore sets lower values for the Airflow containers.

    DWH_WORK_MEM              work_mem per sort / hash node      (default 256MB)
    DWH_MAINTENANCE_WORK_MEM  for index builds                   (default 1GB)
    DWH_PARALLEL_WORKERS      max_parallel_workers_per_gather    (default 8)

The values only change speed and memory use, never results.
"""

from __future__ import annotations

import os
import re

_MEM = re.compile(r"^\d+(kB|MB|GB)$")


def _mem(name: str, default: str) -> str:
    value = os.getenv(name, default).strip()
    if not _MEM.match(value):
        raise SystemExit(f"{name}={value!r}: expected a size such as 128MB or 1GB")
    return value


def _int(name: str, default: int) -> int:
    value = os.getenv(name, str(default)).strip()
    if not value.isdigit():
        raise SystemExit(f"{name}={value!r}: expected a whole number")
    return int(value)


WORK_MEM = _mem("DWH_WORK_MEM", "256MB")
MAINTENANCE_WORK_MEM = _mem("DWH_MAINTENANCE_WORK_MEM", "1GB")
PARALLEL_WORKERS = _int("DWH_PARALLEL_WORKERS", 8)


def session_sql(*, parallel: bool = False, maintenance: bool = False) -> str:
    """SET LOCAL statements for the current transaction."""
    lines = [f"SET LOCAL work_mem = '{WORK_MEM}';"]
    if maintenance:
        lines.append(f"SET LOCAL maintenance_work_mem = '{MAINTENANCE_WORK_MEM}';")
    if parallel:
        lines.append(f"SET LOCAL max_parallel_workers_per_gather = {PARALLEL_WORKERS};")
    return "\n".join(lines) + "\n"


def describe() -> str:
    return f"work_mem={WORK_MEM} maintenance_work_mem={MAINTENANCE_WORK_MEM} parallel_workers={PARALLEL_WORKERS}"
