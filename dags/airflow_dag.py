"""
Daily orchestration of the fire DWH: extraction, then the seven ETL jobs that
build the stage tables and the dds star schema, then the three NDVI jobs, then
the two mining jobs.

    extract_* (9 tasks, data-extraction-scripts/)  sources -> raw/
    load_stage             raw/firms -> stage.raw_firms_modis / _viirs (incremental)
    clean_stage            -> stage.fire_detection_clean
    load_reference         GADM, NASA country archive -> dds.dim_location
    locate_stage           -> stage.fire_detection_location
    load_wdpa              WDPA -> dds.dim_protected_area (SCD Type 2)
    locate_protected_area  -> stage.fire_detection_protected_area + bridge
    load_dds               -> dds star schema
    load_ndvi              raw/ndvi -> stage.raw_ndvi (incremental)
    impute_ndvi            -> stage.ndvi_clean, cloud gaps imputed (+ ndvi_point)
    load_ndvi_dds          -> dds.fact_ndvi_district_month
    mine_fire_events       fact -> dds.ml_fire_event (+ _detection, _silhouette)
    mine_district_anomalies fact + NDVI -> dds.ml_district_month_anomaly

Every ETL and mining job runs in one transaction, checks its result before committing,
logs itself in meta.event_log and replaces its output on a rerun, so a retry or
a manual "Clear" of any task is safe. The jobs run in this order because each
checks that its input is current (load_dds refuses a stale location table).

To rebuild the warehouse without extracting: in the UI, open a run, click
load_stage and "Clear" with "Downstream" ticked.
"""

import os
import shlex
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'dwh_admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

PYTHON = shlex.quote(sys.executable)

# ---------------------------------------------------------------- extraction
# data-extraction-scripts/ is mounted at EXTRACTION_DIR (see docker-compose.yml)
# and lands everything in its raw/ folder. Every extractor is incremental, so
# after the first multi-hour backfill a daily run only fetches what is new.
# Credentials (FIRMS_MAP_KEY, EARTHDATA_*, EE_PROJECT) arrive as environment
# variables; the Earth Engine login is a read-only mount of
# ~/.config/earthengine.
EXTRACTION_DIR = os.environ.get("EXTRACTION_DIR", "/opt/airflow/data-extraction-scripts")

# (task_id, script and flags, execution timeout), in run_all.sh order.
EXTRACTION_STEPS = [
    ("extract_boundaries",    "extract_boundaries.py --inspect", timedelta(minutes=30)),
    ("extract_wdpa",          "extract_wdpa.py --both",          timedelta(hours=2)),
    ("extract_wdpa_history",  "extract_wdpa_history.py --download", timedelta(hours=1)),
    ("extract_ndvi",          "extract_appeears.py --sync",      timedelta(hours=2)),
    ("extract_landcover",     "extract_landcover.py --sync",     timedelta(hours=1)),
    ("extract_firms_nrt",     "extract_firms_nrt.py",            timedelta(hours=1)),
    ("extract_firms_country", "extract_firms_country.py",        timedelta(hours=1)),
    ("extract_firms_archive", "extract_firms.py",                timedelta(hours=12)),
    ("extraction_inventory",  "inventory.py",                    timedelta(minutes=30)),
]

# ---------------------------------------------------------------- ETL
# The repository root as the containers see it: controllers/ and
# data-extraction-scripts/ are mounted next to each other under it, the same
# layout as on the host, so the jobs find raw/ and the extraction config the
# same way they do when run by hand.
PROJECT_DIR = os.environ.get("DWH_PROJECT_DIR", "/opt/airflow")

# (task_id, script, execution timeout), in README "Running the pipeline" order.
# Timeouts are about 3x the time of a full rebuild on a 4 GB Docker VM with the
# reduced DWH_* memory settings of the Airflow containers.
ETL_STEPS = [
    ("load_stage",            "load_stage.py",            timedelta(hours=1)),
    ("clean_stage",           "clean_stage.py",           timedelta(hours=1)),
    ("load_reference",        "load_reference.py",        timedelta(minutes=30)),
    ("locate_stage",          "locate_stage.py",          timedelta(hours=2)),
    ("load_wdpa",             "load_wdpa.py",             timedelta(minutes=30)),
    ("locate_protected_area", "locate_protected_area.py", timedelta(hours=1)),
    ("load_dds",              "load_dds.py",              timedelta(hours=1)),
]

# ---------------------------------------------------------------- mining
# Both read the finished star schema and rebuild their own dds.ml_* tables
# (docs/mining.md). They need scikit-learn and scipy (requirements.txt).
MINING_STEPS = [
    ("mine_fire_events",        "mine_fire_events.py",        timedelta(hours=1)),
    ("mine_district_anomalies", "mine_district_anomalies.py", timedelta(minutes=30)),
]


# ---------------------------------------------------------------- NDVI
# MOD13Q1 samples -> stage.ndvi_clean with the cloud gaps imputed, then mean
# NDVI per district and month (docs/etl_ndvi.md). impute_ndvi needs the GADM
# tables of load_reference; mine_district_anomalies reads
# dds.fact_ndvi_district_month, so these run before the mining jobs.
NDVI_STEPS = [
    ("load_ndvi",     "load_ndvi.py",     timedelta(minutes=30)),
    ("impute_ndvi",   "impute_ndvi.py",   timedelta(minutes=30)),
    ("load_ndvi_dds", "load_ndvi_dds.py", timedelta(minutes=30)),
]


def extraction_task(task_id, script, timeout, first):
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {shlex.quote(EXTRACTION_DIR)} && {PYTHON} {script}",
        env={"PYTHONUNBUFFERED": "1"},
        append_env=True,
        execution_timeout=timeout,
        retries=2,
        retry_delay=timedelta(minutes=30),
        # Sources are independent: a FIRMS outage must not stop WDPA or NDVI
        # from being collected, so each step runs once the previous one has
        # finished, whether or not it succeeded.
        trigger_rule=TriggerRule.ALL_SUCCESS if first else TriggerRule.ALL_DONE,
    )


def etl_task(task_id, script, timeout):
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {shlex.quote(PROJECT_DIR)} && {PYTHON} controllers/{script}",
        env={"PYTHONUNBUFFERED": "1"},
        append_env=True,
        execution_timeout=timeout,
        # A failed job has rolled itself back, so one retry is safe. It is
        # usually a transient problem (database restarting, out of memory).
        retries=1,
        retry_delay=timedelta(minutes=15),
    )


with DAG(
    'fire_dwh_orchestration_dag',
    default_args=default_args,
    description='Daily extraction, warehouse load and mining: raw files -> stage -> dds star schema -> dds.ml_*',
    doc_md=__doc__,
    schedule_interval='0 2 * * *', # Daily at 02:00 AM
    catchup=False,
    # The first extraction run takes hours, and two warehouse loads must never
    # rebuild the same tables at once, so runs never overlap.
    max_active_runs=1,
    tags=['fire_dwh'],
) as dag:

    extraction = [extraction_task(task_id, script, timeout, first=(i == 0))
                  for i, (task_id, script, timeout) in enumerate(EXTRACTION_STEPS)]
    for upstream, downstream in zip(extraction, extraction[1:]):
        upstream >> downstream

    # The mining and NDVI jobs are ordinary controllers/ scripts, run after load_dds.
    etl = [etl_task(task_id, script, timeout)
           for task_id, script, timeout in ETL_STEPS + NDVI_STEPS + MINING_STEPS]
    for upstream, downstream in zip(etl, etl[1:]):
        upstream >> downstream

    # Staging waits for every extraction step to succeed: a failed source
    # leaves the ETL "upstream_failed", so the run is marked failed in the UI
    # instead of quietly loading stale data, and the next day's run picks up
    # where the extractors stopped.
    extraction >> etl[0]
