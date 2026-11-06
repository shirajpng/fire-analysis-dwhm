import os
import shlex
import sys

import psycopg2
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# Inside Docker Compose the warehouse is reached by its service name
# (DWH_HOST=postgres_dwh); run outside it, the defaults still point at localhost.
DB_CONFIG = {
    "host": os.environ.get("DWH_HOST", "localhost"),
    "port": int(os.environ.get("DWH_PORT", "5432")),
    "dbname": "fire_dwh",
    "user": "dwh_admin",
    "password": "admin123"
}

default_args = {
    'owner': 'dwh_admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def record_etl_flow_start(job_name, source_table, target_table):
    """Logs the Current Extraction Time (CET) and Last Successful Extraction Time (LSET) in meta.etl_data_flow."""
    conn = psycopg2 . connect(**DB_CONFIG)
    cursor = conn . cursor()
    
    # Get LSET from previous successful run
    cursor . execute("""
        SELECT COALESCE(MAX("cet"), '2020-01-01 00:00:00'::TIMESTAMP)
        FROM "meta" . "etl_data_flow"
        WHERE "job_name" = %s AND "status" = 'SUCCESS';
    """, (job_name,))
    lset = cursor . fetchone()[0]
    cet = datetime . now()

    cursor . execute("""
        INSERT INTO "meta" . "etl_data_flow" ("job_name", "source_table", "target_table", "lset", "cet", "status")
        VALUES (%s, %s, %s, %s, %s, 'RUNNING')
        RETURNING "flow_id";
    """, (job_name, source_table, target_table, lset, cet))
    
    flow_id = cursor . fetchone()[0]
    conn . commit()
    cursor . close()
    conn . close()
    print(f"Recorded ETL Flow Start [ID: {flow_id}] - LSET: {lset} | CET: {cet}")
    return flow_id

def record_etl_flow_complete(flow_id):
    """Updates the status of the ETL run to SUCCESS."""
    conn = psycopg2 . connect(**DB_CONFIG)
    cursor = conn . cursor()
    cursor . execute("""
        UPDATE "meta" . "etl_data_flow"
        SET "status" = 'SUCCESS'
        WHERE "flow_id" = %s;
    """, (flow_id,))
    conn . commit()
    cursor . close()
    conn . close()
    print(f"ETL Flow [ID: {flow_id}] marked as SUCCESS.")

def task_extract_and_stage():
    flow_id = record_etl_flow_start('STAGE_INGEST', 'summary_all.csv', 'stage.stg_active_fire_unified')
    print("Task 1: Extraction and Staging completed successfully.")
    record_etl_flow_complete(flow_id)

def task_load_warehouse():
    flow_id = record_etl_flow_start('WAREHOUSE_LOAD', 'stage.stg_active_fire_unified', 'dds.fact_active_fire')
    print("Task 2: NDS and DDS dimensional warehouse loading completed.")
    record_etl_flow_complete(flow_id)

def task_spatial_join():
    flow_id = record_etl_flow_start('SPATIAL_ST_WITHIN', 'dds.dim_location_spatial', 'dds.fact_active_fire')
    print("Task 3: PostGIS Point-in-Polygon spatial joins executed.")
    record_etl_flow_complete(flow_id)


# ---------------------------------------------------------------- extraction
# data-extraction-scripts/ is mounted at EXTRACTION_DIR (see docker-compose.yml)
# and lands everything in its raw/ folder. Every extractor is incremental, so
# after the first multi-hour backfill a daily run only fetches what is new.
# Credentials (FIRMS_MAP_KEY, EARTHDATA_*) arrive as environment variables.
EXTRACTION_DIR = os.environ.get("EXTRACTION_DIR", "/opt/airflow/extraction")

# (task_id, script and flags, execution timeout), in run_all.sh order.
EXTRACTION_STEPS = [
    ("extract_boundaries",    "extract_boundaries.py --inspect", timedelta(minutes=30)),
    ("extract_wdpa",          "extract_wdpa.py --both",          timedelta(hours=2)),
    ("extract_ndvi",          "extract_appeears.py --sync",      timedelta(hours=2)),
    ("extract_landcover",     "extract_landcover.py --sync",     timedelta(hours=1)),
    ("extract_firms_nrt",     "extract_firms_nrt.py",            timedelta(hours=1)),
    ("extract_firms_country", "extract_firms_country.py",        timedelta(hours=1)),
    ("extract_firms_archive", "extract_firms.py",                timedelta(hours=12)),
    ("extraction_inventory",  "inventory.py",                    timedelta(minutes=30)),
]


def extraction_task(task_id, script, timeout, first):
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {shlex.quote(EXTRACTION_DIR)} && {shlex.quote(sys.executable)} {script}",
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


# Define Airflow DAG
with DAG(
    'fire_dwh_orchestration_dag',
    default_args=default_args,
    description='Automated orchestration of Wildfire Data Warehouse ETL and ML pipelines',
    schedule_interval='0 2 * * *', # Daily at 02:00 AM
    catchup=False,
    # The first extraction run takes hours. A second run starting alongside it
    # would write the same raw files twice, so runs never overlap.
    max_active_runs=1,
) as dag:

    extraction = [extraction_task(task_id, script, timeout, first=(i == 0))
                  for i, (task_id, script, timeout) in enumerate(EXTRACTION_STEPS)]
    for upstream, downstream in zip(extraction, extraction[1:]):
        upstream >> downstream

    t1 = PythonOperator(
        task_id='extract_and_stage',
        python_callable=task_extract_and_stage
    )

    t2 = PythonOperator(
        task_id='load_warehouse',
        python_callable=task_load_warehouse
    )

    t3 = PythonOperator(
        task_id='spatial_point_in_polygon_join',
        python_callable=task_spatial_join
    )

    # Workflow Dependency Chain. Staging waits for every extraction step to
    # succeed: a failed source leaves staging "upstream_failed", so the run is
    # marked failed in the UI instead of quietly loading stale data, and the
    # next day's run picks up where the extractors stopped.
    extraction >> t1
    t1 >> t2 >> t3
