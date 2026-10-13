import psycopg2
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
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

# Define Airflow DAG
with DAG(
    'fire_dwh_orchestration_dag',
    default_args=default_args,
    description='Automated orchestration of Wildfire Data Warehouse ETL and ML pipelines',
    schedule_interval='0 2 * * *', # Daily at 02:00 AM
    catchup=False
) as dag:

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

    # Workflow Dependency Chain
    t1 >> t2 >> t3
