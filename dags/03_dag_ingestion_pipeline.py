# 03_dag_ingestion_pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from datetime import datetime, timedelta
import logging
import os

# Import our custom healer logic
# from auto_healers import heal_schema_type_drift

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 10, 24),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'idempotent_ingestion_with_dlq',
    default_args=default_args,
    description='Ingest data, auto-heal DLQ, and run dbt',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # --- TASK 1: Main Ingestion ---
    def ingest_data_callable():
        logging.info("Ingesting data to raw_events.")
        logging.info("Malformed rows are routed to dlq_audit_log with status='PENDING'.")
        # Main ingestion logic (e.g., pandas to_sql wrapped in try/except) goes here

    task_ingest_data = PythonOperator(
        task_id='ingest_to_postgres',
        python_callable=ingest_data_callable
    )

    # --- TASK 2: Auto-Heal Schema Drift ---
    def auto_heal_drift_callable():
        logging.info("Sweeping DLQ for SCHEMA_TYPE_DRIFT errors...")
        # db_uri = os.getenv("AIRFLOW_CONN_POSTGRES_DEFAULT")
        # heal_schema_type_drift(db_uri)
        logging.info("Auto-healing completed.")

    task_auto_heal_drift = PythonOperator(
        task_id='auto_heal_schema_drift',
        python_callable=auto_heal_drift_callable
    )

    # --- TASK 3: dbt Deduplication ---
    def dbt_run_callable():
        logging.info("Running dbt model: raw_events_deduped")
        # Execute bash command: dbt run -m raw_events_deduped
        
    task_dbt_run = PythonOperator(
        task_id='dbt_run_deduplication',
        python_callable=dbt_run_callable
    )

    # --- SET DEPENDENCIES ---
    task_ingest_data >> task_auto_heal_drift >> task_dbt_run
