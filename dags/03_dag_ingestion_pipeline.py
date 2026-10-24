# 03_dag_ingestion_pipeline.py
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Defines the root path so Airflow knows where to find the plugins folder
PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/airflow/dags/idempotent-ingestion-project")

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 10, 24),
    "email_on_failure": True,  # Alerts the team if the dbt circuit breaker trips
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "idempotent_ingestion_with_dlq",
    default_args=default_args,
    description="Ingest data, auto-heal DLQ, and run dbt build (Circuit Breaker)",
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # --- TASK 1: Main Ingestion ---
    # Executes the Python script in the plugins folder
    # Handles PII masking, additive/destructive drift, and DLQ routing.
    task_ingest_data = BashOperator(
        task_id="ingest_to_postgres", bash_command=f"python {PROJECT_ROOT}/plugins/ingest_to_postgres.py"
    )

    # --- TASK 2: Auto-Heal Schema Drift ---
    # Executes the second Python script in the plugins folder
    # Sweeps the DLQ, repairs type collisions, and re-inserts them.
    task_auto_heal_drift = BashOperator(
        task_id="auto_heal_schema_drift", bash_command=f"python {PROJECT_ROOT}/plugins/auto_heal_schema_drift.py"
    )

    # --- TASK 3: dbt Circuit Breaker ---
    # Upgraded from legacy `dbt run` to `dbt build`.
    # Runs incremental deduplication, referential integrity tests, and mart building.
    task_dbt_build = BashOperator(
        task_id="dbt_build_circuit_breaker", bash_command=f"cd {PROJECT_ROOT}/transform && dbt deps && dbt build"
    )

    # --- SET DEPENDENCIES ---
    task_ingest_data >> task_auto_heal_drift >> task_dbt_build
