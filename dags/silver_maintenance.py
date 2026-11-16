# Airflow DAG that cleans the Iceberg tables once a week.
# It expires old snapshots and drops audit branches nobody uses.
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG

try:
    from airflow.providers.standard.operators.python import ExternalPythonOperator
except ImportError:
    from airflow.operators.python import ExternalPythonOperator

from jobs.silver import tasks

VENV = "/opt/venvs/silver/bin/python"

with DAG(
    dag_id="silver_maintenance",
    description="Weekly Iceberg maintenance (compaction + snapshot expiry)",
    default_args={"owner": "silver", "retries": 1, "retry_delay": timedelta(minutes=5)},
    start_date=datetime(2026, 7, 16),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    tags=["silver", "iceberg", "maintenance"],
) as dag:

    ExternalPythonOperator(
        task_id="run_maintenance",
        python=VENV,
        python_callable=tasks.run_maintenance,
        expect_airflow=False,
        pool="silver_pool",
    )
