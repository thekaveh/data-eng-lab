"""Paused manual-only checkpoint retention planning DAG."""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from data_eng_lab_airflow_dags.checkpoint_retention.tasks import run_retention_plans

DEFAULT_ARGS = {
    "owner": "data-eng-lab",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="checkpoint_retention",
    description="Manual dry-run inventory for the fixed checkpoint ownership registry.",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["data-eng-lab", "checkpoint", "manual", "dry-run"],
) as checkpoint_retention:
    PythonOperator(
        task_id="plan_checkpoint_retention",
        python_callable=run_retention_plans,
        execution_timeout=timedelta(minutes=5),
    )
