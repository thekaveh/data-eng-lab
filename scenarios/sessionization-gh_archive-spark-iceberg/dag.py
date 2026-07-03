"""Airflow DAG for the sessionization-gh_archive scenario."""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="sessionization_gh_archive",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data-eng-lab", "scenario"],
) as dag:
    # Sessionization (gap-based detection on 30-min inactivity) runs in the notebooks.
    # This is a placeholder task until productionized.
    EmptyOperator(task_id="sessionization_placeholder")
