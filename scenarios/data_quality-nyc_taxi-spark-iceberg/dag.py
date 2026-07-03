"""Airflow DAG for the data_quality-nyc_taxi scenario."""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="data_quality_nyc_taxi",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data-eng-lab", "scenario"],
) as dag:
    # Data quality validation (split into clean and quarantine tables) runs in the notebooks.
    # This is a placeholder task until productionized.
    EmptyOperator(task_id="data_quality_placeholder")
