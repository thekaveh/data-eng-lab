"""Airflow DAG for the federated_query-nyc_taxi scenario (Trino federated query)."""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="federated_query_nyc_taxi",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data-eng-lab", "scenario"],
) as dag:
    # Trino federated query runs interactively via the notebooks; a scheduled
    # TrinoOperator DAG is a future deliverable (needs Atlas A7 / issue #268).
    EmptyOperator(task_id="federated_query_placeholder")
