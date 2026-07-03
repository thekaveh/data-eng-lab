"""Airflow DAG for the bi_query-tpch-trino scenario (multi-engine Trino read of gold marts)."""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="bi_query_tpch",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data-eng-lab", "scenario"],
) as dag:
    # Trino BI query runs interactively via the notebooks; a scheduled TrinoOperator DAG
    # is a future deliverable (needs Atlas A7 / issue #268).
    EmptyOperator(task_id="bi_query_placeholder")
