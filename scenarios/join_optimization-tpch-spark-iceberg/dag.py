"""Airflow DAG for the join_optimization-tpch scenario (broadcast vs sort-merge)."""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="join_optimization_tpch",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data-eng-lab", "scenario"],
) as dag:
    # The join optimization analysis lives in the Scala/PySpark notebooks. This placeholder
    # will be replaced with a productionized JAR in a future phase.
    EmptyOperator(task_id="join_optimization_placeholder")
