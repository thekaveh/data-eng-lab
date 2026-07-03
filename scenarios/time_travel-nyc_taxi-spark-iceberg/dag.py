"""Airflow DAG for the time_travel-nyc_taxi scenario (snapshots, VERSION AS OF, rollback, WAP)."""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="time_travel_nyc_taxi",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data-eng-lab", "scenario"],
) as dag:
    # The time-travel demo currently lives in the Scala/PySpark notebooks. A productionized
    # time-travel pipeline is a future Phase-3 deliverable; this is a placeholder task until then.
    EmptyOperator(task_id="time_travel_placeholder")
