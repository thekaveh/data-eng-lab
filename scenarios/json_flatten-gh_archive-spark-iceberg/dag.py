"""Airflow DAG for the json_flatten-gh_archive scenario (nested JSON -> typed columns)."""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="json_flatten_gh_archive",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data-eng-lab", "scenario"],
) as dag:
    # The JSON flatten transform currently lives in the Scala/PySpark notebooks. A productionized
    # JAR is a future Phase-3 deliverable; this is a placeholder task until then.
    EmptyOperator(task_id="json_flatten_placeholder")
