"""Airflow DAG for the streaming_ingest-gh_archive scenario (file-source Structured Streaming -> Iceberg)."""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="streaming_ingest_gh_archive",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data-eng-lab", "scenario"],
) as dag:
    # File-source Structured Streaming via Spark SQL notebooks; productionized JAR is a future Phase-3 deliverable.
    EmptyOperator(task_id="streaming_ingest_placeholder")
