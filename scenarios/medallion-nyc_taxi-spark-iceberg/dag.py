"""Airflow DAG for the medallion-nyc_taxi scenario (bronze->silver->gold)."""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="medallion_nyc_taxi",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data-eng-lab", "scenario"],
) as dag:
    # The medallion transform currently lives in the Scala/PySpark notebooks. A productionized
    # medallion JAR is a future Phase-3 deliverable; this is a placeholder task until then.
    EmptyOperator(task_id="medallion_placeholder")
