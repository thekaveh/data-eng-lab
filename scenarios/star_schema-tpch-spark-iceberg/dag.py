"""Airflow DAG for the star_schema-tpch scenario (dimensional modeling with TPC-H)."""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="star_schema_tpch",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data-eng-lab", "scenario"],
) as dag:
    # The star schema transform currently lives in the Scala/PySpark notebooks. A productionized
    # JAR is a future Phase-3 deliverable; this is a placeholder task until then.
    EmptyOperator(task_id="star_schema_placeholder")
