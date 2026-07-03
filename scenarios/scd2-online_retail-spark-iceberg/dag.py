"""Airflow DAG for the scd2-online_retail scenario (Slowly Changing Dimension Type 2)."""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="scd2_online_retail",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data-eng-lab", "scenario"],
) as dag:
    # The SCD2 transform currently lives in the Scala/PySpark notebooks. A productionized
    # JAR is a future Phase deliverable; this is a placeholder task until then.
    EmptyOperator(task_id="scd2_placeholder")
