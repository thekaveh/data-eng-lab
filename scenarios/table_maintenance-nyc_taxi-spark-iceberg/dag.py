"""Airflow DAG for the table_maintenance-nyc_taxi scenario (compaction, expiry, orphan cleanup)."""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="table_maintenance_nyc_taxi",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data-eng-lab", "scenario"],
) as dag:
    # The table-maintenance pipeline currently lives in the Scala/PySpark notebooks. A productionized
    # table-maintenance JAR is a future Phase-3 deliverable; this is a placeholder task until then.
    EmptyOperator(task_id="table_maintenance_placeholder")
