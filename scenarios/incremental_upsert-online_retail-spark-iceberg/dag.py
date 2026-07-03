"""Airflow DAG for the incremental_upsert-online_retail scenario (MERGE INTO upserts, CDC)."""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="incremental_upsert_online_retail",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data-eng-lab", "scenario"],
) as dag:
    # The incremental upsert transform currently lives in the Scala/PySpark notebooks. A productionized
    # JAR is a future Phase deliverable; this is a placeholder task until then.
    EmptyOperator(task_id="incremental_upsert_placeholder")
