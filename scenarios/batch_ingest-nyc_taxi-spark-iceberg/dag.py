"""Airflow DAG: batch_ingest-nyc_taxi — placeholder for the batch ingest scenario."""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="batch_ingest_nyc_taxi",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data-eng-lab", "scenario"],
) as dag:
    # The productionized DAG for this scenario is spark-apps/nyc-taxi-etl/dag.py (dag_id nyc_taxi_etl),
    # which carries the full lakehouse catalog conf. This placeholder avoids a second, conf-less JAR submission.
    EmptyOperator(task_id="batch_ingest_placeholder")
