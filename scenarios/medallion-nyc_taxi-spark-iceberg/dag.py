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
    # Productionized as spark-apps/nyc-taxi-medallion (dag_id nyc_taxi_medallion). This scenario
    # placeholder demonstrates the transform via the notebooks.
    EmptyOperator(task_id="medallion_placeholder")
