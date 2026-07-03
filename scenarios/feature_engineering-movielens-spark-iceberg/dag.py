"""Airflow DAG for the feature_engineering-movielens scenario (ML feature marts)."""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="feature_engineering_movielens",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data-eng-lab", "scenario"],
) as dag:
    # The feature engineering pipeline lives in the Scala/PySpark notebooks. This placeholder
    # will be replaced with a productionized JAR in a future phase.
    EmptyOperator(task_id="feature_engineering_placeholder")
