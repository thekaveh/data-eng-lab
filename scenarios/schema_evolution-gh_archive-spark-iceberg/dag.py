"""Airflow DAG for the schema_evolution-gh_archive scenario (add/rename columns)."""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="schema_evolution_gh_archive",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data-eng-lab", "scenario"],
) as dag:
    # Schema evolution via Spark SQL notebooks; productionized JAR is a future Phase-3 deliverable.
    EmptyOperator(task_id="schema_evolution_placeholder")
