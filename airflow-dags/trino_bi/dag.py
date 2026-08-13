"""Read-only production DAGs for bounded TPC-H and NYC Trino analytics."""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from trino_bi.tasks import run_nyc_bi, run_tpch_bi

DEFAULT_ARGS = {
    "owner": "data-eng-lab",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}
TAGS = ["data-eng-lab", "scenario", "trino", "read-only"]


def _build_dag(*, dag_id: str, description: str, schedule: str, callable_):
    with DAG(
        dag_id=dag_id,
        description=description,
        default_args=DEFAULT_ARGS,
        schedule=schedule,
        start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        tags=TAGS,
    ) as dag:
        PythonOperator(task_id="run_bounded_bi_query", python_callable=callable_)
    return dag


tpch_bi_query = _build_dag(
    dag_id="tpch_bi_query",
    description="Daily provenance-bound TPC-H star-schema BI artifact.",
    schedule="0 1 * * *",
    callable_=run_tpch_bi,
)
nyc_taxi_trino_daily = _build_dag(
    dag_id="nyc_taxi_trino_daily",
    description="Daily snapshot-bound NYC taxi BI artifact.",
    schedule="0 2 * * *",
    callable_=run_nyc_bi,
)

DAGS = {
    "tpch_bi_query": tpch_bi_query,
    "nyc_taxi_trino_daily": nyc_taxi_trino_daily,
}
