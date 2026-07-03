"""Airflow DAG for the streaming_ingest-events scenario (Kafka -> Iceberg bronze)."""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# Structured Streaming is long-running (not a scheduled batch DAG); run the streaming job via the
# notebooks / a streaming spark-submit. Live-gated on Atlas A9 / issue #269.
with DAG(
    dag_id="streaming_ingest_events",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data-eng-lab", "scenario", "streaming"],
) as dag:
    EmptyOperator(task_id="streaming_placeholder")
