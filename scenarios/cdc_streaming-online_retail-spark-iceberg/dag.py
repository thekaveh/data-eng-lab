"""Airflow DAG for the cdc_streaming-online_retail scenario (streaming CDC via foreachBatch MERGE)."""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# Structured Streaming is long-running (not a scheduled batch DAG); live-gated on Atlas A9 / issue #269.
with DAG(
    dag_id="cdc_streaming_online_retail",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data-eng-lab", "scenario", "streaming"],
) as dag:
    EmptyOperator(task_id="streaming_placeholder")
