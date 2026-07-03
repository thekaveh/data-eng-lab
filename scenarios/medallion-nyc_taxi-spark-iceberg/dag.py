"""Airflow DAG: medallion-nyc_taxi — bronze->silver->gold Iceberg transforms."""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(dag_id="medallion_nyc_taxi", schedule="@daily",
         start_date=pendulum.datetime(2023, 1, 1, tz="UTC"), catchup=False,
         tags=["data-eng-lab", "scenario"]) as dag:
    # Orchestrates the medallion transform: bronze->silver (dedupe) and silver->gold (daily agg).
    # Runs the productionized medallion JAR (Phase 3a) that implements this scenario.
    SparkSubmitOperator(task_id="medallion_transform", conn_id="spark_default",
                        application="s3a://jars/nyc-taxi-etl/0.1.0/app.jar",
                        java_class="com.thekaveh.dataeng.nyctaxi.MedallionTransform")
