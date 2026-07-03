"""Airflow DAG: batch_ingest-nyc_taxi — land Parquet into Iceberg bronze (via the PySpark notebook logic)."""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(dag_id="batch_ingest_nyc_taxi", schedule="@daily",
         start_date=pendulum.datetime(2023, 1, 1, tz="UTC"), catchup=False,
         tags=["data-eng-lab", "scenario"]) as dag:
    # Runs the productionized nyc-taxi-etl JAR (Phase 3a) that implements this scenario.
    SparkSubmitOperator(task_id="ingest", conn_id="spark_default",
                        application="s3a://jars/nyc-taxi-etl/0.1.0/app.jar",
                        java_class="com.thekaveh.dataeng.nyctaxi.NycTaxiEtl")
