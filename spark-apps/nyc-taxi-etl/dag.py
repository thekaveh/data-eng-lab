"""Airflow DAG: run the nyc-taxi-etl JAR (published to s3a://jars) on the Spark cluster."""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="nyc_taxi_etl",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data-eng-lab", "spark-app"],
) as dag:
    SparkSubmitOperator(
        task_id="submit_nyc_taxi_etl",
        conn_id="spark_default",
        application="s3a://jars/nyc-taxi-etl/0.1.0/app.jar",
        java_class="com.thekaveh.dataeng.nyctaxi.NycTaxiEtl",
        conf={
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.path.style.access": "true",
        },
    )
