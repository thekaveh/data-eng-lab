"""Airflow DAG: run the nyc-taxi-etl Spark job (JAR published to s3a://jars via Jenkins)."""
from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

REGION = os.environ.get("MINIO_REGION", "us-east-1")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
ICEBERG_REST_URI = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")
LAKEHOUSE_BUCKET = os.environ.get("MINIO_BUCKET_ICEBERG_LAKEHOUSE", "lakehouse")


default_args = {
    "owner": "data-eng-lab",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


spark_conf = {
    "spark.master": os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"),
    "spark.app.name": "nyc-taxi-etl",
    "spark.executor.memory": "1g",
    "spark.driver.memory": "1g",
    "spark.standalone.submit.waitAppCompletion": "true",
    "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.endpoint.region": REGION,
    "spark.hadoop.fs.s3a.access.key": os.environ.get("MINIO_ROOT_USER", ""),
    "spark.hadoop.fs.s3a.secret.key": os.environ.get("MINIO_ROOT_PASSWORD", ""),
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.aws.credentials.provider": (
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    ),
    "spark.driverEnv.AWS_ACCESS_KEY_ID": os.environ.get("MINIO_ROOT_USER", ""),
    "spark.driverEnv.AWS_SECRET_ACCESS_KEY": os.environ.get("MINIO_ROOT_PASSWORD", ""),
    "spark.driverEnv.AWS_REGION": REGION,
    "spark.driverEnv.AWS_ENDPOINT_URL_S3": MINIO_ENDPOINT,
    "spark.executorEnv.AWS_ACCESS_KEY_ID": os.environ.get("MINIO_ROOT_USER", ""),
    "spark.executorEnv.AWS_SECRET_ACCESS_KEY": os.environ.get("MINIO_ROOT_PASSWORD", ""),
    "spark.executorEnv.AWS_REGION": REGION,
    "spark.executorEnv.AWS_ENDPOINT_URL_S3": MINIO_ENDPOINT,
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lakehouse.type": "rest",
    "spark.sql.catalog.lakehouse.uri": ICEBERG_REST_URI,
    "spark.sql.catalog.lakehouse.warehouse": f"s3a://{LAKEHOUSE_BUCKET}/",
    "spark.sql.catalog.lakehouse.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.lakehouse.s3.endpoint": MINIO_ENDPOINT,
    "spark.sql.catalog.lakehouse.s3.path-style-access": "true",
    "spark.sql.catalog.lakehouse.s3.access-key-id": os.environ.get(
        "MINIO_ICEBERG_ACCESS_KEY", ""
    ),
    "spark.sql.catalog.lakehouse.s3.secret-access-key": os.environ.get(
        "MINIO_ICEBERG_SECRET_KEY", ""
    ),
    "spark.sql.catalog.lakehouse.client.region": REGION,
    "spark.eventLog.enabled": "true",
    "spark.eventLog.dir": "s3a://spark-history/",
}


with DAG(
    dag_id="nyc_taxi_etl",
    description="Daily ETL: load nyc-taxi landing data into lakehouse.bronze.nyc_taxi_trips.",
    default_args=default_args,
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data-eng-lab", "scenario"],
) as dag:
    # Cluster-mode caveat (atlas#308, partially addressed): the master's REST API
    # (:6066) is enabled upstream, but SparkSubmitHook polls driver status via the
    # spark_default connection (port 7077, required for submission), so the poll
    # fails after submit and can mark this task failed even when the job succeeded.
    # spark.standalone.submit.waitAppCompletion above is the real completion signal;
    # see docs/atlas-feedback-go-live.md ("2026-07-21 live verification").
    SparkSubmitOperator(
        task_id="submit_nyc_taxi_etl",
        conn_id="spark_default",
        application="s3a://jars/nyc-taxi-etl/0.1.0/app.jar",
        java_class="com.thekaveh.dataeng.nyctaxi.NycTaxiEtl",
        deploy_mode="cluster",
        conf=spark_conf,
        application_args=[
            "s3a://landing/nyc_taxi/",
            "lakehouse.bronze.nyc_taxi_trips",
        ],
        verbose=True,
    )
