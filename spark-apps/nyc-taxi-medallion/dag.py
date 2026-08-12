"""Airflow DAG: run the nyc-taxi-medallion Spark job (JAR published to s3a://jars via Jenkins)."""

from __future__ import annotations

import json
import os
import re
import urllib.request
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from atlas_spark_utils import RestConfirmingSparkHook

SCALES = ("tiny", "small", "medium")
DATASET_RESOLVER_URI = os.environ.get("DATASET_RESOLVER_URI", "http://dataset-resolver:8080")
_EXPECTED_NAMES = {
    "tiny": ("yellow_tripdata_2023-01.parquet",),
    "small": tuple(f"yellow_tripdata_2023-{month:02d}.parquet" for month in range(1, 4)),
    "medium": tuple(f"yellow_tripdata_2023-{month:02d}.parquet" for month in range(1, 7)),
}
_OBJECT_FIELDS = {"object_name", "uri", "size_bytes", "sha256", "schema_id"}


def _effective_scale(context) -> str:
    dag_run = context.get("dag_run") if context else None
    conf = getattr(dag_run, "conf", None) or {}
    explicit = conf.get("dataset_scale")
    scale = explicit if explicit is not None else os.environ.get("DATASET_SCALE", "small")
    if scale not in SCALES:
        raise ValueError("dataset scale must be one of: tiny, small, medium")
    return scale


def _resolve_dataset(dataset: str, scale: str) -> tuple[str, ...]:
    request = urllib.request.Request(
        DATASET_RESOLVER_URI.rstrip("/") + "/v1/resolve",
        data=json.dumps({"dataset": dataset, "expected_scale": scale}, separators=(",", ":"), sort_keys=True).encode(
            "utf-8"
        ),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            body = response.read((1 << 20) + 1)
        if len(body) > 1 << 20:
            raise ValueError
        document = json.loads(body)
        if set(document) != {"dataset", "scale", "plan_id", "manifest_sha256", "publication_id", "objects"}:
            raise ValueError
        if document["dataset"] != dataset or document["scale"] != scale:
            raise ValueError
        plan, publication = document["plan_id"], document["publication_id"]
        if (
            re.fullmatch(r"[0-9a-f]{64}", plan or "") is None
            or re.fullmatch(r"[0-9a-f]{32}", publication or "") is None
        ):
            raise ValueError
        if re.fullmatch(r"[0-9a-f]{64}", document["manifest_sha256"] or "") is None:
            raise ValueError
        objects = document["objects"]
        if not isinstance(objects, list) or any(
            not isinstance(item, dict)
            or set(item) != _OBJECT_FIELDS
            or isinstance(item["size_bytes"], bool)
            or not isinstance(item["size_bytes"], int)
            or item["size_bytes"] < 0
            or re.fullmatch(r"[0-9a-f]{64}", item["sha256"] or "") is None
            or not isinstance(item["schema_id"], str)
            or not item["schema_id"]
            for item in objects
        ):
            raise ValueError
        names = tuple(item.get("object_name") for item in objects)
        if names != _EXPECTED_NAMES[scale] or len(names) != len(set(names)):
            raise ValueError
        prefix = f"s3://landing/{dataset}/_generations/{plan}/{publication}/"
        uris = tuple(item.get("uri") for item in objects)
        if any(uri != prefix + name for uri, name in zip(uris, names, strict=True)):
            raise ValueError
        return uris
    except Exception as error:
        raise ValueError("dataset resolution failed") from error


class AtlasSparkSubmitOperator(SparkSubmitOperator):
    """Spark submit operator with standalone-driver REST confirmation."""

    def __init__(self, *, rest_host: str = "spark-master", dataset: str, target_option: str, target: str, **kwargs):
        super().__init__(**kwargs)
        self.rest_host = rest_host
        self.dataset = dataset
        self.target_option = target_option
        self.target = target

    def _get_hook(self):
        return RestConfirmingSparkHook(
            super()._get_hook(),
            rest_host=self.rest_host,
        )

    def execute(self, context):
        immutable_uris = _resolve_dataset(self.dataset, _effective_scale(context))
        self.application_args = [*immutable_uris, self.target_option, self.target]
        return super().execute(context)


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
    "spark.app.name": "nyc-taxi-medallion",
    "spark.executor.memory": "1g",
    "spark.driver.memory": "1g",
    "spark.standalone.submit.waitAppCompletion": "true",
    "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.endpoint.region": REGION,
    "spark.hadoop.fs.s3a.access.key": os.environ.get("MINIO_ROOT_USER", ""),
    "spark.hadoop.fs.s3a.secret.key": os.environ.get("MINIO_ROOT_PASSWORD", ""),
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.aws.credentials.provider": ("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"),
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
    "spark.sql.catalog.lakehouse.s3.access-key-id": os.environ.get("MINIO_ICEBERG_ACCESS_KEY", ""),
    "spark.sql.catalog.lakehouse.s3.secret-access-key": os.environ.get("MINIO_ICEBERG_SECRET_KEY", ""),
    "spark.sql.catalog.lakehouse.client.region": REGION,
    "spark.eventLog.enabled": "true",
    "spark.eventLog.dir": "s3a://spark-history/",
}


with DAG(
    dag_id="nyc_taxi_medallion",
    description="Daily medallion transformation: aggregate nyc-taxi trips from bronze to silver & gold.",
    default_args=default_args,
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data-eng-lab", "scenario"],
) as dag:
    AtlasSparkSubmitOperator(
        task_id="submit_nyc_taxi_medallion",
        conn_id="spark_default",
        application="s3a://jars/nyc-taxi-medallion/0.1.0/app.jar",
        java_class="com.thekaveh.dataeng.medallion.NycTaxiMedallion",
        deploy_mode="cluster",
        conf=spark_conf,
        application_args=[],
        dataset="nyc_taxi",
        target_option="--bronze-table",
        target="lakehouse.bronze.nyc_taxi_trips",
        rest_host="spark-master",
        verbose=True,
    )
