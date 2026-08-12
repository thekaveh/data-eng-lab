"""Airflow DAG: build resolver-verified TPC-H star-schema Iceberg tables."""

from __future__ import annotations

import json
import os
import re
import urllib.request
from datetime import timedelta
from typing import NamedTuple

import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from atlas_spark_utils import RestConfirmingSparkHook

SCALES = ("tiny", "small", "medium")
DATASET_RESOLVER_URI = os.environ.get("DATASET_RESOLVER_URI", "http://dataset-resolver:8080")
EXPECTED_NAMES = (
    "customer.parquet",
    "lineitem.parquet",
    "nation.parquet",
    "orders.parquet",
    "part.parquet",
    "partsupp.parquet",
    "region.parquet",
    "supplier.parquet",
)
_OBJECT_FIELDS = {"object_name", "uri", "size_bytes", "sha256", "schema_id"}
_RESULT_FIELDS = {"dataset", "scale", "plan_id", "manifest_sha256", "publication_id", "objects"}
_MAX_RESOLUTION_BYTES = 1 << 20
_MAX_JSON_DEPTH = 16


class Resolution(NamedTuple):
    uris: tuple[str, ...]
    scale: str
    plan_id: str
    publication_id: str
    manifest_sha256: str


def _unique_mapping(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("dataset resolution failed")
        result[key] = value
    return result


def _reject_constant(_value):
    raise ValueError("dataset resolution failed")


def _json_depth(value) -> int:
    if isinstance(value, dict):
        return 1 + max((_json_depth(item) for item in value.values()), default=0)
    if isinstance(value, list):
        return 1 + max((_json_depth(item) for item in value), default=0)
    return 0


def _effective_scale(context) -> str:
    dag_run = context.get("dag_run") if context else None
    conf = getattr(dag_run, "conf", None) or {}
    explicit = conf.get("dataset_scale")
    scale = explicit if explicit is not None else os.environ.get("DATASET_SCALE", "small")
    if scale not in SCALES:
        raise ValueError("dataset scale must be one of: tiny, small, medium")
    return scale


def _resolve_dataset(dataset: str, scale: str) -> Resolution:
    request = urllib.request.Request(
        DATASET_RESOLVER_URI.rstrip("/") + "/v1/resolve",
        data=json.dumps({"dataset": dataset, "expected_scale": scale}, separators=(",", ":"), sort_keys=True).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            body = response.read(_MAX_RESOLUTION_BYTES + 1)
        if len(body) > _MAX_RESOLUTION_BYTES:
            raise ValueError
        document = json.loads(body, object_pairs_hook=_unique_mapping, parse_constant=_reject_constant)
        if not isinstance(document, dict) or set(document) != _RESULT_FIELDS or _json_depth(document) > _MAX_JSON_DEPTH:
            raise ValueError
        if document["dataset"] != dataset or document["scale"] != scale:
            raise ValueError
        plan, publication, manifest = document["plan_id"], document["publication_id"], document["manifest_sha256"]
        if not isinstance(plan, str) or re.fullmatch(r"[0-9a-f]{64}", plan) is None:
            raise ValueError
        if (
            not isinstance(publication, str)
            or re.fullmatch(r"[0-9a-f]{12}4[0-9a-f]{3}[89ab][0-9a-f]{15}", publication) is None
        ):
            raise ValueError
        if not isinstance(manifest, str) or re.fullmatch(r"[0-9a-f]{64}", manifest) is None:
            raise ValueError
        objects = document["objects"]
        if not isinstance(objects, list) or not objects or len(objects) != len(EXPECTED_NAMES):
            raise ValueError
        if any(
            not isinstance(item, dict)
            or set(item) != _OBJECT_FIELDS
            or not isinstance(item["object_name"], str)
            or isinstance(item["size_bytes"], bool)
            or not isinstance(item["size_bytes"], int)
            or item["size_bytes"] <= 0
            or not isinstance(item["sha256"], str)
            or re.fullmatch(r"[0-9a-f]{64}", item["sha256"]) is None
            or not isinstance(item["schema_id"], str)
            or not item["schema_id"]
            or not isinstance(item["uri"], str)
            for item in objects
        ):
            raise ValueError
        names = tuple(item["object_name"] for item in objects)
        if names != EXPECTED_NAMES or len(names) != len(set(names)):
            raise ValueError
        if tuple(item["schema_id"] for item in objects) != tuple(
            name.removesuffix(".parquet") for name in EXPECTED_NAMES
        ):
            raise ValueError
        prefix = f"s3://landing/{dataset}/_generations/{plan}/{publication}/"
        uris = tuple(item["uri"] for item in objects)
        if any(uri != prefix + name for uri, name in zip(uris, names, strict=True)):
            raise ValueError
        return Resolution(uris, scale, plan, publication, manifest)
    except Exception as error:
        raise ValueError("dataset resolution failed") from error


class AtlasSparkSubmitOperator(SparkSubmitOperator):
    def __init__(self, *, rest_host: str = "spark-master", dataset: str, **kwargs):
        super().__init__(**kwargs)
        self.rest_host = rest_host
        self.dataset = dataset

    def _get_hook(self):
        return RestConfirmingSparkHook(super()._get_hook(), rest_host=self.rest_host)

    def execute(self, context):
        scale = _effective_scale(context)
        resolution = _resolve_dataset(self.dataset, scale)
        self.application_args = [
            *resolution.uris,
            "--dataset-scale",
            resolution.scale,
            "--plan-id",
            resolution.plan_id,
            "--publication-id",
            resolution.publication_id,
            "--manifest-sha256",
            resolution.manifest_sha256,
        ]
        return super().execute(context)


REGION = os.environ.get("MINIO_REGION", "us-east-1")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
ICEBERG_REST_URI = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")
LAKEHOUSE_BUCKET = os.environ.get("MINIO_BUCKET_ICEBERG_LAKEHOUSE", "lakehouse")

default_args = {"owner": "data-eng-lab", "depends_on_past": False, "retries": 1, "retry_delay": timedelta(minutes=2)}
spark_conf = {
    "spark.master": os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"),
    "spark.app.name": "tpch-star-schema",
    "spark.executor.memory": "1g",
    "spark.driver.memory": "1g",
    "spark.standalone.submit.waitAppCompletion": "true",
    "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.endpoint.region": REGION,
    "spark.hadoop.fs.s3a.access.key": os.environ.get("MINIO_ROOT_USER", ""),
    "spark.hadoop.fs.s3a.secret.key": os.environ.get("MINIO_ROOT_PASSWORD", ""),
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
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
    dag_id="tpch_star_schema",
    description="Daily verified TPC-H dimension and fact replacement.",
    default_args=default_args,
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["data-eng-lab", "scenario"],
) as dag:
    AtlasSparkSubmitOperator(
        task_id="submit_tpch_star_schema",
        conn_id="spark_default",
        application="s3a://jars/tpch-star-schema/0.1.0/app.jar",
        java_class="com.thekaveh.dataeng.tpch.TpchStarSchema",
        deploy_mode="cluster",
        conf=spark_conf,
        application_args=[],
        dataset="tpch",
        rest_host="spark-master",
        verbose=True,
    )
