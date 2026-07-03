"""Probe: Airflow S3Hook round-trip to MinIO + spark_default connection present. Exit 0 on success."""
import uuid

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook

hook = S3Hook(aws_conn_id="minio_default")
key = f"_preflight/{uuid.uuid4().hex}.txt"
hook.load_string("ok", key=key, bucket_name="landing", replace=True)
assert hook.check_for_key(key, bucket_name="landing"), "S3Hook round-trip failed"
hook.delete_objects(bucket="landing", keys=[key])
BaseHook.get_connection("spark_default")  # raises if unset
print("airflow->minio+spark OK")
