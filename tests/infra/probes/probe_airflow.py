"""Probe: MinIO S3 round-trip via boto3 + spark_default conn in metadata DB. Exit 0 on success."""
from __future__ import annotations


def probe(exec_fn):
    """Layer-2 probe: exec airflow-scheduler to run this script.

    Returns (ok: bool, message: str).
    """
    rc, out = exec_fn("airflow-scheduler", ["python", "/opt/probes/probe_airflow.py"])
    tail = out.strip().splitlines()[-1] if out.strip() else f"rc={rc}"
    return rc == 0, tail


def _get_connections():
    """Query minio_default and spark_default directly from the Airflow metadata DB.

    Reading the metadata DB is the Atlas-documented way to resolve seeded
    Connections OUTSIDE a task context (infra/services/airflow/README.md,
    "Resolving seeded Connections outside a task", atlas#311): the Airflow-3
    Task-SDK context raises AirflowNotFoundException there by design."""
    from airflow.models import Connection

    try:
        from airflow.settings import Session as _DbSession
        _session = _DbSession()
        try:
            minio = _session.query(Connection).filter_by(conn_id="minio_default").one_or_none()
            spark = _session.query(Connection).filter_by(conn_id="spark_default").one_or_none()
        finally:
            _session.close()
    except Exception:
        from airflow.utils.session import create_session as _cs
        with _cs() as _session:
            minio = _session.query(Connection).filter_by(conn_id="minio_default").one_or_none()
            spark = _session.query(Connection).filter_by(conn_id="spark_default").one_or_none()

    assert minio is not None, "minio_default not defined in the metadata DB"
    assert spark is not None, "spark_default not defined in the metadata DB"
    return minio, spark


if __name__ == "__main__":
    import uuid

    import boto3
    import botocore.config

    conn, _spark_conn = _get_connections()

    extra = conn.extra_dejson if conn.extra else {}
    aws_key = conn.login or extra.get("aws_access_key_id")
    aws_secret = conn.password or extra.get("aws_secret_access_key")
    endpoint_url = extra.get("endpoint_url") or (
        f"http://{conn.host}:{conn.port}" if conn.host and conn.port else None
    )
    region = extra.get("region_name", "us-east-1")

    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        endpoint_url=endpoint_url,
        region_name=region,
        config=botocore.config.Config(s3={"addressing_style": "path"}),
    )

    key = f"_preflight/{uuid.uuid4().hex}.txt"
    s3.put_object(Bucket="landing", Key=key, Body=b"ok")
    try:
        s3.head_object(Bucket="landing", Key=key)
    finally:
        s3.delete_object(Bucket="landing", Key=key)

    print("airflow->minio+spark OK")
