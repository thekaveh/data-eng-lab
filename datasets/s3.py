"""Thin boto3 helper for landing objects into MinIO, configured from infra/.env."""
from __future__ import annotations

from pathlib import Path

import boto3
from botocore.client import Config


def _envval(key: str, env_file: Path) -> str:
    if not env_file.exists():
        return ""
    val = ""
    for line in env_file.read_text(encoding="utf-8").splitlines():
        if line.startswith(f"{key}="):
            val = line.split("=", 1)[1].strip()  # last wins
    return val


def s3_client_from_env(infra_dir: Path):
    env_file = Path(infra_dir) / ".env"
    user = _envval("MINIO_ROOT_USER", env_file)
    password = _envval("MINIO_ROOT_PASSWORD", env_file)
    port = _envval("MINIO_PORT", env_file)
    if not (user and password and port):
        raise RuntimeError(
            f"MinIO creds/port missing in {env_file} — start the stack (make up) first "
            "so Atlas generates them."
        )
    return boto3.client(
        "s3",
        endpoint_url=f"http://localhost:{port}",
        aws_access_key_id=user,
        aws_secret_access_key=password,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def object_exists(client, bucket: str, key: str) -> bool:
    from botocore.exceptions import ClientError

    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def upload_file(client, path: Path, bucket: str, key: str) -> None:
    client.upload_file(str(path), bucket, key)
