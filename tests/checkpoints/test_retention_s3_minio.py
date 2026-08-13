from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.request
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_MINIO_INTEGRATION") != "1",
    reason="set RUN_MINIO_INTEGRATION=1 for the pinned disposable MinIO capability test",
)

MINIO_IMAGE = "minio/minio:RELEASE.2025-09-07T16-13-09Z"
MC_IMAGE = "minio/mc:RELEASE.2025-08-13T08-35-41Z"
ROOT_USER = "retention-integration-root"
ROOT_PASSWORD = "retention-integration-root-password"
ACCESS_KEY = "retention-integration-user"
SECRET_KEY = "retention-integration-secret-password"
BUCKET = "checkpoints"
RUN_UUID = "550e8400-e29b-41d4-a716-446655440000"
PREFIX = f"streaming_test/{RUN_UUID}/"
CONTROL_PREFIX = f"_retention/tombstones/{RUN_UUID}/"
NOW = datetime(2026, 8, 13, 12, 0, 0, tzinfo=timezone.utc)


def _run(*command: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=True, capture_output=True, text=True, timeout=30)


def _client(endpoint: str, access_key: str, secret_key: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
    )


@contextmanager
def _pinned_minio(tmp_path):
    name = f"data-eng-lab-issue86-minio-{uuid.uuid4().hex[:12]}"
    policy_path = tmp_path / "retention-policy.json"
    policy_path.write_text(
        json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["s3:ListBucket"],
                        "Resource": [f"arn:aws:s3:::{BUCKET}"],
                        "Condition": {
                            "StringLike": {"s3:prefix": [f"{PREFIX}*", f"{CONTROL_PREFIX}*"]}
                        },
                    },
                    {
                        "Effect": "Allow",
                        "Action": ["s3:GetObject", "s3:DeleteObject"],
                        "Resource": [f"arn:aws:s3:::{BUCKET}/{PREFIX}*"],
                    },
                    {
                        "Effect": "Allow",
                        "Action": ["s3:GetObject", "s3:PutObject"],
                        "Resource": [f"arn:aws:s3:::{BUCKET}/{CONTROL_PREFIX}*"],
                    },
                ],
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    started = False
    try:
        _run(
            "docker",
            "run",
            "--detach",
            "--rm",
            "--name",
            name,
            "--publish",
            "127.0.0.1::9000",
            "--env",
            f"MINIO_ROOT_USER={ROOT_USER}",
            "--env",
            f"MINIO_ROOT_PASSWORD={ROOT_PASSWORD}",
            MINIO_IMAGE,
            "server",
            "/data",
        )
        started = True
        port = _run("docker", "port", name, "9000/tcp").stdout.strip().rsplit(":", 1)[1]
        endpoint = f"http://127.0.0.1:{port}"
        deadline = time.monotonic() + 30
        while True:
            try:
                with urllib.request.urlopen(f"{endpoint}/minio/health/live", timeout=1) as response:
                    if response.status == 200:
                        break
            except OSError:
                if time.monotonic() >= deadline:
                    raise AssertionError("pinned MinIO did not become healthy") from None
                time.sleep(0.25)
        root = _client(endpoint, ROOT_USER, ROOT_PASSWORD)
        root.create_bucket(Bucket=BUCKET)
        _run(
            "docker",
            "run",
            "--rm",
            "--network",
            f"container:{name}",
            "--volume",
            f"{policy_path}:/policy.json:ro",
            "--env",
            f"ROOT_USER={ROOT_USER}",
            "--env",
            f"ROOT_PASSWORD={ROOT_PASSWORD}",
            "--env",
            f"ACCESS_KEY={ACCESS_KEY}",
            "--env",
            f"SECRET_KEY={SECRET_KEY}",
            "--entrypoint",
            "/bin/sh",
            MC_IMAGE,
            "-c",
            "mc alias set local http://127.0.0.1:9000 \"$ROOT_USER\" \"$ROOT_PASSWORD\" >/dev/null && "
            "mc admin user add local \"$ACCESS_KEY\" \"$SECRET_KEY\" >/dev/null && "
            "mc admin policy create local retention-test /policy.json >/dev/null && "
            "mc admin policy attach local retention-test --user \"$ACCESS_KEY\" >/dev/null",
        )
        yield endpoint, root, _client(endpoint, ACCESS_KEY, SECRET_KEY)
    finally:
        if started:
            subprocess.run(
                ["docker", "rm", "--force", name],
                check=False,
                capture_output=True,
                text=True,
                timeout=20,
            )


def _denied(callable_):
    with pytest.raises(ClientError) as failure:
        callable_()
    assert failure.value.response["Error"]["Code"] in {"AccessDenied", "AllAccessDisabled"}


def test_pinned_minio_prefix_iam_cas_and_conditional_delete_capabilities(tmp_path):
    with _pinned_minio(tmp_path) as (_endpoint, root, client):
        data_key = f"{PREFIX}state/offset"
        foreign_key = "streaming_test/00000000-0000-0000-0000-000000000000/state/offset"
        control_key = f"{CONTROL_PREFIX}prepared.json"
        root.put_object(Bucket=BUCKET, Key=data_key, Body=b"data")
        root.put_object(Bucket=BUCKET, Key=foreign_key, Body=b"foreign")

        inventory = client.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
        assert [item["Key"] for item in inventory["Contents"]] == [data_key]
        _denied(lambda: client.list_objects_v2(Bucket=BUCKET, Prefix="streaming_test/"))
        _denied(lambda: client.get_object(Bucket=BUCKET, Key=foreign_key))
        _denied(lambda: client.put_object(Bucket=BUCKET, Key=f"{PREFIX}unauthorized", Body=b"no"))

        created = client.put_object(Bucket=BUCKET, Key=control_key, Body=b"one", IfNoneMatch="*")
        with pytest.raises(ClientError) as duplicate:
            client.put_object(Bucket=BUCKET, Key=control_key, Body=b"duplicate", IfNoneMatch="*")
        assert duplicate.value.response["Error"]["Code"] in {"PreconditionFailed", "ConditionalRequestConflict"}
        current_etag = created["ETag"]
        replaced = client.put_object(Bucket=BUCKET, Key=control_key, Body=b"two", IfMatch=current_etag)
        with pytest.raises(ClientError) as stale:
            client.put_object(Bucket=BUCKET, Key=control_key, Body=b"stale", IfMatch=current_etag)
        assert stale.value.response["Error"]["Code"] == "PreconditionFailed"
        assert replaced["ETag"] != current_etag

        missing_key = f"{CONTROL_PREFIX}missing-if-match.json"
        with pytest.raises(ClientError) as missing:
            client.put_object(Bucket=BUCKET, Key=missing_key, Body=b"pinned-limitation", IfMatch='"0"')
        assert missing.value.response["Error"]["Code"] == "NoSuchKey"

        stale_condition = {
            "Key": data_key,
            "ETag": '"00000000000000000000000000000000"',
            "Size": 999,
            "LastModifiedTime": NOW,
        }
        deleted = client.delete_objects(Bucket=BUCKET, Delete={"Objects": [stale_condition], "Quiet": False})
        assert deleted.get("Errors", []) == []
        assert [item["Key"] for item in deleted["Deleted"]] == [data_key]
        assert "Contents" not in client.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)

        remaining = root.list_objects_v2(Bucket=BUCKET).get("Contents", [])
        assert {item["Key"] for item in remaining} == {foreign_key, control_key}
