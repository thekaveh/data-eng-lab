from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.request
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from scripts.checkpoints.operations import ApplyRequest, OperationFailure, OperationManager, PrepareRequest
from scripts.checkpoints.policy import _policy_sha256, load_policy
from scripts.checkpoints.records import PlanArtifact, canonical_json_bytes, inventory_sha256, shard_inventory
from scripts.checkpoints.s3_gateway import GatewayFailure, S3Gateway

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
AUDIT_PREFIX = f"_retention/audits/{RUN_UUID}/"
NOW = datetime(2026, 8, 13, 12, 0, 0, tzinfo=timezone.utc)
ROOT = Path(__file__).resolve().parents[2]


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
                            "StringLike": {
                                "s3:prefix": [f"{PREFIX}*", f"{CONTROL_PREFIX}*", f"{AUDIT_PREFIX}*"]
                            }
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
                        "Resource": [
                            f"arn:aws:s3:::{BUCKET}/{CONTROL_PREFIX}*",
                            f"arn:aws:s3:::{BUCKET}/{AUDIT_PREFIX}*",
                        ],
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


def test_real_gateway_partial_retry_is_original_set_confined_and_idempotent(tmp_path):
    with _pinned_minio(tmp_path) as (_endpoint, root, client):
        policy = load_policy(ROOT / "checkpoints/retention-policy.yaml")
        gateway = S3Gateway(client, policy)
        for key, body in ((f"{PREFIX}state/a", b"a"), (f"{PREFIX}state/b", b"b")):
            root.put_object(Bucket=BUCKET, Key=key, Body=body)
        records = gateway.inventory(PREFIX)
        shards = shard_inventory(records, policy.bounds.max_manifest_shard_bytes)
        summary = {
            "actor": "issue86-partial-live",
            "checkpoint_id": "go-live-streaming-test-v1",
            "decision": "eligible",
            "eligible_after": "2026-08-13T12:00:00Z",
            "evaluated_at": "2026-08-13T12:00:00Z",
            "inventory": {
                "newest_last_modified": records[-1].as_json()["last_modified"],
                "object_count": 2,
                "sha256": inventory_sha256(records),
                "total_bytes": 2,
            },
            "manifest_shards": tuple(shard.sha256 for shard in shards),
            "policy_sha256": _policy_sha256(policy),
            "prefix": PREFIX,
            "prefix_sha256": __import__("hashlib").sha256(PREFIX.encode("ascii")).hexdigest(),
            "refusal_codes": (),
            "retention_anchor": "2026-08-13T12:00:00Z",
            "schema_version": 1,
        }
        value = {"schema_version": 1, "summary": summary, "shards": [json.loads(s.body) for s in shards]}
        body = canonical_json_bytes(value, max_bytes=128 * 1024 * 1024)
        artifact = PlanArtifact(summary, shards, body, __import__("hashlib").sha256(body).hexdigest())

        class PartialOnce:
            def __init__(self, delegate):
                self.delegate = delegate
                self.partial = True
                self.delete_calls: list[tuple[str, ...]] = []

            def __getattr__(self, name):
                return getattr(self.delegate, name)

            def delete_records(self, batch):
                keys = tuple(record.key for record in batch)
                self.delete_calls.append(keys)
                if self.partial:
                    self.partial = False
                    deleted = self.delegate.delete_records((batch[0],))
                    raise GatewayFailure("delete_partial", deleted_keys=deleted)
                return self.delegate.delete_records(batch)

        wrapped = PartialOnce(gateway)
        clock = [NOW]
        manager = OperationManager(
            wrapped,
            policy_sha256=_policy_sha256(policy),
            now=lambda: clock[0],
            revalidate=lambda _prefix, _evaluated_at: artifact,
        )
        operation_id = RUN_UUID
        manager.prepare(PrepareRequest(operation_id, artifact, artifact.sha256, "issue86-partial", summary["actor"]))
        clock[0] = NOW + timedelta(seconds=901)
        request = ApplyRequest(operation_id, artifact.sha256, PREFIX)
        with pytest.raises(OperationFailure, match="delete_partial"):
            manager.apply(request)
        assert json.loads(manager.status(operation_id).body)["deleted_objects"] == 1

        foreign_key = f"{PREFIX}foreign-after-plan"
        root.put_object(Bucket=BUCKET, Key=foreign_key, Body=b"foreign")
        with pytest.raises(OperationFailure, match="postflight_not_empty"):
            manager.apply(request)
        assert foreign_key not in {key for call in wrapped.delete_calls for key in call}
        root.delete_object(Bucket=BUCKET, Key=foreign_key)
        assert manager.apply(request).state == "completed"
        delete_count = len(wrapped.delete_calls)
        assert manager.apply(request).state == "completed"
        assert len(wrapped.delete_calls) == delete_count
        assert gateway.inventory(PREFIX) == ()
