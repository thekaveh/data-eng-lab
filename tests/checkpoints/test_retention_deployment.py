from __future__ import annotations

import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
BUCKET = "arn:aws:s3:::checkpoints"

DATA_PATTERNS = (
    "events/*",
    "event_windows/*",
    "online_retail_cdc/*",
    "gh_events_file/tiny/*/*/*",
    "gh_events_file/small/*/*/*",
    "gh_events_file/medium/*/*/*",
    "streaming_test/*/*",
)
CONTROL_PATTERNS = (
    "_retention/leases/*",
    "_retention/terminals/*",
    "_retention/tombstones/*",
    "_retention/audits/*",
    "_retention/capability/*",
)


def _policy() -> dict[str, object]:
    return json.loads((ROOT / "checkpoints/retention-policy.json").read_text(encoding="utf-8"))


def _statement(policy: dict[str, object], sid: str) -> dict[str, object]:
    statements = policy["Statement"]
    assert isinstance(statements, list)
    matches = [statement for statement in statements if statement.get("Sid") == sid]
    assert len(matches) == 1
    return matches[0]


def _arns(patterns: tuple[str, ...]) -> list[str]:
    return [f"{BUCKET}/{pattern}" for pattern in patterns]


def test_iam_policy_has_only_the_reviewed_action_resource_and_prefix_boundary():
    policy = _policy()
    assert policy["Version"] == "2012-10-17"
    assert {item["Sid"] for item in policy["Statement"]} == {
        "AllowListOwnedPrefixes",
        "AllowReadOwnedObjects",
        "AllowDeleteCheckpointData",
        "AllowWriteRetentionControls",
        "DenyUnknownPrefixListing",
        "DenyCheckpointDataWrites",
        "DenyRetentionControlDeletes",
        "DenyUnknownObjectAccess",
    }

    listing = _statement(policy, "AllowListOwnedPrefixes")
    assert listing == {
        "Sid": "AllowListOwnedPrefixes",
        "Effect": "Allow",
        "Action": ["s3:ListBucket"],
        "Resource": [BUCKET],
        "Condition": {"StringLike": {"s3:prefix": list(DATA_PATTERNS + CONTROL_PATTERNS)}},
    }
    assert _statement(policy, "AllowReadOwnedObjects") == {
        "Sid": "AllowReadOwnedObjects",
        "Effect": "Allow",
        "Action": ["s3:GetObject"],
        "Resource": _arns(DATA_PATTERNS + CONTROL_PATTERNS),
    }
    assert _statement(policy, "AllowDeleteCheckpointData") == {
        "Sid": "AllowDeleteCheckpointData",
        "Effect": "Allow",
        "Action": ["s3:DeleteObject"],
        "Resource": _arns(DATA_PATTERNS),
    }
    assert _statement(policy, "AllowWriteRetentionControls") == {
        "Sid": "AllowWriteRetentionControls",
        "Effect": "Allow",
        "Action": ["s3:PutObject"],
        "Resource": _arns(CONTROL_PATTERNS),
    }

    assert _statement(policy, "DenyUnknownPrefixListing") == {
        "Sid": "DenyUnknownPrefixListing",
        "Effect": "Deny",
        "Action": ["s3:ListBucket"],
        "Resource": [BUCKET],
        "Condition": {"StringNotLike": {"s3:prefix": list(DATA_PATTERNS + CONTROL_PATTERNS)}},
    }
    assert _statement(policy, "DenyCheckpointDataWrites")["Resource"] == _arns(DATA_PATTERNS)
    assert _statement(policy, "DenyRetentionControlDeletes")["Resource"] == _arns(CONTROL_PATTERNS)
    unknown = _statement(policy, "DenyUnknownObjectAccess")
    assert unknown["NotResource"] == _arns(DATA_PATTERNS + CONTROL_PATTERNS)
    assert unknown["Action"] == ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]

    serialized = json.dumps(policy, sort_keys=True)
    assert f"{BUCKET}/*" not in serialized
    assert f'{BUCKET}"' in serialized
    assert "s3:PutObject" not in json.dumps(_statement(policy, "AllowDeleteCheckpointData"))
    assert "s3:DeleteObject" not in json.dumps(_statement(policy, "AllowWriteRetentionControls"))


def test_provisioner_uses_root_only_in_init_and_never_xtraces_credentials():
    script = (ROOT / "checkpoints/provision-retention.sh").read_text(encoding="utf-8")
    assert script.startswith("#!/bin/sh\nset -eu\nset +x\n")
    assert "MINIO_ROOT_USER" in script
    assert "MINIO_ROOT_PASSWORD" in script
    assert "MINIO_RETENTION_ACCESS_KEY" in script
    assert "MINIO_RETENTION_SECRET_KEY" in script
    assert "mc admin user svcacct" in script
    assert "--policy /config/retention-policy.json" in script
    assert "echo $" not in script
    assert "set -x" not in script
    assert "env" not in [line.strip() for line in script.splitlines()]
    assert "${#MINIO_RETENTION_ACCESS_KEY}" in script
    assert "${#MINIO_RETENTION_SECRET_KEY}" in script


def test_consumer_manifest_requires_the_ignored_operator_credential_overlay():
    manifest = yaml.safe_load((ROOT / "atlas.consumer.yml").read_text(encoding="utf-8"))
    assert manifest["env"]["file"] == "./atlas.env.user"
    ignored = (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    assert "/atlas.env.user" in ignored

    example = (ROOT / "atlas.env.user.example").read_text(encoding="utf-8")
    assert set(line.split("=", 1)[0] for line in example.splitlines() if line and not line.startswith("#")) == {
        "MINIO_RETENTION_ACCESS_KEY",
        "MINIO_RETENTION_SECRET_KEY",
        "CHECKPOINT_RETENTION_LEASE_TOKEN",
        "CHECKPOINT_RETENTION_OPERATOR_TOKEN",
    }
    assert "CHANGE_ME" in example
    assert "MINIO_ROOT" not in example
    values = dict(line.split("=", 1) for line in example.splitlines() if line and not line.startswith("#"))
    assert 3 <= len(values["MINIO_RETENTION_ACCESS_KEY"]) <= 20
    assert 8 <= len(values["MINIO_RETENTION_SECRET_KEY"]) <= 40
    assert 16 <= len(values["CHECKPOINT_RETENTION_LEASE_TOKEN"]) <= 256
    assert 16 <= len(values["CHECKPOINT_RETENTION_OPERATOR_TOKEN"]) <= 256
    assert values["CHECKPOINT_RETENTION_LEASE_TOKEN"] != values["CHECKPOINT_RETENTION_OPERATOR_TOKEN"]


def test_compose_runtime_is_single_replica_internal_nonroot_and_fail_closed():
    compose = yaml.safe_load((ROOT / "compose/data-eng-lab.yml").read_text(encoding="utf-8"))
    init = compose["services"]["checkpoint-retention-init"]
    runtime = compose["services"]["checkpoint-retention"]

    assert init["image"] == "${MINIO_INIT_IMAGE}"
    assert init["restart"] == "no"
    assert init["entrypoint"] == ["/bin/sh", "/config/provision-retention.sh"]
    assert init["volumes"] == [
        "../checkpoints/provision-retention.sh:/config/provision-retention.sh:ro",
        "../checkpoints/retention-policy.json:/config/retention-policy.json:ro",
    ]
    assert init["environment"] == {
        "MINIO_ROOT_USER": "${MINIO_ROOT_USER:?MINIO_ROOT_USER is required}",
        "MINIO_ROOT_PASSWORD": "${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD is required}",
        "MINIO_RETENTION_ACCESS_KEY": "${MINIO_RETENTION_ACCESS_KEY:?MINIO_RETENTION_ACCESS_KEY is required}",
        "MINIO_RETENTION_SECRET_KEY": "${MINIO_RETENTION_SECRET_KEY:?MINIO_RETENTION_SECRET_KEY is required}",
    }

    assert runtime["build"] == {"context": "..", "dockerfile": "checkpoints/retention.Dockerfile"}
    assert runtime["platform"] == "linux/amd64"
    assert runtime["user"] == "65532:65532"
    assert runtime["read_only"] is True
    assert runtime["deploy"] == {"replicas": 1}
    assert runtime["restart"] == "unless-stopped"
    assert "ports" not in runtime
    assert runtime["networks"] == ["backend-network"]
    assert runtime["depends_on"] == {"checkpoint-retention-init": {"condition": "service_completed_successfully"}}
    assert runtime["environment"] == {
        "MINIO_ENDPOINT": "http://minio:9000",
        "MINIO_RETENTION_ACCESS_KEY": "${MINIO_RETENTION_ACCESS_KEY:?MINIO_RETENTION_ACCESS_KEY is required}",
        "MINIO_RETENTION_SECRET_KEY": "${MINIO_RETENTION_SECRET_KEY:?MINIO_RETENTION_SECRET_KEY is required}",
        "CHECKPOINT_RETENTION_LEASE_TOKEN": (
            "${CHECKPOINT_RETENTION_LEASE_TOKEN:?CHECKPOINT_RETENTION_LEASE_TOKEN is required}"
        ),
        "CHECKPOINT_RETENTION_OPERATOR_TOKEN": (
            "${CHECKPOINT_RETENTION_OPERATOR_TOKEN:?CHECKPOINT_RETENTION_OPERATOR_TOKEN is required}"
        ),
        "DESTRUCTIVE_ENABLED": "${CHECKPOINT_RETENTION_DESTRUCTIVE_ENABLED:-false}",
    }
    assert not {"MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD"} & runtime["environment"].keys()
    assert runtime["healthcheck"]["test"][:2] == ["CMD", "/opt/venv/bin/python"]


def test_runtime_image_is_pinned_minimal_and_contains_only_required_code():
    dockerfile = (ROOT / "checkpoints/retention.Dockerfile").read_text(encoding="utf-8")
    assert "python@sha256:" in dockerfile
    assert "uv@sha256:" in dockerfile
    assert "uv sync --frozen --only-group dev --no-install-project" in dockerfile
    assert "USER 65532:65532" in dockerfile
    assert 'ENTRYPOINT ["/opt/venv/bin/python", "-m", "scripts.checkpoints.service"]' in dockerfile
    assert "COPY infra" not in dockerfile

    dockerignore = (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
    assert "!scripts/checkpoints/" in dockerignore
    assert "!checkpoints/retention-policy.yaml" in dockerignore
