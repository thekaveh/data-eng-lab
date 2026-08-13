from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from scripts.checkpoints.operations import ApplyRequest, OperationFailure, OperationManager, PrepareRequest
from scripts.checkpoints.records import (
    ObjectRecord,
    PlanArtifact,
    canonical_json_bytes,
    inventory_sha256,
    shard_inventory,
)

NOW = datetime(2026, 8, 13, 12, 0, 0, tzinfo=timezone.utc)
OPERATION_ID = "550e8400-e29b-41d4-a716-446655440000"
PREFIX = "streaming_test/11111111-1111-4111-8111-111111111111/"


def _artifact(*, decision="eligible", policy_sha="a" * 64):
    records = (
        ObjectRecord(f"{PREFIX}state/a", "1" * 32, 1, NOW),
        ObjectRecord(f"{PREFIX}state/b", "2" * 32, 2, NOW),
    )
    shards = shard_inventory(records, 1_048_576)
    summary = {
        "actor": "acceptance-engineering",
        "checkpoint_id": "go-live-streaming-test-v1",
        "decision": decision,
        "eligible_after": "2026-08-13T12:00:00Z",
        "evaluated_at": "2026-08-13T12:00:00Z",
        "inventory": {
            "newest_last_modified": "2026-08-12T12:00:00Z",
            "object_count": 2,
            "sha256": inventory_sha256(records),
            "total_bytes": 3,
        },
        "manifest_shards": tuple(shard.sha256 for shard in shards),
        "policy_sha256": policy_sha,
        "prefix": PREFIX,
        "prefix_sha256": __import__("hashlib").sha256(PREFIX.encode()).hexdigest(),
        "refusal_codes": (),
        "retention_anchor": "2026-08-12T12:00:00Z",
        "schema_version": 1,
    }
    body = canonical_json_bytes(
        {"schema_version": 1, "summary": summary, "shards": [json.loads(shard.body) for shard in shards]}
    )
    return PlanArtifact(summary, shards, body, __import__("hashlib").sha256(body).hexdigest())


class FakeGateway:
    def __init__(self):
        self.controls = {}
        self.calls = []
        self.data = tuple(_artifact().shards[0].records)
        self.delete_failure = None
        self.delete_fail_on_call = None
        self.delete_calls = 0
        self.create_failure_suffix = None
        self.head_failure = None

    def create_control(self, key, body):
        self.calls.append(("create", key, body))
        if self.create_failure_suffix and key.endswith(self.create_failure_suffix):
            raise OperationFailure("audit_write_failed")
        if key in self.controls:
            raise RuntimeError("collision")
        etag = __import__("hashlib").md5(body, usedforsecurity=False).hexdigest()
        self.controls[key] = (body, etag)
        return etag

    def read_control(self, key, *, max_bytes):
        self.calls.append(("read", key, max_bytes))
        return self.controls[key]

    def replace_lease(self, key, etag, body):
        self.calls.append(("replace", key, etag, body))
        assert self.controls[key][1] == etag
        next_etag = __import__("hashlib").md5(body, usedforsecurity=False).hexdigest()
        self.controls[key] = (body, next_etag)
        return next_etag

    def head_record(self, record):
        self.calls.append(("head", record.key))
        if self.head_failure is not None:
            raise self.head_failure
        assert record in self.data

    def delete_records(self, records):
        records = tuple(records)
        self.calls.append(("delete", tuple(record.key for record in records)))
        self.delete_calls += 1
        if self.delete_failure is not None or self.delete_calls == self.delete_fail_on_call:
            raise self.delete_failure or OperationFailure("delete_partial")
        removed = {record.key for record in records}
        self.data = tuple(record for record in self.data if record.key not in removed)
        return tuple(record.key for record in records)

    def inventory(self, prefix):
        self.calls.append(("inventory", prefix))
        return self.data


def test_prepare_writes_immutable_shards_before_authoritative_prepared_record():
    gateway = FakeGateway()
    artifact = _artifact()
    request = PrepareRequest(OPERATION_ID, artifact, artifact.sha256, "review-86", "acceptance-engineering")

    status = OperationManager(gateway, policy_sha256="a" * 64, now=lambda: NOW).prepare(request)

    created = [key for operation, key, _body in gateway.calls if operation == "create"]
    assert created == [
        f"_retention/tombstones/{OPERATION_ID}/manifest/0-{artifact.shards[0].sha256}.json",
        f"_retention/tombstones/{OPERATION_ID}/prepared.json",
    ]
    assert status.operation_id == OPERATION_ID
    assert status.state == "prepared"


@pytest.mark.parametrize(
    "request_change",
    [
        {"plan_sha256": "0" * 64},
        {"review": "contains spaces"},
        {"actor": "x" * 129},
    ],
)
def test_prepare_rejects_digest_and_review_identity_drift_before_writes(request_change):
    artifact = _artifact()
    values = {
        "operation_id": OPERATION_ID,
        "artifact": artifact,
        "plan_sha256": artifact.sha256,
        "review": "review-86",
        "actor": "acceptance-engineering",
    }
    values.update(request_change)
    gateway = FakeGateway()

    with pytest.raises(OperationFailure):
        OperationManager(gateway, policy_sha256="a" * 64, now=lambda: NOW).prepare(PrepareRequest(**values))

    assert gateway.calls == []


def test_prepare_refuses_ineligible_plan_and_policy_drift_without_controls():
    for artifact, policy_sha in ((_artifact(decision="refused"), "a" * 64), (_artifact(), "c" * 64)):
        gateway = FakeGateway()
        request = PrepareRequest(OPERATION_ID, artifact, artifact.sha256, "review-86", "acceptance-engineering")
        with pytest.raises(OperationFailure, match="plan_refused|policy_drift"):
            OperationManager(gateway, policy_sha256=policy_sha, now=lambda: NOW).prepare(request)
        assert gateway.calls == []


def _prepared_manager(gateway, artifact, *, now=NOW, revalidate=None, max_delete_keys=1000):
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: now,
        revalidate=revalidate or (lambda _prefix: artifact),
        max_delete_keys=max_delete_keys,
    )
    request = PrepareRequest(OPERATION_ID, artifact, artifact.sha256, "review-86", "acceptance-engineering")
    manager.prepare(request)
    gateway.calls.clear()
    return manager


def test_apply_returns_not_ready_at_899_seconds_without_sleep_or_mutation():
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact, now=NOW)

    status = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW + timedelta(seconds=899),
        revalidate=lambda _prefix: artifact,
    ).apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))

    assert status.state == "not_ready"
    assert not any(call[0] in {"head", "delete", "inventory", "create"} for call in gateway.calls)


def test_apply_revalidates_then_heads_complete_manifest_before_delete_and_proves_empty():
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda prefix: artifact if prefix == PREFIX else pytest.fail("wrong prefix"),
        max_delete_keys=1,
    )

    status = manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))

    assert status.state == "completed"
    operations = [call[0] for call in gateway.calls]
    assert operations.index("head") < operations.index("delete")
    assert operations.count("head") == 2
    assert operations.count("delete") == 2
    assert operations.index("inventory") > max(index for index, value in enumerate(operations) if value == "delete")
    assert gateway.data == ()


def test_apply_rejects_confirmation_or_revalidation_drift_before_head_and_delete():
    artifact = _artifact()
    for request, revalidated in (
        (ApplyRequest(OPERATION_ID, artifact.sha256, "streaming_test/00000000-0000-0000-0000-000000000000/"), artifact),
        (ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX), _artifact(policy_sha="c" * 64)),
    ):
        gateway = FakeGateway()
        _prepared_manager(gateway, artifact)
        manager = OperationManager(
            gateway,
            policy_sha256="a" * 64,
            now=lambda: NOW.replace(minute=15),
            revalidate=lambda _prefix: revalidated,
        )
        with pytest.raises(OperationFailure, match="confirmation_mismatch|revalidation_mismatch"):
            manager.apply(request)
        assert not any(call[0] in {"head", "delete"} for call in gateway.calls)


def test_partial_delete_stops_before_later_batch_and_retry_uses_only_original_remaining_set():
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    gateway.delete_fail_on_call = 2
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda _prefix: artifact,
        max_delete_keys=1,
    )

    with pytest.raises(OperationFailure, match="delete_partial"):
        manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))

    assert sum(call[0] == "delete" for call in gateway.calls) == 2
    assert manager.status(OPERATION_ID).state == "partial"

    first_deleted_key = artifact.shards[0].records[0].key
    gateway.data += (ObjectRecord(f"{PREFIX}foreign-after-plan", "f" * 32, 4, NOW),)
    gateway.delete_fail_on_call = None
    gateway.calls.clear()

    with pytest.raises(OperationFailure, match="postflight_not_empty"):
        manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))

    deleted_keys = tuple(key for call in gateway.calls if call[0] == "delete" for key in call[1])
    assert first_deleted_key not in deleted_keys
    assert f"{PREFIX}foreign-after-plan" not in deleted_keys


def test_head_failure_prevents_every_delete_and_control_flow_is_not_wrapped():
    artifact = _artifact()
    for failure in (OperationFailure("head_mismatch"), KeyboardInterrupt()):
        gateway = FakeGateway()
        _prepared_manager(gateway, artifact)
        gateway.head_failure = failure
        manager = OperationManager(
            gateway,
            policy_sha256="a" * 64,
            now=lambda: NOW.replace(minute=15),
            revalidate=lambda _prefix: artifact,
        )
        with pytest.raises(type(failure)) as caught:
            manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
        if isinstance(failure, KeyboardInterrupt):
            assert caught.value is failure
        assert not any(call[0] == "delete" for call in gateway.calls)


def test_deleted_but_audit_failed_recovery_never_deletes_again():
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    gateway.create_failure_suffix = f"audits/{OPERATION_ID}/{OPERATION_ID}.json"
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda _prefix: artifact,
    )

    with pytest.raises(OperationFailure, match="control_create_failed"):
        manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    delete_count = sum(call[0] == "delete" for call in gateway.calls)

    recovered = manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))

    assert recovered.state == "completed"
    assert sum(call[0] == "delete" for call in gateway.calls) == delete_count
