from __future__ import annotations

import json
import re
from contextlib import contextmanager
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
from scripts.checkpoints.s3_gateway import GatewayFailure

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
        if self.create_failure_suffix and self.create_failure_suffix in key:
            raise OperationFailure("audit_write_failed")
        if key in self.controls:
            raise RuntimeError("collision")
        etag = __import__("hashlib").md5(body, usedforsecurity=False).hexdigest()
        self.controls[key] = (body, etag)
        return etag

    def read_control(self, key, *, max_bytes):
        self.calls.append(("read", key, max_bytes))
        return self.controls[key]

    def list_controls(self, prefix, *, max_keys):
        keys = tuple(sorted(key for key in self.controls if key.startswith(prefix)))
        assert len(keys) <= max_keys
        return keys

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
    assert created[:2] == [
        f"_retention/tombstones/{OPERATION_ID}/manifest/0-{artifact.shards[0].sha256}.json",
        f"_retention/tombstones/{OPERATION_ID}/prepared.json",
    ]
    assert "/results/shards/" in created[2]
    assert "/results/attempts/" in created[3]
    assert created[4].startswith(f"_retention/audits/{OPERATION_ID}/")
    assert status.operation_id == OPERATION_ID
    assert status.state == "prepared"


def test_identical_prepare_retry_reuses_first_authoritative_time_and_evidence():
    gateway = FakeGateway()
    artifact = _artifact()
    clock = [NOW]
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: clock[0],
        quiescence_seconds=0,
        revalidate=lambda *_args: artifact,
    )
    request = PrepareRequest(OPERATION_ID, artifact, artifact.sha256, "review-86", "acceptance-engineering")

    first = manager.prepare(request)
    first_controls = dict(gateway.controls)
    clock[0] += timedelta(seconds=1)
    second = manager.prepare(request)

    assert second.body == first.body
    assert gateway.controls == first_controls
    prepared = json.loads(gateway.controls[f"_retention/tombstones/{OPERATION_ID}/prepared.json"][0])
    assert prepared["prepared_at"] == "2026-08-13T12:00:00Z"

    completed = manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    after_completed = manager.prepare(request)
    assert after_completed.body == completed.body
    assert after_completed.state == "completed"


def test_identical_prepare_retry_repairs_missing_prepare_audit_idempotently():
    gateway = FakeGateway()
    artifact = _artifact()
    manager = OperationManager(gateway, policy_sha256="a" * 64, now=lambda: NOW)
    request = PrepareRequest(OPERATION_ID, artifact, artifact.sha256, "review-86", "acceptance-engineering")
    gateway.create_failure_suffix = f"audits/{OPERATION_ID}/"

    with pytest.raises(OperationFailure):
        manager.prepare(request)
    assert len([key for key in gateway.controls if "/results/attempts/" in key]) == 1
    assert not any(key.startswith(f"_retention/audits/{OPERATION_ID}/") for key in gateway.controls)

    gateway.create_failure_suffix = None
    repaired = manager.prepare(request)
    after_repair = dict(gateway.controls)
    repeated = manager.prepare(request)

    assert repaired.state == repeated.state == "prepared"
    assert repaired.body == repeated.body
    assert gateway.controls == after_repair
    assert len([key for key in gateway.controls if key.startswith(f"_retention/audits/{OPERATION_ID}/")]) == 1


def test_progressed_prepare_retry_repairs_every_validated_historical_audit():
    gateway = FakeGateway()
    artifact = _artifact()
    request = PrepareRequest(OPERATION_ID, artifact, artifact.sha256, "review-86", "acceptance-engineering")
    gateway.create_failure_suffix = f"audits/{OPERATION_ID}/"
    first = OperationManager(gateway, policy_sha256="a" * 64, now=lambda: NOW)
    with pytest.raises(OperationFailure):
        first.prepare(request)
    gateway.create_failure_suffix = None
    progressed = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW + timedelta(seconds=899),
        revalidate=lambda *_args: artifact,
    )

    assert progressed.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX)).state == "not_ready"
    assert progressed.prepare(request).state == "not_ready"
    audits = [
        json.loads(body)
        for key, (body, _etag) in gateway.controls.items()
        if key.startswith(f"_retention/audits/{OPERATION_ID}/")
    ]
    assert [(value["attempt_sequence"], value["decision"]) for value in audits] == [
        (1, "prepared"),
        (2, "not_ready"),
    ]


def test_revalidation_deadline_is_not_persisted_as_revalidation_failure():
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda *_args: (_ for _ in ()).throw(OperationFailure("operation_deadline")),
    )

    with pytest.raises(OperationFailure, match="operation_deadline"):
        manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    assert manager.status(OPERATION_ID).state == "prepared"


def test_status_deadline_preserves_timeout_category():
    gateway = FakeGateway()
    manager = _prepared_manager(gateway, _artifact())

    def deadline_list(_prefix, *, max_keys):
        raise GatewayFailure("operation_deadline")

    gateway.list_controls = deadline_list

    with pytest.raises(OperationFailure, match="operation_deadline"):
        manager.status(OPERATION_ID)


def test_prepare_and_not_ready_are_queryable_immutable_audited_attempts():
    gateway = FakeGateway()
    artifact = _artifact()
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW,
        revalidate=lambda *_args: artifact,
    )
    prepared = manager.prepare(
        PrepareRequest(OPERATION_ID, artifact, artifact.sha256, "review-86", "acceptance-engineering")
    )

    assert prepared.state == "prepared"
    assert manager.status(OPERATION_ID).state == "prepared"
    assert any(key.startswith(f"_retention/audits/{OPERATION_ID}/") for key in gateway.controls)

    not_ready = manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))

    assert not_ready.state == "not_ready"
    assert manager.status(OPERATION_ID).state == "not_ready"
    audits = [key for key in gateway.controls if key.startswith(f"_retention/audits/{OPERATION_ID}/")]
    assert len(audits) == 2


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
        revalidate=revalidate or (lambda _prefix, _evaluated_at: artifact),
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
        revalidate=lambda _prefix, _evaluated_at: artifact,
    ).apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))

    assert status.state == "not_ready"
    value = json.loads(status.body)
    assert value["operation_id"] == OPERATION_ID
    assert value["plan_sha256"] == artifact.sha256
    assert value["schema_version"] == 1
    assert value["state"] == "not_ready"
    assert value["deleted_objects"] == 0
    assert not any(call[0] in {"head", "delete", "inventory"} for call in gateway.calls)


def test_apply_revalidates_then_heads_complete_manifest_before_delete_and_proves_empty():
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda prefix, evaluated_at: (
            artifact
            if prefix == PREFIX and evaluated_at == NOW.replace(minute=15)
            else pytest.fail("wrong revalidation identity")
        ),
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


def test_apply_heads_each_batch_immediately_before_delete_without_redundant_full_pass():
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda *_args: artifact,
        max_delete_keys=1,
    )

    manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))

    destructive = [call[:2] for call in gateway.calls if call[0] in {"head", "delete"}]
    first, second = (record.key for record in artifact.shards[0].records)
    assert destructive == [
        ("head", first),
        ("delete", (first,)),
        ("head", second),
        ("delete", (second,)),
    ]


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
            revalidate=lambda _prefix, _evaluated_at: revalidated,
        )
        with pytest.raises(
            OperationFailure, match="destructive_scope_invalid|confirmation_mismatch|revalidation_mismatch"
        ):
            manager.apply(request)
        assert not any(call[0] in {"head", "delete"} for call in gateway.calls)


def test_apply_accepts_revalidation_with_new_evaluation_time_when_bound_state_is_identical():
    artifact = _artifact()
    current_value = json.loads(artifact.body)
    current_value["summary"]["evaluated_at"] = "2026-08-13T12:15:00Z"
    current_body = canonical_json_bytes(current_value)
    current = PlanArtifact(
        current_value["summary"],
        artifact.shards,
        current_body,
        __import__("hashlib").sha256(current_body).hexdigest(),
    )
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda _prefix, _evaluated_at: current,
    )

    status = manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))

    assert status.state == "completed"


def test_partial_delete_stops_before_later_batch_and_retry_uses_only_original_remaining_set():
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    gateway.delete_fail_on_call = 2
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda _prefix, _evaluated_at: artifact,
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

    with pytest.raises(OperationFailure, match="revalidation_mismatch"):
        manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))

    deleted_keys = tuple(key for call in gateway.calls if call[0] == "delete" for key in call[1])
    assert first_deleted_key not in deleted_keys
    assert f"{PREFIX}foreign-after-plan" not in deleted_keys


def test_mixed_partial_response_persists_successful_keys_for_original_set_retry():
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    first_key = artifact.shards[0].records[0].key

    def partial(records):
        gateway.data = tuple(record for record in gateway.data if record.key != first_key)
        failure = GatewayFailure("delete_partial", deleted_keys=(first_key,))
        raise failure

    gateway.delete_records = partial
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda _prefix, _evaluated_at: artifact,
    )

    with pytest.raises(OperationFailure, match="delete_partial"):
        manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))

    partial_body = json.loads(manager.status(OPERATION_ID).body)
    assert partial_body["deleted_objects"] == 1
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda _prefix, _evaluated_at: artifact,
    )
    gateway.delete_records = FakeGateway.delete_records.__get__(gateway)
    gateway.data += (ObjectRecord(f"{PREFIX}foreign-after-plan", "f" * 32, 4, NOW),)
    with pytest.raises(OperationFailure, match="revalidation_mismatch"):
        manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    assert first_key not in tuple(key for call in gateway.calls if call[0] == "delete" for key in call[1])


def test_partial_and_refused_attempts_always_persist_audit_and_exact_classification():
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    gateway.delete_failure = OperationFailure("delete_partial")
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda *_args: artifact,
    )

    with pytest.raises(OperationFailure, match="delete_partial"):
        manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))

    assert manager.status(OPERATION_ID).state == "refused"
    partial_audits = [key for key in gateway.controls if key.startswith(f"_retention/audits/{OPERATION_ID}/")]
    assert partial_audits

    other_gateway = FakeGateway()
    _prepared_manager(other_gateway, artifact)
    refused = OperationManager(
        other_gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda *_args: _artifact(policy_sha="c" * 64),
    )
    with pytest.raises(OperationFailure, match="revalidation_mismatch"):
        refused.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))

    assert refused.status(OPERATION_ID).state == "refused"
    assert any(key.startswith(f"_retention/audits/{OPERATION_ID}/") for key in other_gateway.controls)


def test_restart_never_infers_external_disappearance_as_a_successful_delete():
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    gateway.data = gateway.data[1:]
    current_records = gateway.data
    current_shards = shard_inventory(current_records, 1_048_576)
    value = json.loads(artifact.body)
    value["summary"]["inventory"] = {
        "newest_last_modified": "2026-08-12T12:00:00Z",
        "object_count": 1,
        "sha256": inventory_sha256(current_records),
        "total_bytes": 2,
    }
    value["summary"]["manifest_shards"] = [current_shards[0].sha256]
    value["shards"] = [json.loads(current_shards[0].body)]
    body = canonical_json_bytes(value)
    current = PlanArtifact(value["summary"], current_shards, body, __import__("hashlib").sha256(body).hexdigest())
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda *_args: current,
    )

    with pytest.raises(OperationFailure, match="revalidation_mismatch"):
        manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    assert not any(call[0] == "delete" for call in gateway.calls)
    assert manager.status(OPERATION_ID).state == "refused"


def test_apply_heads_each_record_once_immediately_before_its_batch_delete():
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda *_args: artifact,
        max_delete_keys=1,
    )

    assert manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX)).state == "completed"
    assert [call[0] for call in gateway.calls if call[0] in {"head", "delete"}] == ["head", "delete", "head", "delete"]


def test_predelete_head_failure_persists_refused_failed_classification_and_audit():
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    gateway.head_failure = GatewayFailure("head_mismatch")
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda *_args: artifact,
    )

    with pytest.raises(OperationFailure, match="head_mismatch"):
        manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    status = manager.status(OPERATION_ID)
    assert status.state == "refused"
    assert json.loads(status.body)["primary_category"] == "head_mismatch"
    audit_values = [
        json.loads(body)
        for key, (body, _etag) in gateway.controls.items()
        if key.startswith(f"_retention/audits/{OPERATION_ID}/")
    ]
    audit = next(value for value in audit_values if value["decision"] == "refused")
    assert audit["refusal_codes"] == ["head_mismatch"]
    shard = json.loads(
        gateway.controls[
            f"_retention/tombstones/{OPERATION_ID}/results/shards/{json.loads(status.body)['result_shards'][0]}.json"
        ][0]
    )
    assert {item["outcome"] for item in shard} == {"failed", "unattempted"}


def test_partial_result_classification_is_sharded_and_restart_safe_beyond_summary_bound():
    records = tuple(ObjectRecord(f"{PREFIX}state/{index:05d}", f"{index:032x}", 1, NOW) for index in range(1_200))
    shards = shard_inventory(records, 1_048_576)
    summary = dict(_artifact().summary)
    summary["inventory"] = {
        "newest_last_modified": "2026-08-13T12:00:00Z",
        "object_count": len(records),
        "sha256": inventory_sha256(records),
        "total_bytes": len(records),
    }
    summary["manifest_shards"] = tuple(shard.sha256 for shard in shards)
    body = canonical_json_bytes(
        {"schema_version": 1, "summary": summary, "shards": [json.loads(shard.body) for shard in shards]},
        max_bytes=128 * 1024 * 1024,
        max_nodes=20_000,
    )
    artifact = PlanArtifact(summary, shards, body, __import__("hashlib").sha256(body).hexdigest())
    gateway = FakeGateway()
    gateway.data = records
    _prepared_manager(gateway, artifact)

    def partial(values):
        deleted = tuple(record.key for record in values[:800])
        gateway.data = tuple(record for record in gateway.data if record.key not in set(deleted))
        raise GatewayFailure("delete_partial", deleted_keys=deleted)

    gateway.delete_records = partial
    first = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda _prefix, _evaluated_at: artifact,
        max_delete_keys=1_000,
    )
    with pytest.raises(OperationFailure, match="delete_partial"):
        first.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))

    partial_status = first.status(OPERATION_ID)
    partial_value = json.loads(partial_status.body)
    assert len(partial_status.body) <= 65_536
    assert partial_value["deleted_objects"] == 800
    assert partial_value["remaining_objects"] == 400
    assert partial_value["result_shards"]

    gateway.delete_records = FakeGateway.delete_records.__get__(gateway)
    restarted = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda _prefix, _evaluated_at: _artifact_for_records(gateway.data),
        max_delete_keys=1_000,
    )
    completed = restarted.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))

    assert completed.state == "completed"
    assert gateway.data == ()


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
            revalidate=lambda _prefix, _evaluated_at: artifact,
        )
        with pytest.raises(type(failure)) as caught:
            manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
        if isinstance(failure, KeyboardInterrupt):
            assert caught.value is failure
        assert not any(call[0] == "delete" for call in gateway.calls)


def test_failed_head_attempt_is_counted_in_refused_status_and_audit():
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    gateway.head_failure = GatewayFailure("head_mismatch")
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda *_args: artifact,
    )

    with pytest.raises(OperationFailure, match="head_mismatch"):
        manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))

    status = json.loads(manager.status(OPERATION_ID).body)
    assert status["state"] == "refused"
    assert status["head_requests"] == 1
    audits = [
        json.loads(body)
        for key, (body, _etag) in gateway.controls.items()
        if key.startswith(f"_retention/audits/{OPERATION_ID}/")
    ]
    refused = [audit for audit in audits if audit["decision"] == "refused"]
    assert len(refused) == 1
    assert refused[0]["head_requests"] == 1


def test_deleted_but_audit_failed_recovery_never_deletes_again():
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    gateway.create_failure_suffix = f"audits/{OPERATION_ID}/"
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda _prefix, _evaluated_at: artifact,
    )

    with pytest.raises(OperationFailure, match="control_create_failed") as caught:
        manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    assert caught.value.partial is True
    delete_count = sum(call[0] == "delete" for call in gateway.calls)

    gateway.create_failure_suffix = None
    restarted = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda _prefix, _evaluated_at: artifact,
    )
    recovered = restarted.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))

    assert recovered.state == "completed"
    assert sum(call[0] == "delete" for call in gateway.calls) == delete_count
    assert any(key.startswith(f"_retention/audits/{OPERATION_ID}/") for key in gateway.controls)


def test_retry_repairs_missing_refused_audit_before_next_attempt():
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    gateway.head_failure = GatewayFailure("head_mismatch")
    gateway.create_failure_suffix = f"audits/{OPERATION_ID}/"
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda *_args: artifact,
    )

    with pytest.raises(OperationFailure, match="head_mismatch"):
        manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    gateway.create_failure_suffix = None
    with pytest.raises(OperationFailure, match="head_mismatch"):
        manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))

    audits = [
        json.loads(body)
        for key, (body, _etag) in gateway.controls.items()
        if key.startswith(f"_retention/audits/{OPERATION_ID}/")
    ]
    assert any(audit["decision"] == "refused" and audit["attempt_sequence"] == 2 for audit in audits)


def test_deleted_but_unpersisted_progress_refuses_restart_without_redelete_or_inference():
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    gateway.create_failure_suffix = "/results/attempts/"
    first = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda *_args: artifact,
    )

    with pytest.raises(OperationFailure) as failure:
        first.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    assert failure.value.partial is True
    delete_count = sum(call[0] == "delete" for call in gateway.calls)
    gateway.create_failure_suffix = None
    restarted = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda *_args: _artifact_for_records(gateway.data),
    )

    with pytest.raises(OperationFailure, match="revalidation_mismatch"):
        restarted.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    assert sum(call[0] == "delete" for call in gateway.calls) == delete_count


def test_deadline_after_delete_persists_partial_evidence_with_cleanup_budget():
    artifact = _artifact()
    gateway = FakeGateway()
    clock = [0.0]
    _prepared_manager(gateway, artifact)
    original_delete = gateway.delete_records

    def delete_then_expire(records):
        result = original_delete(records)
        clock[0] = 901.0
        return result

    gateway.delete_records = delete_then_expire
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda *_args: artifact,
        monotonic=lambda: clock[0],
        max_active_seconds=900,
    )

    with pytest.raises(OperationFailure, match="operation_deadline") as failure:
        manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))

    assert failure.value.partial is True
    assert manager.status(OPERATION_ID).state == "partial"
    audits = [
        json.loads(body)
        for key, (body, _etag) in gateway.controls.items()
        if key.startswith(f"_retention/audits/{OPERATION_ID}/")
    ]
    partial = next(audit for audit in audits if audit["decision"] == "partial")
    assert partial["attempt_sequence"] == 2
    assert partial["deleted_objects"] == 2
    assert partial["remaining_objects"] == 0
    assert partial["primary_category"] == "operation_deadline"


def _artifact_for_records(records):
    shards = shard_inventory(records, 1_048_576)
    summary = dict(_artifact().summary)
    summary["inventory"] = {
        "newest_last_modified": "2026-08-13T12:00:00Z",
        "object_count": len(records),
        "sha256": inventory_sha256(records),
        "total_bytes": sum(record.size_bytes for record in records),
    }
    summary["manifest_shards"] = tuple(shard.sha256 for shard in shards)
    body = canonical_json_bytes(
        {"schema_version": 1, "summary": summary, "shards": [json.loads(shard.body) for shard in shards]},
        max_bytes=128 * 1024 * 1024,
        max_nodes=max(4_096, len(records) * 8 + 128),
    )
    return PlanArtifact(summary, shards, body, __import__("hashlib").sha256(body).hexdigest())


def test_restart_revalidates_exact_original_manifest_minus_persisted_deleted_set():
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    first_key = artifact.shards[0].records[0].key

    def partial(records):
        gateway.data = tuple(record for record in gateway.data if record.key != first_key)
        raise GatewayFailure("delete_partial", deleted_keys=(first_key,))

    gateway.delete_records = partial
    first = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda *_args: artifact,
    )
    with pytest.raises(OperationFailure, match="delete_partial"):
        first.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))

    remaining_artifact = _artifact_for_records(gateway.data)
    gateway.delete_records = FakeGateway.delete_records.__get__(gateway)
    restarted = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda *_args: remaining_artifact,
    )
    assert restarted.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX)).state == "completed"
    assert gateway.data == ()


def test_sequential_same_progress_refusals_remain_ordered_and_retryable():
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    mismatch = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda *_args: _artifact(decision="refused"),
    )
    with pytest.raises(OperationFailure, match="revalidation_mismatch"):
        mismatch.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))

    gateway.head_failure = GatewayFailure("head_mismatch")
    retry = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=16),
        revalidate=lambda *_args: artifact,
    )
    with pytest.raises(OperationFailure, match="head_mismatch"):
        retry.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))

    latest = json.loads(retry.status(OPERATION_ID).body)
    assert latest["attempt_sequence"] == 3
    assert latest["primary_category"] == "head_mismatch"


@pytest.mark.parametrize("progressed_state", ["refused", "partial"])
@pytest.mark.parametrize("backward_clock", [(14, 59), (15, 0)], ids=["before-quiescence", "after-quiescence"])
def test_backward_clock_after_progress_returns_authoritative_history_without_append(progressed_state, backward_clock):
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    if progressed_state == "refused":
        gateway.head_failure = GatewayFailure("head_mismatch")
    else:
        first_key = artifact.shards[0].records[0].key

        def partial(records):
            gateway.data = tuple(record for record in gateway.data if record.key != first_key)
            raise GatewayFailure("delete_partial", deleted_keys=(first_key,))

        gateway.delete_records = partial
    forward = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=16),
        revalidate=lambda *_args: artifact,
    )
    with pytest.raises(OperationFailure):
        forward.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    before = dict(gateway.controls)
    data_before = gateway.data
    gateway.calls.clear()
    backward = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=backward_clock[0], second=backward_clock[1]),
        revalidate=lambda *_args: artifact,
    )

    returned = backward.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))

    assert returned.state == progressed_state
    assert backward.status(OPERATION_ID).state == progressed_state
    assert gateway.controls == before
    assert gateway.data == data_before
    assert not any(call[0] in {"create", "head", "delete"} for call in gateway.calls)


def test_completed_recovery_validates_result_shards_and_exact_absence_before_audit():
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda *_args: artifact,
    )
    completed = manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    audit_keys = [key for key in gateway.controls if key.startswith(f"_retention/audits/{OPERATION_ID}/")]
    for key in audit_keys:
        del gateway.controls[key]
    result_digest = json.loads(completed.body)["result_shards"][0]
    del gateway.controls[f"_retention/tombstones/{OPERATION_ID}/results/shards/{result_digest}.json"]

    with pytest.raises(OperationFailure, match="status_invalid"):
        OperationManager(
            gateway,
            policy_sha256="a" * 64,
            now=lambda: NOW.replace(minute=15),
            revalidate=lambda *_args: _artifact_for_records(gateway.data),
        ).apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    assert not any(key.startswith(f"_retention/audits/{OPERATION_ID}/") for key in gateway.controls)


def test_status_read_never_repairs_missing_audit():
    artifact = _artifact()
    gateway = FakeGateway()
    manager = _prepared_manager(gateway, artifact)
    for key in tuple(gateway.controls):
        if key.startswith(f"_retention/audits/{OPERATION_ID}/"):
            del gateway.controls[key]
    gateway.calls.clear()

    assert manager.status(OPERATION_ID).state == "prepared"
    assert not any(key.startswith(f"_retention/audits/{OPERATION_ID}/") for key in gateway.controls)
    assert not any(call[0] == "create" for call in gateway.calls)


def test_completed_missing_audit_is_not_repaired_when_exact_prefix_is_nonempty():
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda *_args: artifact,
    )
    manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    completed_audit = next(
        key
        for key, (body, _etag) in gateway.controls.items()
        if key.startswith(f"_retention/audits/{OPERATION_ID}/") and json.loads(body)["decision"] == "completed"
    )
    del gateway.controls[completed_audit]
    gateway.data = (ObjectRecord(f"{PREFIX}foreign", "3" * 32, 3, NOW),)
    gateway.calls.clear()

    with pytest.raises(OperationFailure, match="status_invalid"):
        manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    assert completed_audit not in gateway.controls
    assert not any(
        call[0] == "create" and call[1].startswith(f"_retention/audits/{OPERATION_ID}/") for call in gateway.calls
    )


@pytest.mark.parametrize("unexpected_object", [False, True], ids=["empty", "unexpected-object"])
def test_completed_backward_clock_always_proves_fresh_exact_absence(unexpected_object):
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    forward = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=16),
        revalidate=lambda *_args: artifact,
    )
    completed = forward.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    if unexpected_object:
        gateway.data = (ObjectRecord(f"{PREFIX}foreign", "3" * 32, 3, NOW),)
    before = dict(gateway.controls)
    data_before = gateway.data
    gateway.calls.clear()
    backward = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda *_args: artifact,
    )

    if unexpected_object:
        with pytest.raises(OperationFailure, match="status_invalid"):
            backward.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    else:
        assert backward.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX)).body == completed.body

    assert any(call[0] == "inventory" for call in gateway.calls)
    assert gateway.controls == before
    assert gateway.data == data_before
    assert not any(call[0] in {"create", "head", "delete"} for call in gateway.calls)


def test_apply_freezes_validated_attempt_time_before_clock_steps_backward_during_evidence():
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    first_key = artifact.shards[0].records[0].key

    def partial(records):
        gateway.data = tuple(record for record in gateway.data if record.key != first_key)
        raise GatewayFailure("delete_partial", deleted_keys=(first_key,))

    gateway.delete_records = partial
    first = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=16),
        revalidate=lambda *_args: artifact,
    )
    with pytest.raises(OperationFailure, match="delete_partial"):
        first.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    remaining = _artifact_for_records(gateway.data)
    gateway.delete_records = FakeGateway.delete_records.__get__(gateway)
    wall_clock = iter(
        [
            NOW.replace(minute=17),
            NOW.replace(minute=17),
            NOW.replace(minute=15),
        ]
    )
    retry = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: next(wall_clock),
        revalidate=lambda *_args: remaining,
    )

    completed = retry.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))

    assert json.loads(completed.body)["occurred_at"] == "2026-08-13T12:17:00Z"
    assert retry.status(OPERATION_ID).body == completed.body


@pytest.mark.parametrize(
    ("field", "replacement"),
    [("plan_sha256", "b" * 64), ("checkpoint_id", "another-valid-checkpoint")],
)
def test_status_identity_is_bound_to_authoritative_prepared_record(field, replacement):
    artifact = _artifact()
    gateway = FakeGateway()
    manager = _prepared_manager(gateway, artifact)
    attempt_key = next(key for key in gateway.controls if "/results/attempts/" in key)
    value = json.loads(gateway.controls.pop(attempt_key)[0])
    value[field] = replacement
    body = canonical_json_bytes(value)
    replacement_key = (
        attempt_key.rsplit("/", 1)[0]
        + "/"
        + f"{value['attempt_sequence']:06d}-"
        + __import__("hashlib").sha256(body).hexdigest()
        + ".json"
    )
    gateway.controls[replacement_key] = (body, "f" * 32)
    audit_keys = {key for key in gateway.controls if key.startswith(f"_retention/audits/{OPERATION_ID}/")}

    with pytest.raises(OperationFailure, match="status_invalid"):
        manager.status(OPERATION_ID)
    with pytest.raises(OperationFailure, match="status_invalid"):
        manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    assert {key for key in gateway.controls if key.startswith(f"_retention/audits/{OPERATION_ID}/")} == audit_keys


def test_status_and_duplicate_prepare_reject_missing_referenced_result_shard():
    artifact = _artifact()
    gateway = FakeGateway()
    manager = _prepared_manager(gateway, artifact)
    completed = manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    result_digest = json.loads(completed.body)["result_shards"][0]
    del gateway.controls[f"_retention/tombstones/{OPERATION_ID}/results/shards/{result_digest}.json"]
    request = PrepareRequest(OPERATION_ID, artifact, artifact.sha256, "review-86", "acceptance-engineering")

    with pytest.raises(OperationFailure, match="status_invalid"):
        manager.status(OPERATION_ID)
    with pytest.raises(OperationFailure, match="status_invalid"):
        manager.prepare(request)


def test_status_history_requires_first_prepared_and_monotone_deleted_set():
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda *_args: artifact,
    )
    gateway.delete_failure = GatewayFailure(
        "delete_partial",
        deleted_keys=(artifact.shards[0].records[0].key,),
    )
    with pytest.raises(OperationFailure, match="delete_partial"):
        manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))

    attempt_keys = sorted(key for key in gateway.controls if "/results/attempts/" in key)
    second_key = attempt_keys[-1]
    second_value = json.loads(gateway.controls.pop(second_key)[0])
    second_value["state"] = "prepared"
    second_value["primary_category"] = None
    second_body = canonical_json_bytes(second_value)
    gateway.controls[
        second_key.rsplit("/", 1)[0]
        + "/"
        + f"{second_value['attempt_sequence']:06d}-"
        + __import__("hashlib").sha256(second_body).hexdigest()
        + ".json"
    ] = (second_body, "f" * 32)

    with pytest.raises(OperationFailure, match="status_invalid|status_ambiguous"):
        manager.status(OPERATION_ID)


def test_status_history_rejects_transition_back_to_not_ready_after_refusal():
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda *_args: _artifact(decision="refused"),
    )
    with pytest.raises(OperationFailure, match="revalidation_mismatch"):
        manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    attempt_key = sorted(key for key in gateway.controls if "/results/attempts/" in key)[-1]
    value = json.loads(gateway.controls[attempt_key][0])
    value["attempt_sequence"] += 1
    value["occurred_at"] = "2026-08-13T12:16:00Z"
    value["state"] = "not_ready"
    value["primary_category"] = "quiescence_not_ready"
    body = canonical_json_bytes(value)
    gateway.controls[
        attempt_key.rsplit("/", 1)[0]
        + "/"
        + f"{value['attempt_sequence']:06d}-"
        + __import__("hashlib").sha256(body).hexdigest()
        + ".json"
    ] = (body, "f" * 32)

    with pytest.raises(OperationFailure, match="status_ambiguous"):
        manager.status(OPERATION_ID)


@pytest.mark.parametrize(
    ("state", "field", "invalid"),
    [("prepared", "head_requests", 1), ("completed", "postflight_inventory_sha256", None)],
)
def test_status_enforces_state_specific_evidence_invariants(state, field, invalid):
    artifact = _artifact()
    gateway = FakeGateway()
    manager = _prepared_manager(gateway, artifact)
    if state == "completed":
        manager = OperationManager(
            gateway,
            policy_sha256="a" * 64,
            now=lambda: NOW.replace(minute=15),
            revalidate=lambda *_args: artifact,
        )
        manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    attempt_key = sorted(key for key in gateway.controls if "/results/attempts/" in key)[-1]
    value = json.loads(gateway.controls.pop(attempt_key)[0])
    value[field] = invalid
    body = canonical_json_bytes(value)
    gateway.controls[
        attempt_key.rsplit("/", 1)[0]
        + "/"
        + f"{value['attempt_sequence']:06d}-"
        + __import__("hashlib").sha256(body).hexdigest()
        + ".json"
    ] = (body, "f" * 32)

    with pytest.raises(OperationFailure, match="status_invalid|status_ambiguous"):
        manager.status(OPERATION_ID)


def test_prepared_status_rejects_failed_result_classification():
    artifact = _artifact()
    gateway = FakeGateway()
    manager = _prepared_manager(gateway, artifact)
    attempt_key = next(key for key in gateway.controls if "/results/attempts/" in key)
    status = json.loads(gateway.controls.pop(attempt_key)[0])
    shard_key = f"_retention/tombstones/{OPERATION_ID}/results/shards/{status['result_shards'][0]}.json"
    classification = json.loads(gateway.controls[shard_key][0])
    classification[0]["outcome"] = "failed"
    shard_body = canonical_json_bytes(classification)
    shard_digest = __import__("hashlib").sha256(shard_body).hexdigest()
    gateway.controls[f"_retention/tombstones/{OPERATION_ID}/results/shards/{shard_digest}.json"] = (
        shard_body,
        "f" * 32,
    )
    status["result_shards"] = [shard_digest]
    status_body = canonical_json_bytes(status)
    gateway.controls[
        attempt_key.rsplit("/", 1)[0]
        + "/"
        + f"{status['attempt_sequence']:06d}-"
        + __import__("hashlib").sha256(status_body).hexdigest()
        + ".json"
    ] = (status_body, "e" * 32)

    with pytest.raises(OperationFailure, match="status_invalid"):
        manager.status(OPERATION_ID)


@pytest.mark.parametrize(
    ("field", "invalid"),
    [
        ("actor", "bad actor"),
        ("review", "bad review"),
        ("checkpoint_id", "another-valid-checkpoint"),
        ("prefix", f"{PREFIX}nested/"),
        ("prefix_sha256", "b" * 64),
        ("plan_sha256", "not-a-sha"),
        ("inventory_sha256", "not-a-sha"),
        ("manifest_shards", []),
        ("evaluated_at", "not-a-time"),
        ("prepared_at", "2026-08-13T11:59:59Z"),
    ],
)
def test_apply_strictly_validates_every_prepared_identity_before_repair_or_delete(field, invalid):
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    prepared_key = f"_retention/tombstones/{OPERATION_ID}/prepared.json"
    prepared = json.loads(gateway.controls[prepared_key][0])
    prepared[field] = invalid
    prepared_body = canonical_json_bytes(prepared)
    gateway.controls[prepared_key] = (prepared_body, "f" * 32)
    for key in tuple(gateway.controls):
        if key.startswith(f"_retention/audits/{OPERATION_ID}/"):
            del gateway.controls[key]

    with pytest.raises(OperationFailure, match="prepared_invalid"):
        OperationManager(
            gateway,
            policy_sha256="a" * 64,
            now=lambda: NOW.replace(minute=15),
            revalidate=lambda *_args: artifact,
        ).apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    assert not any(call[0] in {"head", "delete"} for call in gateway.calls)
    assert not any(key.startswith(f"_retention/audits/{OPERATION_ID}/") for key in gateway.controls)


def test_status_binds_prepared_inventory_digest_to_exact_manifest_records():
    artifact = _artifact()
    gateway = FakeGateway()
    manager = _prepared_manager(gateway, artifact)
    prepared_key = f"_retention/tombstones/{OPERATION_ID}/prepared.json"
    prepared = json.loads(gateway.controls[prepared_key][0])
    prepared["inventory_sha256"] = "b" * 64
    body = canonical_json_bytes(prepared)
    gateway.controls[prepared_key] = (body, "f" * 32)

    with pytest.raises(OperationFailure, match="status_invalid"):
        manager.status(OPERATION_ID)


@pytest.mark.parametrize(("field", "invalid"), [("schema_version", 999), ("deleted_bytes", 999)])
def test_malformed_status_is_rejected_before_missing_audit_repair(field, invalid):
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    gateway.head_failure = GatewayFailure("head_mismatch")
    gateway.create_failure_suffix = f"audits/{OPERATION_ID}/"
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda *_args: artifact,
    )
    with pytest.raises(OperationFailure, match="head_mismatch"):
        manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    gateway.create_failure_suffix = None
    attempt_key = sorted(key for key in gateway.controls if "/results/attempts/" in key)[-1]
    value = json.loads(gateway.controls.pop(attempt_key)[0])
    value[field] = invalid
    body = canonical_json_bytes(value)
    replacement = (
        attempt_key.rsplit("/", 1)[0]
        + "/"
        + f"{value['attempt_sequence']:06d}-"
        + __import__("hashlib").sha256(body).hexdigest()
        + ".json"
    )
    gateway.controls[replacement] = (body, "f" * 32)
    audit_keys = {key for key in gateway.controls if key.startswith(f"_retention/audits/{OPERATION_ID}/")}

    with pytest.raises(OperationFailure, match="status_invalid"):
        manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    assert {key for key in gateway.controls if key.startswith(f"_retention/audits/{OPERATION_ID}/")} == audit_keys


def test_post_delete_deadline_rebinds_bounded_cleanup_to_persist_partial_evidence():
    artifact = _artifact()
    clock = [0.0]

    class DeadlineGateway(FakeGateway):
        def __init__(self):
            super().__init__()
            self.check = None

        @contextmanager
        def operation_deadline(self, check):
            prior = self.check
            self.check = check
            try:
                check()
                yield
                check()
            finally:
                self.check = prior

        def _checked(self):
            if self.check is not None:
                self.check()

        def list_controls(self, prefix, *, max_keys):
            self._checked()
            return super().list_controls(prefix, max_keys=max_keys)

        def read_control(self, key, *, max_bytes):
            self._checked()
            return super().read_control(key, max_bytes=max_bytes)

        def create_control(self, key, body):
            self._checked()
            return super().create_control(key, body)

        def delete_records(self, records):
            deleted = super().delete_records(records)
            clock[0] = 901.0
            return deleted

    gateway = DeadlineGateway()
    _prepared_manager(gateway, artifact)
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        monotonic=lambda: clock[0],
        revalidate=lambda *_args: artifact,
    )

    with pytest.raises(OperationFailure, match="operation_deadline") as failure:
        manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    assert failure.value.partial is True
    latest = json.loads(manager.status(OPERATION_ID).body)
    assert latest["state"] == "partial"
    assert latest["deleted_objects"] == 2
    assert any(key.startswith(f"_retention/audits/{OPERATION_ID}/") for key in gateway.controls)


def test_completed_audit_binds_prepare_identity_policy_manifest_result_and_absence():
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda *_args: artifact,
    )
    manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    audit_key = next(
        key
        for key, (body, _etag) in gateway.controls.items()
        if key.startswith(f"_retention/audits/{OPERATION_ID}/") and json.loads(body)["decision"] == "completed"
    )
    audit = json.loads(gateway.controls[audit_key][0])
    assert audit["actor"] == "acceptance-engineering"
    assert audit["review"] == "review-86"
    assert audit["policy_sha256"] == "a" * 64
    assert audit["plan_sha256"] == artifact.sha256
    assert audit["planned_bytes"] == 3
    assert audit["manifest_shards"] == [shard.sha256 for shard in artifact.shards]
    assert audit["prepared_at"] == "2026-08-13T12:00:00Z"
    assert audit["evaluated_at"] == "2026-08-13T12:00:00Z"
    assert audit["postflight_inventory_sha256"] == inventory_sha256(())
    assert audit["remaining_objects"] == 0
    assert audit["primary_category"] is None
    assert re.fullmatch(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
        audit["attempt_id"],
    )
    assert audit_key.endswith(f"/{audit['attempt_id']}.json")


def test_repeated_equal_progress_attempts_use_distinct_append_only_sequence_keys():
    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: NOW.replace(minute=15),
        revalidate=lambda *_args: artifact,
    )
    gateway.delete_failure = OperationFailure("delete_partial")
    for _ in range(2):
        with pytest.raises(OperationFailure, match="delete_partial"):
            manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))
    attempt_keys = [key for key in gateway.controls if "/results/attempts/" in key]
    assert len(attempt_keys) == 3
    assert attempt_keys[0] != attempt_keys[1]
