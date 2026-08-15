from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scripts.checkpoints.planner import PlanFailure, PlanRequest, RetentionPlanner, write_plan_exclusive
from scripts.checkpoints.policy import load_policy
from scripts.checkpoints.records import ObjectRecord

ROOT = Path(__file__).resolve().parents[2]
POLICY = load_policy(ROOT / "checkpoints" / "retention-policy.yaml")
NOW = datetime(2026, 8, 13, 12, 0, 0, tzinfo=timezone.utc)
RUN_UUID = "550e8400-e29b-41d4-a716-446655440000"
PREFIX = f"streaming_test/{RUN_UUID}/"
CHECKPOINT_ID = "go-live-streaming-test-v1"


class ReadOnlyGateway:
    def __init__(self, *, empty=False):
        self.calls = []
        self.records = (
            ()
            if empty
            else (
                ObjectRecord(f"{PREFIX}state/a", "a" * 32, 1, NOW - timedelta(days=1)),
                ObjectRecord(f"{PREFIX}state/b", "b" * 32, 2, NOW - timedelta(days=1)),
            )
        )

    def inventory(self, prefix):
        self.calls.append(("inventory", prefix))
        return self.records

    def read_control(self, key, *, max_bytes):
        self.calls.append(("read", key, max_bytes))
        if "/leases/" in key:
            value = {
                "acquired_at": "2026-08-12T12:00:00Z",
                "checkpoint_id": CHECKPOINT_ID,
                "epoch": "11111111-1111-4111-8111-111111111111",
                "expires_at": "2026-08-12T12:10:00Z",
                "heartbeat_at": "2026-08-12T12:00:00Z",
                "owner_id": "acceptance-engineering",
                "prefix": PREFIX,
                "schema_version": 1,
                "session_id": "issue86-live-001",
                "state": "stopped",
                "terminal_evidence": {
                    "exclusive_run": True,
                    "generation": {"run_uuid": RUN_UUID},
                    "successful": True,
                },
                "workload": "go-live-streaming-test",
            }
        else:
            value = {
                "checkpoint_id": CHECKPOINT_ID,
                "generation": {"run_uuid": RUN_UUID},
                "occurred_at": "2026-08-12T12:00:00Z",
                "prefix": PREFIX,
                "recovery_approved": True,
                "schema_version": 1,
                "sink_disposition_approved": True,
                "source_available": True,
                "state": "stopped",
                "exclusive_run": True,
                "successful": True,
            }
        return json.dumps(value, sort_keys=True, separators=(",", ":")).encode(), "c" * 32

    def __getattr__(self, name):
        if name in {"create_control", "replace_lease", "delete_records", "head_record"}:
            raise AssertionError(f"dry run attempted mutation: {name}")
        raise AttributeError(name)


def _request(**changes):
    values = {
        "checkpoint_id": CHECKPOINT_ID,
        "prefix": PREFIX,
        "actor": "acceptance-engineering",
        "evaluated_at": NOW,
    }
    values.update(changes)
    return PlanRequest(**values)


def test_planner_is_read_only_and_binds_deterministic_summary_shards_and_sha():
    gateway = ReadOnlyGateway()
    planner = RetentionPlanner(gateway, POLICY)

    first = planner.plan(_request())
    second = planner.plan(_request())

    summary = dict(first.summary)
    assert summary["schema_version"] == 1
    assert summary["checkpoint_id"] == CHECKPOINT_ID
    assert summary["prefix"] == PREFIX
    assert summary["actor"] == "acceptance-engineering"
    assert summary["decision"] == "eligible"
    assert summary["inventory"]["object_count"] == 2
    assert summary["inventory"]["total_bytes"] == 3
    assert len(summary["inventory"]["sha256"]) == 64
    assert summary["manifest_shards"] == tuple(shard.sha256 for shard in first.shards)
    assert first == second
    assert (
        gateway.calls
        == [
            ("inventory", PREFIX),
            ("read", f"_retention/leases/{CHECKPOINT_ID}.json", 65_536),
            ("read", f"_retention/terminals/{CHECKPOINT_ID}.json", 65_536),
        ]
        * 2
    )


def test_planner_materializes_policy_scale_inventory_with_explicit_node_bound():
    gateway = ReadOnlyGateway()
    gateway.records = tuple(
        ObjectRecord(f"{PREFIX}state/{index:05d}", f"{index:032x}", 1, NOW - timedelta(days=1))
        for index in range(1_000)
    )

    artifact = RetentionPlanner(gateway, POLICY).plan(_request())

    assert artifact.summary["inventory"]["object_count"] == 1_000
    assert len(json.loads(artifact.body)["shards"][0]) == 1_000


@pytest.mark.parametrize(
    "changes",
    [
        {"checkpoint_id": "foreign-v1"},
        {"prefix": "streaming_test/"},
        {"actor": "contains spaces"},
        {"actor": "x" * 129},
        {"evaluated_at": NOW.replace(microsecond=1)},
        {"evaluated_at": NOW.replace(tzinfo=None)},
    ],
)
def test_planner_rejects_wrong_identity_actor_and_clock_before_mutation(changes):
    gateway = ReadOnlyGateway()
    with pytest.raises(PlanFailure):
        RetentionPlanner(gateway, POLICY).plan(_request(**changes))
    assert gateway.calls == []


def test_planner_refuses_empty_inventory_without_any_write():
    gateway = ReadOnlyGateway(empty=True)

    with pytest.raises(PlanFailure, match="inventory_empty"):
        RetentionPlanner(gateway, POLICY).plan(_request())

    assert not any(call[0] not in {"inventory", "read"} for call in gateway.calls)


def test_planner_refuses_terminal_identity_drift_before_accepting_a_plan():
    class ForeignTerminalGateway(ReadOnlyGateway):
        def read_control(self, key, *, max_bytes):
            body, etag = super().read_control(key, max_bytes=max_bytes)
            if "/terminals/" in key:
                value = json.loads(body)
                value["prefix"] = "streaming_test/00000000-0000-0000-0000-000000000000/"
                body = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
            return body, etag

    with pytest.raises(PlanFailure, match="terminal_identity_mismatch"):
        RetentionPlanner(ForeignTerminalGateway(), POLICY).plan(_request())


@pytest.mark.parametrize("control", ["leases", "terminals"])
def test_planner_requires_exact_schema_version_one_for_every_control(control):
    class WrongVersionGateway(ReadOnlyGateway):
        def read_control(self, key, *, max_bytes):
            body, etag = super().read_control(key, max_bytes=max_bytes)
            if f"/{control}/" in key:
                value = json.loads(body)
                value["schema_version"] = 2
                body = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
            return body, etag

    with pytest.raises(PlanFailure, match="lease_malformed|terminal_malformed"):
        RetentionPlanner(WrongVersionGateway(), POLICY).plan(_request())


def test_active_rotated_generation_refuses_before_decoding_stale_prior_terminal():
    current_uuid = "11111111-1111-4111-8111-111111111111"
    current_prefix = f"streaming_test/{current_uuid}/"

    class RotatedActiveGateway(ReadOnlyGateway):
        def __init__(self):
            super().__init__()
            self.records = (ObjectRecord(f"{current_prefix}state/a", "a" * 32, 1, NOW - timedelta(days=1)),)

        def read_control(self, key, *, max_bytes):
            body, etag = super().read_control(key, max_bytes=max_bytes)
            value = json.loads(body)
            if "/leases/" in key:
                value.update(prefix=current_prefix, state="active", expires_at="2026-08-13T12:10:00Z")
            return json.dumps(value, sort_keys=True, separators=(",", ":")).encode(), etag

    artifact = RetentionPlanner(RotatedActiveGateway(), POLICY).plan(_request(prefix=current_prefix, evaluated_at=NOW))

    assert artifact.summary["decision"] == "refused"
    assert "lease_active" in artifact.summary["refusal_codes"]
    assert "terminal_missing" in artifact.summary["refusal_codes"]


def test_stopped_rotated_generation_cannot_be_eligible_with_stale_prior_terminal():
    current_uuid = "11111111-1111-4111-8111-111111111111"
    current_prefix = f"streaming_test/{current_uuid}/"

    class RotatedStoppedGateway(ReadOnlyGateway):
        def __init__(self):
            super().__init__()
            self.records = (ObjectRecord(f"{current_prefix}state/a", "a" * 32, 1, NOW - timedelta(days=1)),)

        def read_control(self, key, *, max_bytes):
            body, etag = super().read_control(key, max_bytes=max_bytes)
            value = json.loads(body)
            if "/leases/" in key:
                value["prefix"] = current_prefix
            return json.dumps(value, sort_keys=True, separators=(",", ":")).encode(), etag

    with pytest.raises(PlanFailure, match="terminal_identity_mismatch"):
        RetentionPlanner(RotatedStoppedGateway(), POLICY).plan(_request(prefix=current_prefix))


def test_planner_treats_absent_terminal_as_a_refusal_fact_not_transport_failure():
    class MissingTerminalGateway(ReadOnlyGateway):
        def read_control(self, key, *, max_bytes):
            if "/terminals/" in key:
                from scripts.checkpoints.s3_gateway import GatewayFailure

                raise GatewayFailure("control_missing")
            return super().read_control(key, max_bytes=max_bytes)

    artifact = RetentionPlanner(MissingTerminalGateway(), POLICY).plan(_request(evaluated_at=NOW - timedelta(days=1)))

    assert artifact.summary["decision"] == "refused"
    assert "terminal_missing" in artifact.summary["refusal_codes"]


def test_exclusive_plan_write_is_mode_0600_atomic_and_refuses_existing_target(tmp_path):
    artifact = RetentionPlanner(ReadOnlyGateway(), POLICY).plan(_request())
    target = tmp_path / "reviewed-plan.json"

    write_plan_exclusive(target, artifact)

    assert target.read_bytes() == artifact.body
    assert os.stat(target).st_mode & 0o777 == 0o600
    assert list(tmp_path.iterdir()) == [target]
    with pytest.raises(PlanFailure, match="plan_target_exists"):
        write_plan_exclusive(target, artifact)
    assert target.read_bytes() == artifact.body
