from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[2]
LIVE = ROOT / "tests/scenarios/test_checkpoint_retention_live.py"


def _live():
    spec = importlib.util.spec_from_file_location("checkpoint_retention_live_helpers", LIVE)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _completed(*command, stdout="", stderr=""):
    return subprocess.CompletedProcess(command, 0, stdout, stderr)


def test_owned_stack_rejects_any_all_state_container_before_start_and_cleans_partial_start():
    live = _live()
    mutations = []
    with pytest.raises(RuntimeError, match="exclusive acceptance refused"):
        with live._owned_stack(
            runner=lambda *command, **_kwargs: mutations.append(command),
            probe=lambda: ("data-eng-lab-stopped",),
        ):
            pass
    assert mutations == []

    events = []
    probes = iter([(), ()])

    def runner(*command, **_kwargs):
        events.append(command)
        if command == ("./scripts/start-all.sh",):
            raise RuntimeError("credential=must-not-escape")
        return _completed(*command)

    with pytest.raises(RuntimeError, match="must-not-escape"):
        with live._owned_stack(runner=runner, probe=lambda: next(probes)):
            pass
    assert events == [("./scripts/start-all.sh",), ("./scripts/stop-all.sh",)]


def test_owned_stack_preserves_body_primary_and_sanitizes_cleanup_note():
    live = _live()
    probes = iter([(), ("leftover",)])
    primary = KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt) as failure:
        with live._owned_stack(
            runner=lambda *command, **_kwargs: _completed(*command),
            probe=lambda: next(probes),
        ):
            raise primary
    assert failure.value is primary
    rendered = "\n".join(getattr(primary, "__notes__", ()))
    assert "leftover" not in rendered
    assert "cleanup_failed" in rendered


def test_owned_fixture_objects_delete_only_registered_exact_keys_on_failure():
    live = _live()
    deleted = []

    class Client:
        def put_object(self, *, Bucket, Key, Body):
            assert Bucket == "checkpoints"
            assert Body in {b"state", b"sentinel"}

        def delete_objects(self, *, Bucket, Delete):
            assert Bucket == "checkpoints"
            keys = [item["Key"] for item in Delete["Objects"]]
            deleted.extend(keys)
            return {"Deleted": [{"Key": key} for key in keys]}

        def list_objects_v2(self, *, Bucket, Prefix, MaxKeys):
            assert Bucket == "checkpoints"
            assert MaxKeys == 1000
            assert Prefix.endswith("/")
            return {"IsTruncated": False}

    fixture = "streaming_test/550e8400-e29b-41d4-a716-446655440000/state/offset"
    sentinel = "unknown-retention/11111111-1111-4111-8111-111111111111/sentinel"
    with pytest.raises(RuntimeError, match="primary"):
        with live._owned_fixture_objects(Client()) as put:
            put(fixture, b"state")
            put(sentinel, b"sentinel")
            raise RuntimeError("primary")
    assert deleted == [fixture, sentinel]


def test_owned_fixture_objects_reject_broad_or_foreign_cleanup_targets_before_put():
    live = _live()

    class Client:
        def put_object(self, **_kwargs):
            raise AssertionError("must not mutate")

    with live._owned_fixture_objects(Client()) as put:
        for key in (
            "streaming_test/",
            "streaming_test/not-a-uuid/state/offset",
            "events/550e8400-e29b-41d4-a716-446655440000/state/offset",
            "_retention/leases/go-live-streaming-test-v1.json",
        ):
            with pytest.raises(AssertionError, match="owned fixture key"):
                put(key, b"x")


def test_owned_fixture_cleanup_failure_cannot_replace_body_primary():
    live = _live()
    primary = KeyboardInterrupt()

    class Client:
        def put_object(self, **_kwargs):
            return None

        def delete_objects(self, **_kwargs):
            raise RuntimeError("credential=must-not-escape")

    with pytest.raises(KeyboardInterrupt) as failure:
        with live._owned_fixture_objects(Client()) as put:
            put("unknown-retention/11111111-1111-4111-8111-111111111111/sentinel", b"x")
            raise primary
    assert failure.value is primary
    assert getattr(primary, "__notes__", ()) == ["owned_fixture_cleanup_failed"]


def test_runtime_capability_evidence_is_exactly_one_recent_control_and_is_cleaned():
    live = _live()
    started_at = datetime(2026, 8, 13, 12, 0, tzinfo=timezone.utc)
    owned_key = "_retention/capability/550e8400-e29b-41d4-a716-446655440000.json"
    old_key = "_retention/capability/11111111-1111-4111-8111-111111111111.json"
    deleted = []

    class Body:
        def __init__(self):
            self.closed = False

        def read(self, size):
            assert size == 129
            return b'{"profile":"minio-2025-09-manual-verified-readback","schema_version":1}'

        def close(self):
            self.closed = True

    class Client:
        def list_objects_v2(self, *, Bucket, Prefix, MaxKeys):
            assert Bucket == "checkpoints" and Prefix == "_retention/capability/" and MaxKeys == 1000
            remaining = [] if owned_key in deleted else [{"Key": owned_key, "LastModified": started_at}]
            return {
                "Contents": [
                    {"Key": old_key, "LastModified": started_at - timedelta(days=1)},
                    *remaining,
                ],
                "IsTruncated": False,
            }

        def get_object(self, *, Bucket, Key):
            assert Bucket == "checkpoints" and Key == owned_key
            return {"Body": Body()}

        def delete_objects(self, *, Bucket, Delete):
            assert Bucket == "checkpoints"
            keys = [item["Key"] for item in Delete["Objects"]]
            deleted.extend(keys)
            return {"Deleted": [{"Key": key} for key in keys], "Errors": []}

    with live._owned_runtime_capability(Client(), started_at) as key:
        assert key == owned_key
    assert deleted == [owned_key]


def test_maintenance_iam_probe_executes_exact_allowed_and_denied_calls_with_cleanup():
    live = _live()
    fixture = "streaming_test/550e8400-e29b-41d4-a716-446655440000/"
    deleted = []

    class Body:
        def read(self, size):
            assert size == 65
            return b"state"

        def close(self):
            return None

    class Root:
        def delete_object(self, *, Bucket, Key):
            assert Bucket == "checkpoints"
            deleted.append(Key)

        def list_objects_v2(self, **_kwargs):
            return {"IsTruncated": False}

    class Maintenance:
        def list_objects_v2(self, *, Bucket, Prefix, MaxKeys):
            if Prefix == "streaming_test/":
                raise ClientError({"Error": {"Code": "AccessDenied"}}, "ListObjectsV2")
            return {"IsTruncated": False, "Contents": [{}, {}]}

        def get_object(self, *, Bucket, Key):
            if Key.startswith("unknown-retention/"):
                raise ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")
            return {"Body": Body()}

        def put_object(self, *, Bucket, Key, Body):
            if Key.startswith(fixture):
                raise ClientError({"Error": {"Code": "AccessDenied"}}, "PutObject")

        def delete_object(self, *, Bucket, Key):
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "DeleteObject")

    evidence = live._prove_maintenance_iam(
        Root(), Maintenance(), fixture, "unknown-retention/11111111-1111-4111-8111-111111111111/sentinel"
    )
    assert evidence["allowed"] == 3 and evidence["denied"] == 4
    assert deleted == [evidence["control_key"]]


def test_disposable_identity_and_review_facts_are_exact_and_bounded():
    live = _live()
    identity = live._fixture_identity("550e8400-e29b-41d4-a716-446655440000")
    assert identity == {
        "checkpoint_id": "go-live-streaming-test-v1",
        "generation": {"run_uuid": "550e8400-e29b-41d4-a716-446655440000"},
        "prefix": "streaming_test/550e8400-e29b-41d4-a716-446655440000/",
        "workload": "go-live-streaming-test",
    }
    assert live._review_facts("2026-08-13T12:00:00Z") == {
        "actor": "issue86-live-acceptance",
        "evaluated_at": "2026-08-13T12:00:00Z",
    }
    for invalid in ("not-a-uuid", "550E8400-E29B-41D4-A716-446655440000", "../escape"):
        with pytest.raises(AssertionError):
            live._fixture_identity(invalid)


def test_snapshot_comparison_rejects_production_or_policy_mutation_but_allows_named_fixture_controls():
    live = _live()
    before = {
        "policy_sha256": "a" * 64,
        "production": {"events/": ["one"], "online_retail_cdc/": []},
        "controls": {"_retention/leases/streaming-events-v1.json": "b" * 64},
    }
    assert live._assert_stable_snapshot(before, json.loads(json.dumps(before))) is None
    for field, value in (("policy_sha256", "c" * 64), ("production", {"events/": []}), ("controls", {})):
        after = json.loads(json.dumps(before))
        after[field] = value
        with pytest.raises(AssertionError, match="snapshot changed"):
            live._assert_stable_snapshot(before, after)


def test_operation_evidence_requires_exact_plan_prepare_result_and_audit_identity():
    live = _live()
    operation_id = "550e8400-e29b-41d4-a716-446655440000"
    prefix = "streaming_test/11111111-1111-4111-8111-111111111111/"
    plan_sha = "a" * 64
    evidence = {
        "audit": {"operation_id": operation_id, "plan_sha256": plan_sha, "decision": "completed"},
        "plan": {"summary": {"decision": "eligible", "prefix": prefix}, "plan_sha256": plan_sha},
        "prepared": {"operation_id": operation_id, "plan_sha256": plan_sha, "prefix": prefix},
        "result": {"operation_id": operation_id, "plan_sha256": plan_sha, "state": "completed"},
    }
    assert live._assert_operation_evidence(evidence, operation_id, plan_sha, prefix) is None
    for category in evidence:
        changed = json.loads(json.dumps(evidence))
        if category == "plan":
            changed[category]["plan_sha256"] = "b" * 64
        else:
            changed[category].update(operation_id="22222222-2222-4222-8222-222222222222")
        with pytest.raises(AssertionError, match="evidence mismatch"):
            live._assert_operation_evidence(changed, operation_id, plan_sha, prefix)


def test_metrics_and_logs_are_bounded_fixed_and_redacted():
    live = _live()
    body = (
        b"\n".join(
            [
                b"checkpoint_retention_plans_total 3",
                b'checkpoint_retention_prepared_total{outcome="completed"} 1',
                b"checkpoint_retention_deleted_objects_total 4",
            ]
        )
        + b"\n"
    )
    metrics = live._parse_metrics(body)
    assert metrics["checkpoint_retention_plans_total"] == 3
    assert metrics['checkpoint_retention_prepared_total{outcome="completed"}'] == 1
    invalid = (
        b"x" * 65_537,
        body + b"fixture_uuid 1\n",
        body + b"api-secret-token\n",
        body + b'checkpoint_retention_plans_total{prefix="unique-fixture"} 1\n',
        body + b'checkpoint_retention_prepared_total{outcome="unknown"} 1\n',
        body + b"checkpoint_retention_plans_total 1\n",
    )
    for invalid in invalid:
        with pytest.raises(AssertionError):
            live._parse_metrics(invalid)


def test_live_module_is_a_genuine_run_infra_opt_in_with_no_refresh_or_family_delete():
    source = LIVE.read_text(encoding="utf-8")
    assert 'os.environ.get("RUN_INFRA") != "1"' in source
    assert "@pytest.mark.skipif" in source
    assert "download_datasets.py" not in source
    assert "--refresh" not in source
    assert 'delete_object(Bucket="checkpoints", Key="streaming_test/"' not in source
    assert 'delete_objects(Bucket="checkpoints", Delete={"Prefix"' not in source
    assert "remove_volume" not in source


def test_live_identity_is_one_explicit_bounded_value_for_provisioning_and_probe():
    live = _live()
    source = LIVE.read_text(encoding="utf-8")

    assert 3 <= len(live.LIVE_RETENTION_ACCESS_KEY) <= 20
    assert source.count('LIVE_RETENTION_ACCESS_KEY = "retention86live"') == 1
    assert 'os.environ["MINIO_RETENTION_ACCESS_KEY"] = LIVE_RETENTION_ACCESS_KEY' in source
    assert "_client(\n                endpoint,\n                LIVE_RETENTION_ACCESS_KEY," in source
