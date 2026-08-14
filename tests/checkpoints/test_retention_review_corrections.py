from __future__ import annotations

import hashlib
import json
import types
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scripts.checkpoints import planner, service
from scripts.checkpoints import retention as retention_cli
from scripts.checkpoints.metrics import render_metrics
from scripts.checkpoints.operations import ApplyRequest, OperationFailure, OperationManager, PrepareRequest
from scripts.checkpoints.records import (
    ObjectRecord,
    PlanArtifact,
    canonical_json_bytes,
    decode_plan_artifact,
    inventory_sha256,
    shard_inventory,
)

NOW = datetime(2026, 8, 13, 12, 0, tzinfo=timezone.utc)
PREFIX = "streaming_test/11111111-1111-4111-8111-111111111111/"


class Backend:
    def __init__(self):
        self.calls = []

    def invoke(self, action, payload, operation_id=None):
        self.calls.append((action, payload, operation_id))
        return {"state": "accepted"}

    def health(self):
        return {"ready": True}

    def metrics(self):
        return b""


def _headers(body: bytes, token: str):
    return (
        ("Authorization", f"Bearer {token}"),
        ("Content-Type", "application/json"),
        ("Content-Length", str(len(body))),
    )


@pytest.mark.parametrize(
    "body",
    [
        b'{"acquired_at":"2026-08-13T12:00:00Z","checkpoint_id":"go-live-streaming-test-v1",'
        b'"expires_at":"2026-08-13T13:00:00Z","heartbeat_at":"2026-08-13T12:00:00Z",'
        b'"prefix":"streaming_test/11111111-1111-4111-8111-111111111111/",'
        b'"state":"active","state":"stopped"}',
        b'{"acquired_at":"2026-08-13T12:00:00Z","checkpoint_id":"go-live-streaming-test-v1",'
        b'"expires_at":"2026-08-13T13:00:00Z","heartbeat_at":"2026-08-13T12:00:00Z",'
        b'"prefix":"streaming_test/11111111-1111-4111-8111-111111111111/",'
        b'"state":"stopped","unexpected":true}',
    ],
)
def test_planner_lease_controls_reject_duplicate_and_unknown_fields(body):
    with pytest.raises(planner.PlanFailure, match="lease_malformed"):
        planner._decode_lease(body, "a" * 32)


def test_lease_and_operator_tokens_are_exact_route_scopes():
    backend = Backend()
    app = service.RetentionApplication(
        backend,
        lease_token="lease-only-token",
        operator_token="manual-operator-token",
    )
    body = b"{}"

    assert app.dispatch("POST", "/v1/leases/acquire", _headers(body, "lease-only-token"), body).status == 200
    with pytest.raises(service.ServiceFailure, match="unauthorized"):
        app.dispatch("POST", "/v1/plans", _headers(body, "lease-only-token"), body)
    with pytest.raises(service.ServiceFailure, match="unauthorized"):
        app.dispatch("POST", "/v1/leases/acquire", _headers(body, "manual-operator-token"), body)
    assert app.dispatch("POST", "/v1/plans", _headers(body, "manual-operator-token"), body).status == 200


def test_destructive_routes_reject_every_non_disposable_prefix_before_operations(monkeypatch):
    operations = types.SimpleNamespace(
        prepare=lambda _request: pytest.fail("non-disposable prepare reached operations"),
        apply=lambda _request: pytest.fail("non-disposable apply reached operations"),
    )
    policy = types.SimpleNamespace(
        entries={"streaming-events-v1": types.SimpleNamespace(durability="durable_stream")},
        match_prefix=lambda _prefix: types.SimpleNamespace(checkpoint_id="streaming-events-v1"),
        bounds=types.SimpleNamespace(max_manifest_shard_bytes=1_048_576),
    )
    backend = service.RuntimeBackend(
        gateway=types.SimpleNamespace(probe_capabilities=lambda: {"automatic_apply": False}),
        leases=object(),
        planner=object(),
        operations=operations,
        policy=policy,
        destructive_enabled=True,
    )
    monkeypatch.setattr(
        service,
        "decode_plan_artifact",
        lambda *_args, **_kwargs: types.SimpleNamespace(summary={"prefix": "events/"}),
    )

    with pytest.raises(service.ServiceFailure, match="destructive_scope_invalid"):
        backend.invoke(
            "prepare",
            {"actor": "operator", "plan": {}, "plan_sha256": "a" * 64, "review": "review-86"},
            None,
        )
    with pytest.raises(service.ServiceFailure, match="destructive_scope_invalid"):
        backend.invoke(
            "apply",
            {"confirm_prefix": "events/", "plan_sha256": "a" * 64},
            "550e8400-e29b-41d4-a716-446655440000",
        )


def test_policy_sized_plan_round_trip_has_explicit_large_node_bound():
    records = tuple(ObjectRecord(f"{PREFIX}state/{index:05d}", f"{index:032x}", 1, NOW) for index in range(1_000))
    shards = shard_inventory(records, 1_048_576)
    summary = {
        "actor": "operator",
        "checkpoint_id": "go-live-streaming-test-v1",
        "decision": "eligible",
        "eligible_after": "2026-08-13T12:00:00Z",
        "evaluated_at": "2026-08-13T12:00:00Z",
        "inventory": {
            "newest_last_modified": "2026-08-13T12:00:00Z",
            "object_count": len(records),
            "sha256": inventory_sha256(records),
            "total_bytes": len(records),
        },
        "manifest_shards": tuple(shard.sha256 for shard in shards),
        "policy_sha256": "a" * 64,
        "prefix": PREFIX,
        "prefix_sha256": hashlib.sha256(PREFIX.encode()).hexdigest(),
        "refusal_codes": (),
        "retention_anchor": "2026-08-13T12:00:00Z",
        "schema_version": 1,
    }
    value = {"schema_version": 1, "summary": summary, "shards": [json.loads(shard.body) for shard in shards]}
    body = canonical_json_bytes(value, max_bytes=128 * 1024 * 1024, max_nodes=600_128)

    decoded = decode_plan_artifact(
        body,
        max_body_bytes=128 * 1024 * 1024,
        max_shard_bytes=1_048_576,
        max_nodes=600_128,
    )

    assert sum(len(shard.records) for shard in decoded.shards) == 1_000


def test_health_requires_observed_storage_capability_evidence():
    gateway = types.SimpleNamespace(
        probe_capabilities=lambda: {"profile": "manual-verified-readback", "automatic_apply": False}
    )
    backend = service.RuntimeBackend(
        gateway=gateway,
        leases=object(),
        planner=object(),
        operations=object(),
        policy=types.SimpleNamespace(entries={}),
        destructive_enabled=False,
    )

    with pytest.raises(service.ServiceFailure, match="capability_failed"):
        backend.health()


def test_health_runs_the_observed_capability_probe_once_per_runtime():
    calls = []
    capabilities = {
        "automatic_apply": False,
        "conditional_create": True,
        "conditional_create_conflict": True,
        "conditional_delete": False,
        "conditional_replace_verified_readback": True,
        "data_put_denied": True,
        "exact_leaf_delete": True,
        "exact_leaf_get": True,
        "exact_leaf_list": True,
        "multi_delete": True,
        "observed": True,
        "other_bucket_denied": True,
        "profile": "minio-2025-09-manual-verified-readback",
        "root_list_denied": True,
        "stale_replace_denied": True,
        "unknown_control_denied": True,
    }

    def probe():
        calls.append("probe")
        return dict(capabilities)

    backend = service.RuntimeBackend(
        gateway=types.SimpleNamespace(probe_capabilities=probe),
        leases=object(),
        planner=object(),
        operations=object(),
        policy=types.SimpleNamespace(entries={}),
        destructive_enabled=False,
    )

    assert backend.health()["ready"] is True
    assert backend.health()["ready"] is True
    assert calls == ["probe"]


def test_categorized_partial_response_is_not_rewritten_to_backend_failure():
    class Partial(Backend):
        def invoke(self, *_args, **_kwargs):
            raise service.ServiceFailure("delete_partial", status=409, state="partial")

    app = service.RetentionApplication(
        Partial(), lease_token="lease-only-token", operator_token="manual-operator-token"
    )
    body = b"{}"
    response = app.dispatch(
        "POST",
        "/v1/operations/550e8400-e29b-41d4-a716-446655440000/apply",
        _headers(body, "manual-operator-token"),
        body,
    )
    assert response.status == 409
    assert json.loads(response.body) == {"code": "delete_partial", "state": "partial"}


def test_http_server_has_explicit_worker_and_request_timeout_bounds():
    app = service.RetentionApplication(
        Backend(), lease_token="lease-only-token", operator_token="manual-operator-token"
    )
    server = service.create_server(
        ("0.0.0.0", 8080),
        app,
        max_workers=8,
        request_timeout_seconds=15,
    )
    try:
        assert server.max_workers == 8
        assert server.request_timeout_seconds == 15
    finally:
        server.server_close()


def test_runtime_shares_one_checkpoint_lock_registry_between_leases_and_apply(monkeypatch):
    policy = types.SimpleNamespace(
        lease=types.SimpleNamespace(quiescence_seconds=900),
        bounds=types.SimpleNamespace(max_summary_bytes=65_536, max_delete_keys=1_000, max_active_seconds=900),
    )
    client = types.SimpleNamespace(close=lambda: None)
    monkeypatch.setenv("MINIO_RETENTION_ACCESS_KEY", "retention-user")
    monkeypatch.setenv("MINIO_RETENTION_SECRET_KEY", "retention-secret")
    monkeypatch.setattr(service, "load_policy", lambda _path: policy)
    monkeypatch.setattr(service, "build_s3_client", lambda *_args: client)
    monkeypatch.setattr(service, "_policy_digest", lambda _policy: "a" * 64)
    startup_health = []
    monkeypatch.setattr(
        service.RuntimeBackend,
        "health",
        lambda _self: startup_health.append("observed") or {"ready": True},
    )

    backend = service.build_runtime()

    assert backend._leases._locks is backend._operations._locks
    assert startup_health == ["observed"]


def test_runtime_closes_storage_client_when_startup_capability_probe_fails(monkeypatch):
    policy = types.SimpleNamespace(
        lease=types.SimpleNamespace(quiescence_seconds=900),
        bounds=types.SimpleNamespace(max_summary_bytes=65_536, max_delete_keys=1_000, max_active_seconds=900),
    )
    closed = []
    client = types.SimpleNamespace(close=lambda: closed.append("closed"))
    monkeypatch.setenv("MINIO_RETENTION_ACCESS_KEY", "retention-user")
    monkeypatch.setenv("MINIO_RETENTION_SECRET_KEY", "retention-secret")
    monkeypatch.setattr(service, "load_policy", lambda _path: policy)
    monkeypatch.setattr(service, "build_s3_client", lambda *_args: client)
    monkeypatch.setattr(service, "_policy_digest", lambda _policy: "a" * 64)
    monkeypatch.setattr(
        service.RuntimeBackend,
        "health",
        lambda _self: (_ for _ in ()).throw(service.ServiceFailure("capability_failed")),
    )

    with pytest.raises(service.ServiceFailure, match="capability_failed"):
        service.build_runtime()
    assert closed == ["closed"]


def test_apply_holds_checkpoint_lock_across_revalidation_head_delete_and_status():
    events = []

    class Locks:
        @contextmanager
        def hold(self, checkpoint_id):
            events.append(("lock-enter", checkpoint_id))
            yield
            events.append(("lock-exit", checkpoint_id))

    class Gateway:
        def __init__(self):
            self.controls = {}
            self.data = ()

        def create_control(self, key, body):
            self.controls[key] = (body, "a" * 32)
            events.append(("create", key))
            return "a" * 32

        def read_control(self, key, *, max_bytes):
            return self.controls[key]

        def list_controls(self, prefix, *, max_keys):
            return tuple(sorted(key for key in self.controls if key.startswith(prefix)))

        def head_record(self, _record):
            events.append(("head",))

        def delete_records(self, records):
            events.append(("delete",))
            self.data = tuple(record for record in self.data if record not in records)

        def inventory(self, _prefix):
            events.append(("inventory",))
            return self.data

    gateway = Gateway()
    records = (ObjectRecord(f"{PREFIX}state/a", "1" * 32, 1, NOW),)
    gateway.data = records
    shards = shard_inventory(records, 1_048_576)
    summary = {
        "actor": "operator",
        "checkpoint_id": "go-live-streaming-test-v1",
        "decision": "eligible",
        "eligible_after": "2026-08-13T12:00:00Z",
        "evaluated_at": "2026-08-13T12:00:00Z",
        "inventory": {
            "newest_last_modified": "2026-08-13T12:00:00Z",
            "object_count": 1,
            "sha256": inventory_sha256(records),
            "total_bytes": 1,
        },
        "manifest_shards": tuple(shard.sha256 for shard in shards),
        "policy_sha256": "a" * 64,
        "prefix": PREFIX,
        "prefix_sha256": hashlib.sha256(PREFIX.encode()).hexdigest(),
        "refusal_codes": (),
        "retention_anchor": "2026-08-13T12:00:00Z",
        "schema_version": 1,
    }
    body = canonical_json_bytes({"schema_version": 1, "summary": summary, "shards": [json.loads(shards[0].body)]})
    artifact = PlanArtifact(summary, shards, body, hashlib.sha256(body).hexdigest())
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: datetime(2026, 8, 13, 12, 15, tzinfo=timezone.utc),
        revalidate=lambda *_args: events.append(("revalidate",)) or artifact,
        locks=Locks(),
        quiescence_seconds=0,
    )
    manager.prepare(
        PrepareRequest("550e8400-e29b-41d4-a716-446655440000", artifact, artifact.sha256, "review-86", "operator")
    )
    events.clear()

    manager.apply(ApplyRequest("550e8400-e29b-41d4-a716-446655440000", artifact.sha256, PREFIX))

    assert events[0] == ("lock-enter", "go-live-streaming-test-v1")
    assert events[-1] == ("lock-exit", "go-live-streaming-test-v1")
    assert [event[0] for event in events].index("revalidate") < [event[0] for event in events].index("head")
    assert [event[0] for event in events].index("delete") < [event[0] for event in events].index("create")


def test_apply_deadline_covers_revalidation_head_and_prevents_late_delete():
    from tests.checkpoints.test_retention_operations import OPERATION_ID, FakeGateway, _artifact, _prepared_manager

    artifact = _artifact()
    gateway = FakeGateway()
    _prepared_manager(gateway, artifact)
    ticks = iter((0.0, 1.0, 2.0, 901.0))
    manager = OperationManager(
        gateway,
        policy_sha256="a" * 64,
        now=lambda: datetime(2026, 8, 13, 12, 15, tzinfo=timezone.utc),
        monotonic=lambda: next(ticks),
        max_active_seconds=900,
        revalidate=lambda *_args: artifact,
    )
    with pytest.raises(OperationFailure, match="operation_deadline"):
        manager.apply(ApplyRequest(OPERATION_ID, artifact.sha256, PREFIX))

    assert not any(call[0] in {"head", "delete"} for call in gateway.calls)
    assert not any("/results/" in call[1] for call in gateway.calls if call[0] == "create")


def test_plan_metrics_publish_inventory_eligibility_and_refusal_outcomes():
    artifact = types.SimpleNamespace(
        body=json.dumps(
            {
                "schema_version": 1,
                "shards": [[]],
                "summary": {
                    "checkpoint_id": "go-live-streaming-test-v1",
                    "decision": "refused",
                    "inventory": {"object_count": 3, "total_bytes": 21},
                    "refusal_codes": ["lease_active"],
                },
            },
            separators=(",", ":"),
        ).encode(),
    )
    backend = service.RuntimeBackend(
        gateway=types.SimpleNamespace(probe_capabilities=lambda: {"automatic_apply": False}),
        leases=object(),
        planner=types.SimpleNamespace(plan=lambda _request: artifact),
        operations=object(),
        policy=types.SimpleNamespace(entries={}),
        destructive_enabled=False,
    )
    backend.invoke(
        "plan",
        {
            "actor": "operator",
            "checkpoint_id": "go-live-streaming-test-v1",
            "prefix": PREFIX,
        },
        None,
    )
    metrics = backend.metrics()

    assert b'checkpoint_retention_objects{checkpoint_id="go-live-streaming-test-v1"} 3\n' in metrics
    assert b'checkpoint_retention_bytes{checkpoint_id="go-live-streaming-test-v1"} 21\n' in metrics
    assert b'checkpoint_retention_eligible_bytes{checkpoint_id="go-live-streaming-test-v1"} 0\n' in metrics
    assert b'checkpoint_retention_refusals_total{refusal_code="lease_active"} 1\n' in metrics


@pytest.mark.parametrize(
    "body",
    [
        b'{"checkpoint_id":"go-live-streaming-test-v1","exclusive_run":true,'
        b'"generation":{},"occurred_at":"2026-08-13T12:00:00Z","prefix":"'
        + PREFIX.encode()
        + b'","recovery_approved":true,"schema_version":1,"sink_disposition_approved":true,'
        b'"source_available":true,"state":"stopped","successful":false,"successful":true}',
        b'{"checkpoint_id":"go-live-streaming-test-v1","exclusive_run":true,'
        b'"generation":{},"occurred_at":"2026-08-13T12:00:00Z","prefix":"'
        + PREFIX.encode()
        + b'","recovery_approved":true,"schema_version":1,"sink_disposition_approved":true,'
        b'"source_available":true,"state":"stopped","successful":true,"unknown":false}',
    ],
)
def test_planner_terminal_controls_reject_duplicate_and_unknown_fields(body):
    with pytest.raises(planner.PlanFailure, match="terminal_malformed"):
        planner._decode_terminal(body, "go-live-streaming-test-v1", PREFIX)


def test_cli_preserves_policy_sized_plan_response(monkeypatch):
    value = {"schema_version": 1, "summary": {"padding": "x" * 70_000}, "shards": []}
    body = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()

    class Response:
        def read(self, size):
            assert size == retention_cli._MAX_PLAN_BODY + 1
            return body

        def close(self):
            return None

    monkeypatch.setattr(retention_cli, "_open", lambda _request, timeout: Response())
    assert retention_cli._request("/v1/plans", {"actor": "operator"}, "token") == body


def test_metrics_registry_accepts_every_policy_emitted_refusal_and_never_breaks_endpoint():
    body = render_metrics({"checkpoint_retention_refusals_total": {("generation_identity_mismatch",): 1}})
    assert b"generation_identity_mismatch" in body


def test_runtime_returns_any_persisted_destructive_partial_as_partial():
    partial = canonical_json_bytes(
        {
            "checkpoint_id": "go-live-streaming-test-v1",
            "deleted_bytes": 1,
            "deleted_objects": 1,
            "operation_id": "550e8400-e29b-41d4-a716-446655440000",
            "plan_sha256": "a" * 64,
            "planned_objects": 2,
            "primary_category": "postflight_not_empty",
            "remaining_bytes": 1,
            "remaining_objects": 1,
            "result_shards": ["b" * 64],
            "schema_version": 1,
            "state": "partial",
        }
    )
    operations = types.SimpleNamespace(
        apply=lambda _request: (_ for _ in ()).throw(OperationFailure("postflight_not_empty", partial=True)),
        status=lambda operation_id: types.SimpleNamespace(operation_id=operation_id, state="partial", body=partial),
    )
    policy = types.SimpleNamespace(
        entries={"go-live-streaming-test-v1": types.SimpleNamespace(durability="disposable_acceptance")},
        match_prefix=lambda _prefix: types.SimpleNamespace(checkpoint_id="go-live-streaming-test-v1"),
    )
    backend = service.RuntimeBackend(
        gateway=types.SimpleNamespace(probe_capabilities=lambda: {}),
        leases=object(),
        planner=object(),
        operations=operations,
        policy=policy,
        destructive_enabled=True,
    )
    result = backend.invoke(
        "apply",
        {"confirm_prefix": PREFIX, "plan_sha256": "a" * 64},
        "550e8400-e29b-41d4-a716-446655440000",
    )
    assert result["state"] == "partial"
    assert result["primary_category"] == "postflight_not_empty"


def test_runtime_reports_evidence_write_failure_after_completed_delete_as_partial():
    operations = types.SimpleNamespace(
        apply=lambda _request: (_ for _ in ()).throw(OperationFailure("control_create_failed", partial=True)),
        status=lambda operation_id: types.SimpleNamespace(operation_id=operation_id, state="completed", body=b"{}"),
    )
    policy = types.SimpleNamespace(
        entries={"go-live-streaming-test-v1": types.SimpleNamespace(durability="disposable_acceptance")},
        match_prefix=lambda _prefix: types.SimpleNamespace(checkpoint_id="go-live-streaming-test-v1"),
    )
    backend = service.RuntimeBackend(
        gateway=types.SimpleNamespace(probe_capabilities=lambda: {}),
        leases=object(),
        planner=object(),
        operations=operations,
        policy=policy,
        destructive_enabled=True,
    )

    with pytest.raises(service.ServiceFailure, match="control_create_failed") as caught:
        backend.invoke(
            "apply",
            {"confirm_prefix": PREFIX, "plan_sha256": "a" * 64},
            "550e8400-e29b-41d4-a716-446655440000",
        )
    assert caught.value.state == "partial"
    assert caught.value.status == 409


def test_bulk_plan_records_plan_inventory_and_refusal_metrics():
    artifact = types.SimpleNamespace(
        summary={
            "checkpoint_id": "go-live-streaming-test-v1",
            "decision": "refused",
            "inventory": {"object_count": 3, "total_bytes": 21},
            "policy_sha256": "a" * 64,
            "refusal_codes": ("generation_identity_mismatch",),
        }
    )
    entry = types.SimpleNamespace(prefix=PREFIX, durability="disposable_acceptance")
    policy = types.SimpleNamespace(entries={"go-live-streaming-test-v1": entry})
    backend = service.RuntimeBackend(
        gateway=types.SimpleNamespace(probe_capabilities=lambda: {}),
        leases=object(),
        planner=types.SimpleNamespace(plan=lambda _request: artifact),
        operations=object(),
        policy=policy,
        destructive_enabled=False,
    )
    monkeypatch_digest = "a" * 64
    original = service._policy_digest
    service._policy_digest = lambda _policy: monkeypatch_digest
    try:
        backend.invoke("plan", {"actor": "operator", "checkpoint_ids": ["go-live-streaming-test-v1"]}, None)
    finally:
        service._policy_digest = original
    metrics = backend.metrics()
    assert b'checkpoint_retention_plans_total{decision="refused"} 1' in metrics
    assert b'checkpoint_retention_objects{checkpoint_id="go-live-streaming-test-v1"} 3' in metrics
    assert b"generation_identity_mismatch" in metrics


def test_http_boundary_records_auth_and_invalid_request_metrics_without_exposing_payload():
    class RecordingBackend(Backend):
        def __init__(self):
            super().__init__()
            self.failures = []

        def record_request_failure(self, outcome):
            self.failures.append(outcome)

    backend = RecordingBackend()
    app = service.RetentionApplication(backend, lease_token="lease-only-token", operator_token="manual-operator-token")
    with pytest.raises(service.ServiceFailure, match="unauthorized"):
        app.dispatch("POST", "/v1/plans", _headers(b"{}", "wrong-token"), b"{}")
    duplicate = b'{"actor":"operator","actor":"payload-secret"}'
    with pytest.raises(service.ServiceFailure, match="json_duplicate_key"):
        app.dispatch("POST", "/v1/plans", _headers(duplicate, "manual-operator-token"), duplicate)

    assert backend.failures == ["unauthorized", "invalid_request"]


def test_unexpected_backend_failure_is_http_500_and_cli_exit_5(monkeypatch):
    class Broken(Backend):
        def invoke(self, *_args, **_kwargs):
            raise RuntimeError("secret=must-not-escape")

    app = service.RetentionApplication(Broken(), lease_token="lease", operator_token="operator")
    with pytest.raises(service.ServiceFailure, match="backend_failure") as failure:
        app.dispatch("POST", "/v1/plans", _headers(b"{}", "operator"), b"{}")
    assert failure.value.status == 500

    class ErrorResponse:
        code = 500

        def read(self, _size):
            return b'{"code":"backend_failure"}'

        def close(self):
            return None

    monkeypatch.setattr(
        retention_cli,
        "_open",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            __import__("urllib.error").error.HTTPError("x", 500, "", {}, ErrorResponse())
        ),
    )
    with pytest.raises(retention_cli.CliFailure) as cli:
        retention_cli._request("/v1/plans", {"actor": "operator"}, "token")
    assert cli.value.exit_code == 5


def test_runbook_uses_actor_only_facts_and_locked_runtime_install():
    runbook = (Path(__file__).resolve().parents[2] / "docs/checkpoint-retention.md").read_text(encoding="utf-8")
    dockerfile = (Path(__file__).resolve().parents[2] / "checkpoints/retention.Dockerfile").read_text(encoding="utf-8")
    manual = runbook.split("### Manual command sequence", 1)[1].split("###", 1)[0]
    assert "evaluated_at" not in manual
    assert "only `actor`" in manual
    assert "uv export --frozen" in dockerfile
    assert "--require-hashes" in dockerfile
