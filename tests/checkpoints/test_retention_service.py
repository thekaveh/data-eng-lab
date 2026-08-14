from __future__ import annotations

import importlib
import json
import traceback
import types
from datetime import datetime, timezone

import pytest


def _service():
    return importlib.import_module("scripts.checkpoints.service")


class FakeBackend:
    def __init__(self):
        self.calls = []

    def health(self):
        return {"capability_profile": "manual-verified-readback", "ready": True}

    def metrics(self):
        return b"checkpoint_retention_plans_total 0\n"

    def invoke(self, action, payload, operation_id=None):
        self.calls.append((action, payload, operation_id))
        return {"action": action, "operation_id": operation_id, "state": "accepted"}


def _headers(body: bytes, token="api-token"):
    return (
        ("Authorization", f"Bearer {token}"),
        ("Content-Type", "application/json"),
        ("Content-Length", str(len(body))),
    )


def test_import_has_no_network_or_server_side_effect(monkeypatch):
    module = _service()
    monkeypatch.setattr("socket.socket", lambda *_args, **_kwargs: pytest.fail("import opened socket"))
    module = importlib.reload(module)
    assert callable(module.create_server)


def test_health_and_metrics_are_bounded_fixed_public_internal_routes():
    app = _service().RetentionApplication(FakeBackend(), token="api-token")

    health = app.dispatch("GET", "/healthz", (), b"")
    metrics = app.dispatch("GET", "/metrics", (), b"")

    assert health.status == 200
    assert json.loads(health.body) == {"capability_profile": "manual-verified-readback", "ready": True}
    assert health.content_type == "application/json"
    assert metrics.status == 200
    assert metrics.body == b"checkpoint_retention_plans_total 0\n"
    assert metrics.content_type.startswith("text/plain")


@pytest.mark.parametrize(
    ("method", "path", "action", "operation_id"),
    [
        ("POST", "/v1/leases/acquire", "lease_acquire", None),
        ("POST", "/v1/leases/heartbeat", "lease_heartbeat", None),
        ("POST", "/v1/leases/terminal", "lease_terminal", None),
        ("POST", "/v1/plans", "plan", None),
        ("POST", "/v1/operations/prepare", "prepare", None),
        (
            "POST",
            "/v1/operations/550e8400-e29b-41d4-a716-446655440000/apply",
            "apply",
            "550e8400-e29b-41d4-a716-446655440000",
        ),
        (
            "GET",
            "/v1/operations/550e8400-e29b-41d4-a716-446655440000",
            "status",
            "550e8400-e29b-41d4-a716-446655440000",
        ),
    ],
)
def test_exact_routes_dispatch_only_typed_canonical_json(method, path, action, operation_id):
    backend = FakeBackend()
    app = _service().RetentionApplication(backend, token="api-token")
    body = b"{}" if method == "POST" else b""
    headers = _headers(body) if method == "POST" else (("Authorization", "Bearer api-token"),)

    response = app.dispatch(method, path, headers, body)

    assert response.status == 200
    assert backend.calls == [(action, {} if method == "POST" else None, operation_id)]
    assert (
        response.body
        == json.dumps(
            {"action": action, "operation_id": operation_id, "state": "accepted"},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    )


@pytest.mark.parametrize(
    ("headers", "body", "path", "code"),
    [
        (
            (("Authorization", "Bearer wrong"),),
            b"",
            "/v1/operations/550e8400-e29b-41d4-a716-446655440000",
            "unauthorized",
        ),
        (
            (("Authorization", "Bearer api-token"), ("Authorization", "Bearer api-token")),
            b"",
            "/v1/operations/550e8400-e29b-41d4-a716-446655440000",
            "header_duplicate",
        ),
        ((("Authorization", "Bearer api-token"),), b"{}", "/v1/plans", "content_type_invalid"),
        (_headers(b"{}") + (("Transfer-Encoding", "chunked"),), b"{}", "/v1/plans", "transfer_encoding_forbidden"),
        (_headers(b"{}")[:-1] + (("Content-Length", "3"),), b"{}", "/v1/plans", "content_length_mismatch"),
        (_headers(b"{"), b"{", "/v1/plans", "json_invalid"),
        (_headers(b'{"a":1,"a":2}'), b'{"a":1,"a":2}', "/v1/plans", "json_duplicate_key"),
        (
            _headers((b'{"a":' * 40) + b"0" + (b"}" * 40)),
            (b'{"a":' * 40) + b"0" + (b"}" * 40),
            "/v1/plans",
            "json_structure_bound",
        ),
        (_headers(b"x" * 65_537), b"x" * 65_537, "/v1/plans", "body_too_large"),
        (_headers(b"{}"), b"{}", "/v1/plans?apply=true", "path_invalid"),
    ],
)
def test_request_boundary_rejects_auth_header_body_and_path_ambiguity(headers, body, path, code):
    app = _service().RetentionApplication(FakeBackend(), token="api-token")
    with pytest.raises(_service().ServiceFailure, match=code):
        app.dispatch("POST", path, headers, body)


def test_dependency_failure_chain_is_sanitized():
    class BrokenBackend(FakeBackend):
        def invoke(self, *_args, **_kwargs):
            raise RuntimeError("credential=super-secret endpoint=http://private.invalid")

    app = _service().RetentionApplication(BrokenBackend(), token="api-token")
    with pytest.raises(_service().ServiceFailure, match="backend_failure") as failure:
        app.dispatch("POST", "/v1/plans", _headers(b"{}"), b"{}")

    rendered = "".join(traceback.format_exception(failure.value))
    assert "super-secret" not in rendered
    assert "private.invalid" not in rendered
    assert failure.value.__cause__ is None


def test_main_builds_runtime_serves_forever_and_closes_server_and_runtime(monkeypatch):
    module = _service()
    events = []

    class Runtime(FakeBackend):
        def close(self):
            events.append("runtime.close")

    class Server:
        def serve_forever(self):
            events.append("serve")

        def server_close(self):
            events.append("server.close")

    runtime = Runtime()
    monkeypatch.setenv("CHECKPOINT_RETENTION_LEASE_TOKEN", "runtime-lease-token")
    monkeypatch.setenv("CHECKPOINT_RETENTION_OPERATOR_TOKEN", "runtime-operator-token")
    monkeypatch.setattr(module, "build_runtime", lambda: runtime)

    def server_factory(address, application):
        assert address == ("0.0.0.0", 8080)
        assert isinstance(application, module.RetentionApplication)
        return Server()

    monkeypatch.setattr(module, "create_server", server_factory)

    assert module.main() == 0
    assert events == ["serve", "server.close", "runtime.close"]


def test_main_fails_closed_before_server_for_missing_token_and_sanitizes_build_failure(monkeypatch):
    module = _service()
    monkeypatch.delenv("CHECKPOINT_RETENTION_LEASE_TOKEN", raising=False)
    monkeypatch.delenv("CHECKPOINT_RETENTION_OPERATOR_TOKEN", raising=False)
    monkeypatch.setattr(module, "build_runtime", lambda: pytest.fail("runtime must not build"))
    with pytest.raises(module.ServiceFailure, match="configuration_invalid"):
        module.main()

    monkeypatch.setenv("CHECKPOINT_RETENTION_LEASE_TOKEN", "runtime-lease-token")
    monkeypatch.setenv("CHECKPOINT_RETENTION_OPERATOR_TOKEN", "runtime-operator-token")
    monkeypatch.setattr(
        module,
        "build_runtime",
        lambda: (_ for _ in ()).throw(RuntimeError("credential=must-not-escape")),
    )
    with pytest.raises(module.ServiceFailure, match="runtime_initialization_failed") as failure:
        module.main()
    rendered = "".join(traceback.format_exception(failure.value))
    assert "must-not-escape" not in rendered
    assert failure.value.__cause__ is None


def test_runtime_backend_maps_exact_lease_requests_without_leaking_dependency_objects():
    module = _service()
    calls = []

    class Leases:
        @staticmethod
        def _body():
            return b'{"heartbeat_at":"2026-08-13T12:00:00Z"}'

        def acquire(self, request):
            calls.append(("acquire", request))
            return types.SimpleNamespace(
                epoch="550e8400-e29b-41d4-a716-446655440000",
                etag="a" * 32,
                body=self._body(),
            )

        def heartbeat(self, request):
            calls.append(("heartbeat", request))
            return types.SimpleNamespace(epoch=request.epoch, etag="b" * 32, body=self._body())

        def terminal(self, request):
            calls.append(("terminal", request))
            return types.SimpleNamespace(epoch=request.epoch, etag="c" * 32, body=self._body())

    backend = module.RuntimeBackend(
        gateway=types.SimpleNamespace(probe_capabilities=lambda: {"automatic_apply": False}),
        leases=Leases(),
        planner=object(),
        operations=object(),
        policy=types.SimpleNamespace(entries={}),
        destructive_enabled=False,
    )
    acquired = backend.invoke(
        "lease_acquire",
        {
            "checkpoint_id": "streaming-events-v1",
            "owner_id": "jupyter-notebook",
            "prefix": "events/",
            "session_id": "550e8400-e29b-41d4-a716-446655440001",
            "workload": "streaming_ingest-events-spark-iceberg",
        },
        None,
    )
    assert acquired == {
        "epoch": "550e8400-e29b-41d4-a716-446655440000",
        "etag": "a" * 32,
        "state": "accepted",
    }
    assert calls[0][0] == "acquire"


def test_runtime_backend_refuses_destructive_route_while_disabled():
    module = _service()
    backend = module.RuntimeBackend(
        gateway=types.SimpleNamespace(probe_capabilities=lambda: {"automatic_apply": False}),
        leases=object(),
        planner=object(),
        operations=object(),
        policy=types.SimpleNamespace(entries={}),
        destructive_enabled=False,
    )
    assert backend.invoke(
        "apply",
        {"confirm_prefix": "streaming_test/550e8400-e29b-41d4-a716-446655440000/", "plan_sha256": "a" * 64},
        "550e8400-e29b-41d4-a716-446655440000",
    ) == {"state": "refused", "refusal_codes": ["destructive_disabled"]}


def test_runtime_backend_routes_one_exact_plan_and_prepare_with_server_owned_operation_id(monkeypatch):
    module = _service()
    calls = []
    artifact = types.SimpleNamespace(
        body=b'{"schema_version":1,"shards":[],"summary":{"decision":"eligible"}}',
        sha256="a" * 64,
    )

    class Planner:
        def plan(self, request):
            calls.append(("plan", request))
            return artifact

    class Operations:
        def prepare(self, request):
            calls.append(("prepare", request))
            return types.SimpleNamespace(
                body=b'{"operation_id":"550e8400-e29b-41d4-a716-446655440000","state":"prepared"}'
            )

    decoded = types.SimpleNamespace(summary={"prefix": "streaming_test/550e8400-e29b-41d4-a716-446655440000/"})
    monkeypatch.setattr(module, "decode_plan_artifact", lambda body, **_bounds: decoded, raising=False)
    backend = module.RuntimeBackend(
        gateway=types.SimpleNamespace(probe_capabilities=lambda: {"automatic_apply": False}),
        leases=object(),
        planner=Planner(),
        operations=Operations(),
        policy=types.SimpleNamespace(entries={}),
        destructive_enabled=False,
        now=lambda: datetime(2026, 8, 13, 12, 0, tzinfo=timezone.utc),
        operation_id=lambda: "550e8400-e29b-41d4-a716-446655440000",
    )
    backend._require_disposable = lambda _prefix: None

    planned = backend.invoke(
        "plan",
        {
            "actor": "acceptance-engineering",
            "checkpoint_id": "go-live-streaming-test-v1",
            "prefix": "streaming_test/550e8400-e29b-41d4-a716-446655440000/",
        },
        None,
    )
    prepared = backend.invoke(
        "prepare",
        {
            "actor": "acceptance-engineering",
            "plan": json.loads(artifact.body),
            "plan_sha256": "a" * 64,
            "review": "review-86",
        },
        None,
    )

    assert planned == json.loads(artifact.body)
    assert prepared == {
        "operation_id": "550e8400-e29b-41d4-a716-446655440000",
        "state": "prepared",
    }
    assert calls[0][0] == "plan"
    assert calls[0][1].evaluated_at == datetime(2026, 8, 13, 12, 0, tzinfo=timezone.utc)
    assert calls[1][0] == "prepare"
    assert calls[1][1].artifact is decoded
    assert calls[1][1].operation_id == "550e8400-e29b-41d4-a716-446655440000"

    with pytest.raises(module.ServiceFailure, match="request_invalid"):
        backend.invoke(
            "plan",
            {
                "actor": "acceptance-engineering",
                "checkpoint_id": "go-live-streaming-test-v1",
                "evaluated_at": "2099-01-01T00:00:00Z",
                "prefix": "streaming_test/550e8400-e29b-41d4-a716-446655440000/",
            },
            None,
        )


def test_runtime_backend_bulk_plan_returns_registry_ordered_bounded_refusals(monkeypatch):
    module = _service()
    entries = {
        "static": types.SimpleNamespace(checkpoint_id="static", prefix="events/"),
        "dynamic": types.SimpleNamespace(checkpoint_id="dynamic", prefix="streaming_test/{run_uuid}/"),
    }
    monkeypatch.setattr(module, "_policy_digest", lambda _policy: "d" * 64)

    class Planner:
        def plan(self, request):
            if request.checkpoint_id == "static":
                raise module.PlanFailure("inventory_empty")
            raise AssertionError("dynamic template must not be inventoried")

    backend = module.RuntimeBackend(
        gateway=types.SimpleNamespace(probe_capabilities=lambda: {"automatic_apply": False}),
        leases=object(),
        planner=Planner(),
        operations=object(),
        policy=types.SimpleNamespace(entries=entries),
        destructive_enabled=False,
        now=lambda: datetime(2026, 8, 13, 12, 0, tzinfo=timezone.utc),
    )

    result = backend.invoke(
        "plan",
        {"actor": "airflow-dry-run", "checkpoint_ids": ["static", "dynamic"]},
        None,
    )

    assert result == {
        "plans": [
            {
                "checkpoint_id": "static",
                "decision": "refused",
                "inventory": {"object_count": 0, "total_bytes": 0},
                "policy_sha256": "d" * 64,
                "refusal_codes": ["inventory_empty"],
            },
            {
                "checkpoint_id": "dynamic",
                "decision": "refused",
                "inventory": {"object_count": 0, "total_bytes": 0},
                "policy_sha256": "d" * 64,
                "refusal_codes": ["concrete_prefix_required"],
            },
        ],
        "state": "accepted",
    }


def test_build_runtime_wires_live_revalidation_and_exact_runtime_clock(monkeypatch):
    module = _service()
    from pathlib import Path

    from scripts.checkpoints.policy import load_policy

    policy = load_policy(Path(__file__).resolve().parents[2] / "checkpoints" / "retention-policy.yaml")
    client = types.SimpleNamespace(close=lambda: None)
    monkeypatch.setenv("MINIO_RETENTION_ACCESS_KEY", "retention-user")
    monkeypatch.setenv("MINIO_RETENTION_SECRET_KEY", "retention-secret")
    monkeypatch.setattr(module, "load_policy", lambda _path: policy)
    monkeypatch.setattr(module, "build_s3_client", lambda *_args: client)
    startup_health = []
    monkeypatch.setattr(
        module.RuntimeBackend,
        "health",
        lambda _self: startup_health.append("observed") or {"ready": True},
    )

    backend = module.build_runtime()

    assert backend._now is module._now
    assert callable(backend._operations._revalidate)
    assert startup_health == ["observed"]


def test_runtime_metrics_track_fixed_low_cardinality_plan_prepare_apply_outcomes(monkeypatch):
    module = _service()
    artifact = types.SimpleNamespace(
        body=b'{"schema_version":1,"shards":[],"summary":{"decision":"eligible"}}',
        sha256="a" * 64,
    )

    class Planner:
        def plan(self, _request):
            return artifact

    class Operations:
        def prepare(self, _request):
            return types.SimpleNamespace(
                body=b'{"operation_id":"550e8400-e29b-41d4-a716-446655440000","state":"prepared"}'
            )

        def apply(self, _request):
            return types.SimpleNamespace(
                body=b'{"deleted_objects":2,"operation_id":"550e8400-e29b-41d4-a716-446655440000","state":"completed"}'
            )

    monkeypatch.setattr(
        module,
        "decode_plan_artifact",
        lambda *_args, **_kwargs: types.SimpleNamespace(
            summary={"prefix": "streaming_test/550e8400-e29b-41d4-a716-446655440000/"}
        ),
    )
    backend = module.RuntimeBackend(
        gateway=types.SimpleNamespace(probe_capabilities=lambda: {"automatic_apply": False}),
        leases=object(),
        planner=Planner(),
        operations=Operations(),
        policy=types.SimpleNamespace(entries={}, bounds=types.SimpleNamespace(max_manifest_shard_bytes=1_048_576)),
        destructive_enabled=True,
        operation_id=lambda: "550e8400-e29b-41d4-a716-446655440000",
    )
    backend._require_disposable = lambda _prefix: None
    backend.invoke(
        "plan",
        {
            "actor": "acceptance-engineering",
            "checkpoint_id": "go-live-streaming-test-v1",
            "prefix": "streaming_test/550e8400-e29b-41d4-a716-446655440000/",
        },
        None,
    )
    backend.invoke(
        "prepare",
        {"actor": "acceptance-engineering", "plan": {}, "plan_sha256": "a" * 64, "review": "review-86"},
        None,
    )
    backend.invoke(
        "apply",
        {
            "confirm_prefix": "streaming_test/550e8400-e29b-41d4-a716-446655440000/",
            "plan_sha256": "a" * 64,
        },
        "550e8400-e29b-41d4-a716-446655440000",
    )
    backend.invoke(
        "apply",
        {
            "confirm_prefix": "streaming_test/550e8400-e29b-41d4-a716-446655440000/",
            "plan_sha256": "a" * 64,
        },
        "550e8400-e29b-41d4-a716-446655440000",
    )

    metrics = backend.metrics()
    assert b'checkpoint_retention_plans_total{decision="eligible"} 1\n' in metrics
    assert b'checkpoint_retention_prepared_total{outcome="completed"} 1\n' in metrics
    assert b'checkpoint_retention_deleted_objects_total{outcome="completed"} 2\n' in metrics
    assert b'checkpoint_retention_deleted_objects_total{outcome="completed"} 4\n' not in metrics


def test_runtime_backend_current_refusal_outranks_stale_partial_status():
    module = _service()

    class Operations:
        def apply(self, _request):
            raise module.OperationFailure("confirmation_mismatch")

        def status(self, _operation_id):
            return types.SimpleNamespace(
                state="partial",
                body=b'{"operation_id":"550e8400-e29b-41d4-a716-446655440000","state":"partial"}',
            )

    backend = module.RuntimeBackend(
        gateway=types.SimpleNamespace(probe_capabilities=lambda: {"automatic_apply": False}),
        leases=object(),
        planner=object(),
        operations=Operations(),
        policy=types.SimpleNamespace(entries={}),
        destructive_enabled=True,
    )
    backend._require_disposable = lambda _prefix: None

    with pytest.raises(module.ServiceFailure, match="confirmation_mismatch") as failure:
        backend.invoke(
            "apply",
            {
                "confirm_prefix": "streaming_test/550e8400-e29b-41d4-a716-446655440000/",
                "plan_sha256": "a" * 64,
            },
            "550e8400-e29b-41d4-a716-446655440000",
        )

    assert failure.value.state == "refused"


def test_runtime_prepare_preserves_typed_operation_refusal_taxonomy(monkeypatch):
    module = _service()

    class Operations:
        def prepare(self, _request):
            raise module.OperationFailure("policy_drift")

    monkeypatch.setattr(
        module,
        "decode_plan_artifact",
        lambda *_args, **_kwargs: types.SimpleNamespace(
            summary={"prefix": "streaming_test/550e8400-e29b-41d4-a716-446655440000/"}
        ),
    )
    backend = module.RuntimeBackend(
        gateway=types.SimpleNamespace(probe_capabilities=lambda: {"automatic_apply": False}),
        leases=object(),
        planner=object(),
        operations=Operations(),
        policy=types.SimpleNamespace(entries={}),
        destructive_enabled=False,
    )
    backend._require_disposable = lambda _prefix: None

    with pytest.raises(module.ServiceFailure, match="policy_drift") as failure:
        backend.invoke(
            "prepare",
            {"actor": "operator", "plan": {}, "plan_sha256": "a" * 64, "review": "review-86"},
            None,
        )

    assert failure.value.status == 409
    assert failure.value.state == "refused"
