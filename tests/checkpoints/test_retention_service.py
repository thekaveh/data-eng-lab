from __future__ import annotations

import importlib
import json
import traceback
import types

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
    monkeypatch.setattr("socket.socket", lambda *_args, **_kwargs: pytest.fail("import opened socket"))
    module = importlib.reload(_service())
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
    monkeypatch.setenv("CHECKPOINT_RETENTION_API_TOKEN", "runtime-token")
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
    monkeypatch.delenv("CHECKPOINT_RETENTION_API_TOKEN", raising=False)
    monkeypatch.setattr(module, "build_runtime", lambda: pytest.fail("runtime must not build"))
    with pytest.raises(module.ServiceFailure, match="configuration_invalid"):
        module.main()

    monkeypatch.setenv("CHECKPOINT_RETENTION_API_TOKEN", "runtime-token")
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
        def acquire(self, request):
            calls.append(("acquire", request))
            return types.SimpleNamespace(epoch="550e8400-e29b-41d4-a716-446655440000", etag="a" * 32, body=b"{}")

        def heartbeat(self, request):
            calls.append(("heartbeat", request))
            return types.SimpleNamespace(epoch=request.epoch, etag="b" * 32, body=b"{}")

        def terminal(self, request):
            calls.append(("terminal", request))
            return types.SimpleNamespace(epoch=request.epoch, etag="c" * 32, body=b"{}")

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
