from __future__ import annotations

import importlib
import json
import traceback

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
