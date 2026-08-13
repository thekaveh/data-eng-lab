from __future__ import annotations

import json
import socket
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import ANY

import pytest

from datasets.locking import canonical_json
from datasets.publication import ResolvedDataset, ResolvedObject


@dataclass
class _Services:
    client: object
    registry: object


RESULT = ResolvedDataset(
    dataset="movielens",
    scale="small",
    plan_id="1" * 64,
    manifest_sha256="2" * 64,
    publication_id="0123456789ab4def8123456789abcdef",
    objects=(
        ResolvedObject("ratings.csv", "s3://landing/generation/ratings.csv", 3, "3" * 64, "ratings-v1"),
        ResolvedObject("movies.csv", "s3://landing/generation/movies.csv", 4, "4" * 64, "movies-v1"),
    ),
)


@pytest.fixture
def resolver_module():
    import datasets.resolver_service as module

    return module


@pytest.fixture
def service(resolver_module, monkeypatch):
    calls: list[tuple[object, object, str, str]] = []

    def fake_resolver(client, registry, dataset, scale):
        calls.append((client, registry, dataset, scale))
        return RESULT

    monkeypatch.setattr(resolver_module, "resolve_active_dataset", fake_resolver)
    server = resolver_module.create_server(_Services(object(), {"movielens": object()}), host="127.0.0.1", port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", calls
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _request(base: str, path: str, *, method: str = "GET", body: bytes | None = None, content_type=None):
    headers = {} if content_type is None else {"Content-Type": content_type}
    request = urllib.request.Request(base + path, data=body, headers=headers, method=method)
    try:
        response = urllib.request.urlopen(request, timeout=2)
    except urllib.error.HTTPError as error:
        return error.code, error.headers, error.read()
    with response:
        return response.status, response.headers, response.read()


def _raw_request(port: int, request: bytes) -> tuple[int, bytes]:
    with socket.create_connection(("127.0.0.1", port), timeout=2) as connection:
        connection.sendall(request)
        connection.shutdown(socket.SHUT_WR)
        response = b""
        while chunk := connection.recv(4096):
            response += chunk
    status = int(response.split(b" ", 2)[1])
    return status, response.partition(b"\r\n\r\n")[2]


def _raw_response(port: int, request: bytes) -> bytes:
    with socket.create_connection(("127.0.0.1", port), timeout=2) as connection:
        connection.sendall(request)
        connection.shutdown(socket.SHUT_WR)
        response = b""
        while chunk := connection.recv(4096):
            response += chunk
    return response


def test_resolve_request_returns_exact_canonical_frozen_result(resolver_module, monkeypatch):
    monkeypatch.setattr(resolver_module, "resolve_active_dataset", lambda *_args: RESULT)
    body = resolver_module.resolve_request(
        {"dataset": "movielens", "expected_scale": "small"},
        _Services(object(), {"movielens": object()}),
    )
    assert body == canonical_json(
        {
            "dataset": "movielens",
            "manifest_sha256": "2" * 64,
            "objects": [
                {
                    "object_name": "ratings.csv",
                    "schema_id": "ratings-v1",
                    "sha256": "3" * 64,
                    "size_bytes": 3,
                    "uri": "s3://landing/generation/ratings.csv",
                },
                {
                    "object_name": "movies.csv",
                    "schema_id": "movies-v1",
                    "sha256": "4" * 64,
                    "size_bytes": 4,
                    "uri": "s3://landing/generation/movies.csv",
                },
            ],
            "plan_id": "1" * 64,
            "publication_id": "0123456789ab4def8123456789abcdef",
            "scale": "small",
        }
    )
    assert not body.endswith(b"\n")


@pytest.mark.parametrize(
    ("document", "message"),
    [
        ({"dataset": "movielens"}, "expected_scale is required"),
        ({"expected_scale": "small"}, "dataset is required"),
        ({"dataset": "movielens", "expected_scale": "small", "extra": True}, "request fields are not exact"),
        ({"dataset": 1, "expected_scale": "small"}, "dataset must be a valid identifier"),
        ({"dataset": "../secret", "expected_scale": "small"}, "dataset must be a valid identifier"),
        ({"dataset": "movielens", "expected_scale": 1}, "expected_scale must be one of: tiny, small, medium"),
        ({"dataset": "movielens", "expected_scale": "large"}, "expected_scale must be one of: tiny, small, medium"),
    ],
)
def test_resolve_request_rejects_invalid_exact_fields(resolver_module, document, message):
    with pytest.raises(resolver_module.RequestError, match=f"^{message}$"):
        resolver_module.resolve_request(document, _Services(object(), {"movielens": object()}))


@pytest.mark.parametrize("dataset", ["nyc_taxi", "gh_archive", "movielens", "online_retail", "tpch"])
def test_every_production_dataset_identifier_is_accepted(resolver_module, monkeypatch, dataset):
    registry = {name: object() for name in ("nyc_taxi", "gh_archive", "movielens", "online_retail", "tpch")}
    monkeypatch.setattr(resolver_module, "resolve_active_dataset", lambda *_args: RESULT)
    assert resolver_module.resolve_request(
        {"dataset": dataset, "expected_scale": "small"}, _Services(object(), registry)
    )


def test_unknown_well_formed_dataset_fails_before_resolver_access(resolver_module, monkeypatch):
    monkeypatch.setattr(
        resolver_module,
        "resolve_active_dataset",
        lambda *_args: (_ for _ in ()).throw(AssertionError("resolver must not run")),
    )
    with pytest.raises(resolver_module.RequestError, match="^unknown dataset$"):
        resolver_module.resolve_request(
            {"dataset": "unknown_dataset", "expected_scale": "small"},
            _Services(object(), {"movielens": object()}),
        )


def test_http_resolve_success_has_exact_json_and_security_headers(service):
    base, calls = service
    status, headers, body = _request(
        base,
        "/v1/resolve",
        method="POST",
        body=b'{"dataset":"movielens","expected_scale":"small"}',
        content_type="application/json",
    )
    assert status == 200
    assert headers["Content-Type"] == "application/json"
    assert headers["Cache-Control"] == "no-store"
    assert headers["X-Content-Type-Options"] == "nosniff"
    assert body == canonical_json(json.loads(body))
    assert calls and calls[0][2:] == ("movielens", "small")


def test_resolve_endpoint_requires_expected_scale(service):
    base, calls = service
    status, _headers, body = _request(
        base,
        "/v1/resolve",
        method="POST",
        body=b'{"dataset":"movielens"}',
        content_type="application/json",
    )
    assert status == 400
    assert json.loads(body) == {"error": "expected_scale is required"}
    assert calls == []


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        (b"not-json", "request body must be valid JSON"),
        (b"[]", "request body must be a JSON mapping"),
        (b'{"dataset":"a","dataset":"b","expected_scale":"small"}', "request fields must be unique"),
        (b'{"dataset":"movielens","expected_scale":NaN}', "request body must be valid JSON"),
    ],
)
def test_resolve_endpoint_rejects_malformed_or_duplicate_json(service, body, expected):
    base, calls = service
    status, _headers, response_body = _request(
        base, "/v1/resolve", method="POST", body=body, content_type="application/json"
    )
    assert status == 400
    assert json.loads(response_body) == {"error": expected}
    assert calls == []


def test_resolve_endpoint_rejects_deep_or_trailing_json(service):
    base, calls = service
    for body in (
        b"[" * 1500 + b"]" * 1500,
        b'{"dataset":"movielens","expected_scale":"small"} trailing',
    ):
        status, _, response_body = _request(
            base, "/v1/resolve", method="POST", body=body, content_type="application/json"
        )
        assert status == 400
        assert response_body == b'{"error":"request body must be valid JSON"}'
    assert calls == []


@pytest.mark.parametrize(
    ("headers", "status", "message"),
    [
        (
            b"Content-Length: 2\r\nContent-Length: 2\r\nContent-Type: application/json",
            400,
            "content length must be unique",
        ),
        (
            b"Content-Length: 2\r\nContent-Type: application/json\r\nContent-Type: application/json",
            400,
            "content type must be unique",
        ),
        (b"Transfer-Encoding: chunked\r\nContent-Type: application/json", 400, "transfer encoding is not supported"),
        (
            b"Transfer-Encoding: chunked\r\nContent-Length: 2\r\nContent-Type: application/json",
            400,
            "transfer encoding is not supported",
        ),
        (b"Content-Length: -1\r\nContent-Type: application/json", 400, "content length is invalid"),
        (b"Content-Length: +2\r\nContent-Type: application/json", 400, "content length is invalid"),
        (b"Content-Length: 02\r\nContent-Type: application/json", 400, "content length is invalid"),
        (b"Content-Length: 999999999999999999999\r\nContent-Type: application/json", 413, "request body is too large"),
    ],
)
def test_raw_http_framing_is_strict(service, headers, status, message):
    base, calls = service
    port = int(base.rsplit(":", 1)[1])
    actual, body = _raw_request(
        port,
        b"POST /v1/resolve HTTP/1.1\r\nHost: resolver\r\n" + headers + b"\r\n\r\n{}",
    )
    assert (actual, body) == (status, canonical_json({"error": message}))
    assert calls == []


@pytest.mark.parametrize("method", ["PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "TRACE", "CONNECT"])
def test_every_unsupported_method_is_canonical_json(service, method):
    base, _calls = service
    port = int(base.rsplit(":", 1)[1])
    status, body = _raw_request(
        port, f"{method} /v1/resolve HTTP/1.1\r\nHost: resolver\r\nContent-Length: 0\r\n\r\n".encode()
    )
    assert status == 405
    if method == "HEAD":
        assert body == b""
    else:
        assert body == b'{"error":"method not allowed"}'


@pytest.mark.parametrize(
    ("path", "allow"),
    [("/v1/resolve", "POST"), ("/healthz", "GET"), ("/unknown", "GET, POST")],
)
def test_arbitrary_valid_method_is_canonical_405_with_route_allow(service, path, allow):
    base, _calls = service
    port = int(base.rsplit(":", 1)[1])
    response = _raw_response(port, f"FOO {path} HTTP/1.1\r\nHost: resolver\r\n\r\n".encode())
    head, body = response.split(b"\r\n\r\n", 1)
    assert head.startswith(b"HTTP/1.1 405 ")
    assert f"Allow: {allow}\r\n".encode() in head + b"\r\n"
    assert b"Content-Type: application/json" in head
    assert b"Connection: close" in head
    assert body == b'{"error":"method not allowed"}'


@pytest.mark.parametrize(
    ("raw_request", "status", "body"),
    [
        (b"GET /healthz HTTP/2.0\r\nHost: resolver\r\n\r\n", 505, b'{"error":"HTTP version is not supported"}'),
        (b"GET /healthz BOGUS\r\nHost: resolver\r\n\r\n", 400, b'{"error":"bad request"}'),
        (b"NOT A VALID REQUEST LINE\r\n\r\n", 400, b'{"error":"bad request"}'),
    ],
)
def test_parser_errors_are_complete_http11_canonical_json(service, raw_request, status, body):
    base, _calls = service
    response = _raw_response(int(base.rsplit(":", 1)[1]), raw_request)
    head, actual_body = response.split(b"\r\n\r\n", 1)
    assert head.startswith(f"HTTP/1.1 {status} ".encode())
    assert b"Content-Type: application/json" in head
    assert f"Content-Length: {len(body)}".encode() in head
    assert b"Connection: close" in head
    assert actual_body == body


def test_parser_failures_are_canonical_bounded_json(service):
    base, _calls = service
    port = int(base.rsplit(":", 1)[1])
    status, body = _raw_request(port, b"GET /" + b"x" * 70000 + b" HTTP/1.1\r\n\r\n")
    assert status == 414
    assert body == b'{"error":"request URI is too long"}'


def test_http_contract_bounds_type_method_and_paths(service):
    base, calls = service
    status, _, body = _request(base, "/v1/resolve", method="POST", body=b"{}", content_type="text/plain")
    assert (status, json.loads(body)) == (415, {"error": "content type must be application/json"})

    status, _, body = _request(
        base,
        "/v1/resolve",
        method="POST",
        body=b"{" + b" " * (16 * 1024),
        content_type="application/json",
    )
    assert (status, json.loads(body)) == (413, {"error": "request body is too large"})

    status, headers, body = _request(base, "/v1/resolve")
    assert status == 405 and headers["Allow"] == "POST" and json.loads(body) == {"error": "method not allowed"}
    status, _, body = _request(base, "/missing")
    assert status == 404 and json.loads(body) == {"error": "not found"}
    assert calls == []


def test_health_is_constant_and_never_resolves(service):
    base, calls = service
    status, headers, body = _request(base, "/healthz")
    assert status == 200
    assert headers["Cache-Control"] == "no-store"
    assert body == b'{"status":"ok"}'
    assert calls == []


def test_health_remains_responsive_while_resolution_is_busy(resolver_module, monkeypatch):
    entered = threading.Event()
    release = threading.Event()

    def slow(*_args):
        entered.set()
        assert release.wait(2)
        return RESULT

    monkeypatch.setattr(resolver_module, "resolve_active_dataset", slow)
    services = _Services(object(), {"movielens": object()})
    server = resolver_module.create_server(services, host="127.0.0.1", port=0, max_resolver_work=1, max_connections=4)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    request_args = dict(
        method="POST",
        body=b'{"dataset":"movielens","expected_scale":"small"}',
        content_type="application/json",
    )
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            busy = pool.submit(_request, base, "/v1/resolve", **request_args)
            assert entered.wait(1)
            started = time.monotonic()
            assert _request(base, "/healthz")[0] == 200
            assert time.monotonic() - started < 0.5
            assert _request(base, "/v1/resolve", **request_args) == (
                503,
                ANY,
                b'{"error":"resolver is busy"}',
            )
            release.set()
            assert busy.result(timeout=2)[0] == 200
    finally:
        release.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_partial_body_times_out_without_blocking_health(resolver_module):
    server = resolver_module.create_server(
        _Services(object(), {"movielens": object()}),
        host="127.0.0.1",
        port=0,
        request_timeout=0.15,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    connection = socket.create_connection(("127.0.0.1", server.server_port), timeout=1)
    try:
        connection.sendall(
            b"POST /v1/resolve HTTP/1.1\r\nHost: resolver\r\nContent-Type: application/json\r\n"
            b"Content-Length: 100\r\n\r\n{"
        )
        assert _request(f"http://127.0.0.1:{server.server_port}", "/healthz")[0] == 200
        time.sleep(0.25)
        response = connection.recv(4096)
        assert b" 408 " in response and response.endswith(b'{"error":"request body timed out"}')
    finally:
        connection.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_resolution_failure_returns_no_partial_result_or_sensitive_details(resolver_module, monkeypatch, capsys):
    secret = "http://minio:9000/?token=top-secret"

    def fail(*_args):
        raise RuntimeError(secret)

    monkeypatch.setattr(resolver_module, "resolve_active_dataset", fail)
    server = resolver_module.create_server(_Services(object(), {"movielens": object()}), host="127.0.0.1", port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        status, _headers, body = _request(
            f"http://127.0.0.1:{server.server_port}",
            "/v1/resolve",
            method="POST",
            body=b'{"dataset":"movielens","expected_scale":"small"}',
            content_type="application/json",
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
    assert status == 500
    assert body == b'{"error":"dataset resolution failed"}'
    assert b"objects" not in body and secret.encode() not in body
    captured = capsys.readouterr()
    assert secret not in captured.out + captured.err


def test_container_service_environment_uses_only_container_endpoint(resolver_module, monkeypatch, tmp_path):
    captured = {}
    monkeypatch.setenv("MINIO_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("MINIO_ROOT_USER", "generated-user")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "generated-password")
    monkeypatch.setattr(resolver_module.boto3, "client", lambda *args, **kwargs: captured.update(kwargs) or object())
    client = resolver_module.container_s3_client()
    assert client is not None
    assert captured["endpoint_url"] == "http://minio:9000"
    assert captured["aws_access_key_id"] == "generated-user"
    assert captured["aws_secret_access_key"] == "generated-password"
    assert captured["config"].retries["total_max_attempts"] == 1
    assert captured["config"].connect_timeout == 3
    assert captured["config"].read_timeout == 30


def test_server_close_quiesces_requests_before_closing_client(resolver_module):
    closed = []

    class Client:
        def close(self):
            closed.append("client")

    services = resolver_module.ResolverServices(Client(), {"movielens": object()})
    server = resolver_module.create_server(services, host="127.0.0.1", port=0)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert closed == ["client"]


def test_resolver_dockerfile_is_pinned_locked_internal_and_non_root():
    text = Path("datasets/resolver.Dockerfile").read_text(encoding="utf-8")
    assert "FROM --platform=linux/amd64 python@sha256:" in text
    assert "ghcr.io/astral-sh/uv@sha256:" in text
    assert "uv sync --frozen" in text
    assert "a376ce1b5bd5621290aaded68c22572690395419876da41814e28469bb4186b1" in text
    assert (
        'LABEL org.data-eng-lab.uv-lock-sha256="a376ce1b5bd5621290aaded68c22572690395419876da41814e28469bb4186b1"'
        in text
    )
    assert "USER 65532:65532" in text
    assert 'ENTRYPOINT ["/opt/venv/bin/python", "-m", "datasets.resolver_service"]' in text
    assert "COPY lakehouse/__init__.py lakehouse/atlas_endpoints.py /workspace/lakehouse/" in text
    assert "datasets/tpch_lock_export.py" not in text
    assert "find /opt/venv /workspace" in text and "*.pyc" in text
    assert "HEALTHCHECK" in text and "/healthz" in text
    assert "EXPOSE" not in text
    assert "COPY ." not in text
    assert "pip install" not in text


def test_dockerignore_is_deny_by_default_and_allows_only_build_inputs():
    lines = Path(".dockerignore").read_text(encoding="utf-8").splitlines()
    assert lines[0] == "**"
    assert set(lines[1:]) == {
        "!pyproject.toml",
        "!uv.lock",
        "!datasets/",
        "!datasets/__init__.py",
        "!datasets/acquisition.py",
        "!datasets/locking.py",
        "!datasets/publication.py",
        "!datasets/registry.py",
        "!datasets/registry.yaml",
        "!datasets/resolver_service.py",
        "!datasets/s3.py",
        "!datasets/schema.py",
        "!datasets/schema_inspection.py",
        "!datasets/tpch-lock-requirements.txt",
        "!datasets/tpch_lock_export.py",
        "!datasets/verification.py",
        "!lakehouse/",
        "!lakehouse/__init__.py",
            "!lakehouse/atlas_endpoints.py",
            "!scripts/",
            "!scripts/checkpoints/",
            "!scripts/checkpoints/**",
            "!checkpoints/",
            "!checkpoints/retention-policy.yaml",
        }
