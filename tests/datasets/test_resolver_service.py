from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

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
    server = resolver_module.create_server(_Services(object(), object()), host="127.0.0.1", port=0)
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


def test_resolve_request_returns_exact_canonical_frozen_result(resolver_module, monkeypatch):
    monkeypatch.setattr(resolver_module, "resolve_active_dataset", lambda *_args: RESULT)
    body = resolver_module.resolve_request(
        {"dataset": "movielens", "expected_scale": "small"},
        _Services(object(), object()),
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
        resolver_module.resolve_request(document, _Services(object(), object()))


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


def test_resolution_failure_returns_no_partial_result_or_sensitive_details(resolver_module, monkeypatch, capsys):
    secret = "http://minio:9000/?token=top-secret"

    def fail(*_args):
        raise RuntimeError(secret)

    monkeypatch.setattr(resolver_module, "resolve_active_dataset", fail)
    server = resolver_module.create_server(_Services(object(), object()), host="127.0.0.1", port=0)
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
    assert "COPY lakehouse /workspace/lakehouse" in text
    assert "HEALTHCHECK" in text and "/healthz" in text
    assert "EXPOSE" not in text
    assert "COPY ." not in text
    assert "pip install" not in text
