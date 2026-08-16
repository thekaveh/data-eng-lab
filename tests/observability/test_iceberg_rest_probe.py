from __future__ import annotations

import json
import socket
import threading
import time
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from types import TracebackType
from typing import Self

import pytest

from scripts.observability.iceberg_rest_probe import (
    ProbeConfig,
    ProbeFailure,
    ProbeResult,
    decode_catalog_config,
    probe_catalog,
)


def test_catalog_config_accepts_a_bounded_object() -> None:
    assert decode_catalog_config(
        b'{"defaults":{"warehouse":"s3://warehouse"},"overrides":{},"endpoints":["GET /v1/config"],"extension":true}'
    ) == {
        "defaults": {"warehouse": "s3://warehouse"},
        "overrides": {},
        "endpoints": ["GET /v1/config"],
        "extension": True,
    }


@pytest.mark.parametrize(
    "body",
    [
        b'{"defaults":{},"defaults":{}}',
        b"[]",
        b"\xff",
        b'{"value":NaN}',
        b'{"value":Infinity}',
        b'{"defaults":[]}',
        b'{"overrides":{"warehouse":1}}',
        b'{"endpoints":["GET /v1/config",1]}',
    ],
)
def test_catalog_config_rejects_malformed_or_ambiguous_json(body: bytes) -> None:
    with pytest.raises(ProbeFailure, match="^catalog_response_malformed$"):
        decode_catalog_config(body)


def test_catalog_config_rejects_a_body_above_the_exact_bound() -> None:
    body = json.dumps({"padding": "x" * 65_530}, separators=(",", ":")).encode()
    assert len(body) > 65_536
    with pytest.raises(ProbeFailure, match="^catalog_response_too_large$"):
        decode_catalog_config(body)


def test_catalog_config_accepts_depth_16_and_rejects_depth_17() -> None:
    def nested(depth: int) -> bytes:
        value: object = "leaf"
        for _ in range(depth - 1):
            value = [value]
        return json.dumps({"value": value}, separators=(",", ":")).encode()

    assert decode_catalog_config(nested(16))
    with pytest.raises(ProbeFailure, match="^catalog_response_malformed$"):
        decode_catalog_config(nested(17))


def test_catalog_config_rejects_more_than_4096_composed_nodes() -> None:
    body = json.dumps({"values": [None] * 4_096}, separators=(",", ":")).encode()
    with pytest.raises(ProbeFailure, match="^catalog_response_malformed$"):
        decode_catalog_config(body)


class _Handler(BaseHTTPRequestHandler):
    status = 200
    content_type = "application/json"
    body = b'{"defaults":{},"overrides":{}}'
    delay = 0.0
    location: str | None = None

    def do_GET(self) -> None:  # noqa: N802
        if self.delay:
            time.sleep(self.delay)
        self.send_response(self.status)
        self.send_header("Content-Type", self.content_type)
        if self.location is not None:
            self.send_header("Location", self.location)
        self.send_header("Content-Length", str(len(self.body)))
        self.end_headers()
        try:
            self.wfile.write(self.body)
        except BrokenPipeError:
            pass

    def log_message(self, _format: str, *_args: object) -> None:
        return


@contextmanager
def _server(
    *,
    status: int = 200,
    content_type: str = "application/json",
    body: bytes = b'{"defaults":{},"overrides":{}}',
    delay: float = 0.0,
    location: str | None = None,
) -> Iterator[str]:
    handler = type(
        "ConfiguredHandler",
        (_Handler,),
        {
            "status": status,
            "content_type": content_type,
            "body": body,
            "delay": delay,
            "location": location,
        },
    )
    server = HTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=1)


def test_probe_reports_valid_and_slow_catalog_responses() -> None:
    with _server() as origin:
        result = probe_catalog(ProbeConfig(origin, 0.2, 65_536))
    assert result.success is True
    assert result.http_status_code == 200
    assert result.result == "success"
    assert result.duration_seconds >= 0

    with _server(delay=0.02) as origin:
        slow = probe_catalog(ProbeConfig(origin, 0.2, 65_536, slow_seconds=0.01))
    assert slow.success is True
    assert slow.result == "slow"


@pytest.mark.parametrize(
    ("server", "expected"),
    [
        ({"body": b"{"}, "malformed"),
        ({"content_type": "text/plain"}, "malformed"),
        ({"body": b"x" * 65_537}, "malformed"),
        ({"status": 500}, "http_error"),
    ],
)
def test_probe_returns_closed_failure_categories(server: dict[str, object], expected: str) -> None:
    with _server(**server) as origin:  # type: ignore[arg-type]
        result = probe_catalog(ProbeConfig(origin, 0.2, 65_536))
    assert result.success is False
    assert result.result == expected
    assert result.http_status_code == (500 if expected == "http_error" else 200)


def test_probe_rejects_redirects_without_following_them() -> None:
    with _server() as destination:
        with _server(status=302, location=f"{destination}/v1/config") as origin:
            result = probe_catalog(ProbeConfig(origin, 0.2, 65_536))
    assert result == ProbeResult(False, result.duration_seconds, 302, "http_error")


def test_probe_classifies_timeout_and_unavailable_without_details() -> None:
    with _server(delay=0.1) as origin:
        timed_out = probe_catalog(ProbeConfig(origin, 0.01, 65_536))
    assert timed_out.result == "timeout"
    assert timed_out.http_status_code == 0

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    unavailable = probe_catalog(ProbeConfig(f"http://127.0.0.1:{port}", 0.05, 65_536))
    assert unavailable == ProbeResult(False, unavailable.duration_seconds, 0, "unavailable")


def test_probe_builds_a_no_proxy_opener_and_uses_only_config_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    built_handlers: list[tuple[object, ...]] = []

    class Response:
        status = 200
        headers = {"Content-Type": "application/json"}
        closed = False

        def read(self, size: int) -> bytes:
            assert size == 65_537
            return b'{"defaults":{},"overrides":{}}'

        def close(self) -> None:
            self.closed = True

        def __enter__(self) -> Self:
            return self

        def __exit__(
            self,
            _type: type[BaseException] | None,
            _value: BaseException | None,
            _traceback: TracebackType | None,
        ) -> None:
            self.close()

    response = Response()

    class Opener:
        def open(self, request: urllib.request.Request, timeout: float) -> Response:
            assert request.full_url == "http://127.0.0.1:8181/v1/config"
            assert request.get_method() == "GET"
            assert timeout == 2.0
            return response

    def build_opener(*handlers: object) -> Opener:
        built_handlers.append(handlers)
        return Opener()

    monkeypatch.setattr(urllib.request, "build_opener", build_opener)
    result = probe_catalog(ProbeConfig("http://127.0.0.1:8181", 2.0, 65_536))
    assert result.success is True
    assert response.closed is True
    proxy_handlers = [handler for handler in built_handlers[0] if isinstance(handler, urllib.request.ProxyHandler)]
    assert len(proxy_handlers) == 1
    assert proxy_handlers[0].proxies == {}


@pytest.mark.parametrize(
    "origin",
    [
        "https://example.com/path",
        "http://user:pass@example.com",
        "http://example.com?target=other",
        "ftp://example.com",
    ],
)
def test_probe_rejects_non_origin_targets_before_io(origin: str) -> None:
    with pytest.raises(ProbeFailure, match="^probe_origin_invalid$"):
        probe_catalog(ProbeConfig(origin, 2.0, 65_536))


@pytest.mark.parametrize("error", [KeyboardInterrupt(), SystemExit(2)])
def test_probe_preserves_control_flow(error: BaseException) -> None:
    class Opener:
        def open(self, _request: object, timeout: float) -> None:
            assert timeout == 2.0
            raise error

    with pytest.raises(type(error)):
        probe_catalog(ProbeConfig("http://127.0.0.1:8181", 2.0, 65_536), opener=Opener())
