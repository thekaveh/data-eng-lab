from __future__ import annotations

import http.client
import json
import math
import subprocess
import sys
import threading
from collections.abc import Iterator
from contextlib import contextmanager

import pytest

from scripts.observability import iceberg_rest_probe
from scripts.observability.iceberg_rest_probe import (
    ProbeConfig,
    ProbeFailure,
    ProbeResult,
    build_server,
    render_metrics,
)

METRIC_PREFIX = "data_eng_lab_iceberg_rest_synthetic_probe_"
RESULTS = ("success", "slow", "malformed", "timeout", "http_error", "unavailable")


@pytest.mark.parametrize(("result", "success"), [(name, name in {"success", "slow"}) for name in RESULTS])
def test_metrics_are_closed_bounded_and_one_hot(result: str, success: bool) -> None:
    body = render_metrics(ProbeResult(success, 0.125, 200 if success else 0, result))
    text = body.decode("ascii")
    assert len(body) < 8_192
    assert "iceberg-rest:8181" not in text
    for metric_type in ("success", "duration_seconds", "http_status_code", "result"):
        name = f"{METRIC_PREFIX}{metric_type}"
        assert f"# HELP {name} " in text
        assert f"# TYPE {name} gauge\n" in text
    assert f'{METRIC_PREFIX}success{{target="catalog"}} {1 if success else 0}\n' in text
    assert f'{METRIC_PREFIX}duration_seconds{{target="catalog"}} 0.125\n' in text
    assert f'{METRIC_PREFIX}http_status_code{{target="catalog"}} {200 if success else 0}\n' in text
    for category in RESULTS:
        expected = 1 if category == result else 0
        assert f'{METRIC_PREFIX}result{{target="catalog",result="{category}"}} {expected}\n' in text


@pytest.mark.parametrize(
    "value",
    [
        ProbeResult(True, math.nan, 200, "success"),
        ProbeResult(True, -1.0, 200, "success"),
        ProbeResult(True, 0.1, 700, "success"),
        ProbeResult(True, 0.1, 200, "timeout"),
        ProbeResult(False, 0.1, 0, "unknown"),
    ],
)
def test_metrics_reject_impossible_results(value: ProbeResult) -> None:
    with pytest.raises(ProbeFailure, match="^probe_result_invalid$"):
        render_metrics(value)


def test_module_import_does_not_create_a_socket() -> None:
    script = (
        "import urllib.request\n"
        "from http.server import BaseHTTPRequestHandler, HTTPServer\n"
        "import socket\n"
        "def forbidden(*args, **kwargs): raise AssertionError('socket created')\n"
        "socket.socket = forbidden\n"
        "import scripts.observability.iceberg_rest_probe\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, result.stderr


@contextmanager
def _running_server(monkeypatch: pytest.MonkeyPatch, result: ProbeResult) -> Iterator[tuple[str, int]]:
    monkeypatch.setattr(iceberg_rest_probe, "probe_catalog", lambda _config: result)
    server = build_server(
        ProbeConfig("http://iceberg-rest:8181", 2.0, 65_536),
        host="127.0.0.1",
        port=0,
    )
    assert server.__class__.__name__ == "HTTPServer"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_address
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=1)


def _request(address: tuple[str, int], method: str, path: str) -> tuple[int, dict[str, str], bytes]:
    connection = http.client.HTTPConnection(*address, timeout=2)
    try:
        connection.request(method, path)
        response = connection.getresponse()
        body = response.read(8_193)
        headers = {name.lower(): value for name, value in response.getheaders()}
        return response.status, headers, body
    finally:
        connection.close()


def test_health_and_metrics_routes_are_exact(monkeypatch: pytest.MonkeyPatch) -> None:
    result = ProbeResult(True, 0.25, 200, "success")
    with _running_server(monkeypatch, result) as address:
        status, headers, body = _request(address, "GET", "/healthz")
        assert status == 200
        assert headers["content-type"] == "application/json"
        assert int(headers["content-length"]) == len(body)
        assert json.loads(body) == {"status": "ready", "target": "catalog"}

        status, headers, body = _request(address, "GET", "/metrics")
        assert status == 200
        assert headers["content-type"] == "text/plain; version=0.0.4; charset=utf-8"
        assert int(headers["content-length"]) == len(body)
        assert body == render_metrics(result)


@pytest.mark.parametrize(
    ("method", "path", "expected"),
    [
        ("GET", "/metrics?target=other", 404),
        ("GET", "/unknown", 404),
        ("POST", "/metrics", 405),
        ("HEAD", "/healthz", 405),
    ],
)
def test_service_rejects_unknown_queries_paths_and_methods(
    monkeypatch: pytest.MonkeyPatch, method: str, path: str, expected: int
) -> None:
    with _running_server(monkeypatch, ProbeResult(True, 0.25, 200, "success")) as address:
        status, headers, body = _request(address, method, path)
    assert status == expected
    assert headers["content-type"] == "application/json"
    if method == "HEAD":
        assert int(headers["content-length"]) > 0
        assert body == b""
    else:
        assert int(headers["content-length"]) == len(body)
        assert json.loads(body) == {"code": "method_not_allowed" if expected == 405 else "not_found"}
