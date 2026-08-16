"""Bounded synthetic availability probe for the fixed Atlas Iceberg REST catalog."""

from __future__ import annotations

import json
import math
import os
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, NoReturn

MAX_CATALOG_BODY_BYTES = 65_536
MAX_JSON_DEPTH = 16
MAX_JSON_NODES = 4_096
FIXED_CATALOG_ORIGIN = "http://iceberg-rest:8181"
RESULT_CATEGORIES = (
    "success",
    "slow",
    "malformed",
    "timeout",
    "http_error",
    "unavailable",
)
METRIC_PREFIX = "data_eng_lab_iceberg_rest_synthetic_probe_"


class ProbeFailure(Exception):
    """A closed, sanitized probe failure."""


@dataclass(frozen=True)
class ProbeConfig:
    """Immutable bounds for one fixed-origin catalog probe."""

    origin: str
    timeout_seconds: float
    max_body_bytes: int
    slow_seconds: float = 1.0


@dataclass(frozen=True)
class ProbeResult:
    """Closed, low-cardinality result of one catalog probe."""

    success: bool
    duration_seconds: float
    http_status_code: int
    result: str


def _malformed() -> NoReturn:
    raise ProbeFailure("catalog_response_malformed")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            _malformed()
        value[key] = item
    return value


def _reject_constant(_value: str) -> NoReturn:
    _malformed()


def _validate_bounds(value: object) -> None:
    pending: list[tuple[object, int]] = [(value, 1)]
    nodes = 0
    while pending:
        item, depth = pending.pop()
        nodes += 1
        if nodes > MAX_JSON_NODES:
            _malformed()
        if isinstance(item, dict):
            if depth > MAX_JSON_DEPTH:
                _malformed()
            pending.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            if depth > MAX_JSON_DEPTH:
                _malformed()
            pending.extend((child, depth + 1) for child in item)
        elif isinstance(item, float) and not math.isfinite(item):
            _malformed()


def _validate_string_mapping(value: object) -> None:
    if not isinstance(value, dict) or any(
        not isinstance(key, str) or not isinstance(item, str) for key, item in value.items()
    ):
        _malformed()


def decode_catalog_config(body: bytes) -> Mapping[str, object]:
    """Decode one bounded Iceberg REST configuration response."""

    if len(body) > MAX_CATALOG_BODY_BYTES:
        raise ProbeFailure("catalog_response_too_large")
    try:
        text = body.decode("utf-8", errors="strict")
        value = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except ProbeFailure:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError):
        raise ProbeFailure("catalog_response_malformed") from None
    if not isinstance(value, dict):
        _malformed()
    _validate_bounds(value)
    for name in ("defaults", "overrides"):
        if name in value:
            _validate_string_mapping(value[name])
    if "endpoints" in value and (
        not isinstance(value["endpoints"], list) or any(not isinstance(item, str) for item in value["endpoints"])
    ):
        _malformed()
    return value


class _RejectRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        return None


def _validate_probe_config(config: ProbeConfig) -> str:
    try:
        parsed = urllib.parse.urlsplit(config.origin)
        hostname = parsed.hostname
        _ = parsed.port
    except (TypeError, ValueError):
        raise ProbeFailure("probe_origin_invalid") from None
    if (
        parsed.scheme not in {"http", "https"}
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
        or config.timeout_seconds <= 0
        or config.max_body_bytes <= 0
        or config.max_body_bytes > MAX_CATALOG_BODY_BYTES
        or config.slow_seconds < 0
    ):
        raise ProbeFailure("probe_origin_invalid")
    return f"{config.origin}/v1/config"


def probe_catalog(
    config: ProbeConfig,
    *,
    opener: Any | None = None,
    monotonic: Any = time.monotonic,
) -> ProbeResult:
    """Perform one bounded request against the configured catalog origin."""

    url = _validate_probe_config(config)
    if opener is None:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _RejectRedirects())
    request = urllib.request.Request(
        url,
        method="GET",
        headers={"Accept": "application/json", "User-Agent": "data-eng-lab-probe/1"},
    )
    started = monotonic()
    try:
        response = opener.open(request, timeout=config.timeout_seconds)
        try:
            status = int(response.status)
            content_type = response.headers.get("Content-Type", "")
            body = response.read(config.max_body_bytes + 1)
        finally:
            response.close()
    except urllib.error.HTTPError as error:
        try:
            status = int(error.code)
        finally:
            error.close()
        return ProbeResult(False, max(0.0, monotonic() - started), status, "http_error")
    except (TimeoutError, socket.timeout):
        return ProbeResult(False, max(0.0, monotonic() - started), 0, "timeout")
    except urllib.error.URLError as error:
        category = "timeout" if isinstance(error.reason, TimeoutError) else "unavailable"
        return ProbeResult(False, max(0.0, monotonic() - started), 0, category)
    except OSError:
        return ProbeResult(False, max(0.0, monotonic() - started), 0, "unavailable")

    duration = max(0.0, monotonic() - started)
    if status < 200 or status >= 300:
        return ProbeResult(False, duration, status, "http_error")
    if content_type.split(";", 1)[0].strip().lower() != "application/json":
        return ProbeResult(False, duration, status, "malformed")
    try:
        decode_catalog_config(body)
    except ProbeFailure:
        return ProbeResult(False, duration, status, "malformed")
    result = "slow" if duration > config.slow_seconds else "success"
    return ProbeResult(True, duration, status, result)


def _validate_result(value: ProbeResult) -> None:
    if (
        not isinstance(value, ProbeResult)
        or type(value.success) is not bool
        or not isinstance(value.duration_seconds, (int, float))
        or isinstance(value.duration_seconds, bool)
        or not math.isfinite(value.duration_seconds)
        or value.duration_seconds < 0
        or type(value.http_status_code) is not int
        or not 0 <= value.http_status_code <= 599
        or value.result not in RESULT_CATEGORIES
        or value.success != (value.result in {"success", "slow"})
    ):
        raise ProbeFailure("probe_result_invalid")


def render_metrics(value: ProbeResult) -> bytes:
    """Render one result as a fixed, bounded Prometheus text body."""

    _validate_result(value)
    metrics = (
        (
            "success",
            "Whether the latest synthetic catalog probe returned valid JSON.",
            str(int(value.success)),
        ),
        (
            "duration_seconds",
            "Elapsed time of the latest synthetic catalog probe.",
            repr(float(value.duration_seconds)),
        ),
        (
            "http_status_code",
            "HTTP status observed by the latest synthetic catalog probe.",
            str(value.http_status_code),
        ),
    )
    lines: list[str] = []
    for suffix, help_text, sample in metrics:
        name = f"{METRIC_PREFIX}{suffix}"
        lines.extend(
            (
                f"# HELP {name} {help_text}",
                f"# TYPE {name} gauge",
                f'{name}{{target="catalog"}} {sample}',
            )
        )
    result_name = f"{METRIC_PREFIX}result"
    lines.extend(
        (
            f"# HELP {result_name} Closed outcome of the latest synthetic catalog probe.",
            f"# TYPE {result_name} gauge",
        )
    )
    for category in RESULT_CATEGORIES:
        lines.append(f'{result_name}{{target="catalog",result="{category}"}} {int(value.result == category)}')
    body = ("\n".join(lines) + "\n").encode("ascii")
    if len(body) >= 8_192:
        raise ProbeFailure("probe_result_invalid")
    return body


def build_server(config: ProbeConfig, *, host: str = "0.0.0.0", port: int = 8080) -> HTTPServer:
    """Build, but do not start, the single-threaded internal probe server."""

    _validate_probe_config(config)
    if not isinstance(host, str) or type(port) is not int or not 0 <= port <= 65_535:
        raise ProbeFailure("server_config_invalid")

    class Handler(BaseHTTPRequestHandler):
        server_version = "IcebergRestSyntheticProbe/1"
        sys_version = ""

        def do_GET(self) -> None:  # noqa: N802
            if "?" in self.path or self.path not in {"/healthz", "/metrics"}:
                self._json(404, {"code": "not_found"})
                return
            if self.path == "/healthz":
                self._json(200, {"status": "ready", "target": "catalog"})
                return
            self._send(
                200,
                "text/plain; version=0.0.4; charset=utf-8",
                render_metrics(probe_catalog(config)),
            )

        def do_POST(self) -> None:  # noqa: N802
            self._method_not_allowed()

        def do_PUT(self) -> None:  # noqa: N802
            self._method_not_allowed()

        def do_DELETE(self) -> None:  # noqa: N802
            self._method_not_allowed()

        def do_HEAD(self) -> None:  # noqa: N802
            self._method_not_allowed()

        def _method_not_allowed(self) -> None:
            self._json(405, {"code": "method_not_allowed"})

        def _json(self, status: int, value: Mapping[str, str]) -> None:
            body = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
            self._send(status, "application/json", body)

        def _send(self, status: int, content_type: str, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def log_message(self, _format: str, *_args: object) -> None:
            return

    return HTTPServer((host, port), Handler)


def _environment_float(name: str, default: float, maximum: float) -> float:
    raw = os.environ.get(name, str(default))
    try:
        value = float(raw)
    except ValueError:
        raise ProbeFailure("configuration_invalid") from None
    if not math.isfinite(value) or value <= 0 or value > maximum:
        raise ProbeFailure("configuration_invalid")
    return value


def main() -> int:
    origin = os.environ.get("ICEBERG_REST_PROBE_ORIGIN", FIXED_CATALOG_ORIGIN)
    if origin != FIXED_CATALOG_ORIGIN:
        raise ProbeFailure("configuration_invalid")
    config = ProbeConfig(
        origin=origin,
        timeout_seconds=_environment_float("ICEBERG_REST_PROBE_TIMEOUT_SECONDS", 2.0, 2.0),
        max_body_bytes=MAX_CATALOG_BODY_BYTES,
        slow_seconds=_environment_float("ICEBERG_REST_PROBE_SLOW_SECONDS", 1.0, 2.0),
    )
    server = build_server(config)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        return 130
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
