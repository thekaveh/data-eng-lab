"""Bounded synthetic availability probe for the fixed Atlas Iceberg REST catalog."""

from __future__ import annotations

import json
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, NoReturn

MAX_CATALOG_BODY_BYTES = 65_536
MAX_JSON_DEPTH = 16
MAX_JSON_NODES = 4_096


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
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError):
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
    parsed = urllib.parse.urlsplit(config.origin)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
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
    try:
        parsed.port
    except ValueError:
        raise ProbeFailure("probe_origin_invalid") from None
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
