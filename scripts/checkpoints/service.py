"""Bounded authenticated HTTP adapter for checkpoint retention."""

from __future__ import annotations

import hmac
import json
import re
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Mapping, Sequence


class ServiceFailure(ValueError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class HttpResponse:
    status: int
    body: bytes
    content_type: str = "application/json"


_OPERATION_ID = r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
_APPLY = re.compile(rf"/v1/operations/(?P<operation_id>{_OPERATION_ID})/apply")
_STATUS = re.compile(rf"/v1/operations/(?P<operation_id>{_OPERATION_ID})")
_POST_ROUTES = {
    "/v1/leases/acquire": "lease_acquire",
    "/v1/leases/heartbeat": "lease_heartbeat",
    "/v1/leases/terminal": "lease_terminal",
    "/v1/plans": "plan",
    "/v1/operations/prepare": "prepare",
}
_MAX_BODY = 65_536


class RetentionApplication:
    def __init__(self, backend: object, *, token: str) -> None:
        if not isinstance(token, str) or not token or len(token.encode("utf-8")) > 256:
            raise ServiceFailure("token_invalid")
        self._backend = backend
        self._token = token

    def dispatch(
        self,
        method: str,
        path: str,
        headers: Sequence[tuple[str, str]],
        body: bytes,
    ) -> HttpResponse:
        if not isinstance(method, str) or not isinstance(path, str) or "?" in path or "#" in path:
            raise ServiceFailure("path_invalid")
        normalized = _headers(headers)
        if method == "GET" and path == "/healthz":
            return self._response(self._backend.health())
        if method == "GET" and path == "/metrics":
            try:
                metrics = self._backend.metrics()
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException:
                raise ServiceFailure("backend_failure") from None
            if type(metrics) is not bytes or len(metrics) > _MAX_BODY:
                raise ServiceFailure("response_invalid")
            return HttpResponse(200, metrics, "text/plain; version=0.0.4")
        self._authenticate(normalized)
        if method == "POST":
            payload = _decode_body(normalized, body)
            action = _POST_ROUTES.get(path)
            operation_id = None
            if action is None:
                match = _APPLY.fullmatch(path)
                if match:
                    action = "apply"
                    operation_id = match.group("operation_id")
            if action is None:
                raise ServiceFailure("route_invalid")
            return self._invoke(action, payload, operation_id)
        if method == "GET":
            if body:
                raise ServiceFailure("body_forbidden")
            match = _STATUS.fullmatch(path)
            if not match:
                raise ServiceFailure("route_invalid")
            return self._invoke("status", None, match.group("operation_id"))
        raise ServiceFailure("method_invalid")

    def _authenticate(self, headers: Mapping[str, str]) -> None:
        authorization = headers.get("authorization")
        expected = f"Bearer {self._token}"
        if not isinstance(authorization, str) or not hmac.compare_digest(authorization, expected):
            raise ServiceFailure("unauthorized")

    def _invoke(self, action: str, payload: dict[str, object] | None, operation_id: str | None) -> HttpResponse:
        try:
            value = self._backend.invoke(action, payload, operation_id)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise ServiceFailure("backend_failure") from None
        return self._response(value)

    @staticmethod
    def _response(value: object) -> HttpResponse:
        try:
            body = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
        except (TypeError, ValueError, UnicodeError):
            raise ServiceFailure("response_invalid") from None
        if len(body) > _MAX_BODY:
            raise ServiceFailure("response_invalid")
        return HttpResponse(200, body)


def create_server(address: tuple[str, int], application: RetentionApplication) -> ThreadingHTTPServer:
    if address != ("0.0.0.0", 8080) or not isinstance(application, RetentionApplication):
        raise ServiceFailure("server_config_invalid")

    class Handler(BaseHTTPRequestHandler):
        server_version = "CheckpointRetention/1"
        sys_version = ""

        def do_GET(self):
            self._handle()

        def do_POST(self):
            self._handle()

        def _handle(self):
            length_values = self.headers.get_all("Content-Length", failobj=[])
            if len(length_values) > 1:
                self._send_failure(ServiceFailure("header_duplicate"))
                return
            try:
                length = int(length_values[0]) if length_values else 0
            except ValueError:
                self._send_failure(ServiceFailure("content_length_invalid"))
                return
            if length < 0 or length > _MAX_BODY:
                self._send_failure(ServiceFailure("body_too_large"))
                return
            body = self.rfile.read(length) if length else b""
            try:
                response = application.dispatch(self.command, self.path, tuple(self.headers.raw_items()), body)
            except ServiceFailure as error:
                self._send_failure(error)
                return
            self.send_response(response.status)
            self.send_header("Content-Type", response.content_type)
            self.send_header("Content-Length", str(len(response.body)))
            self.end_headers()
            self.wfile.write(response.body)

        def _send_failure(self, error: ServiceFailure):
            body = json.dumps({"code": error.code}, separators=(",", ":")).encode("ascii")
            self.send_response(400 if error.code != "unauthorized" else 401)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, _format, *_args):
            return

    server = ThreadingHTTPServer(address, Handler)
    server.daemon_threads = True
    return server


def _headers(headers: Sequence[tuple[str, str]]) -> dict[str, str]:
    result: dict[str, str] = {}
    for name, value in headers:
        if not isinstance(name, str) or not isinstance(value, str):
            raise ServiceFailure("header_invalid")
        key = name.lower()
        if key in result:
            raise ServiceFailure("header_duplicate")
        if len(name) > 128 or len(value) > 512 or "\r" in value or "\n" in value:
            raise ServiceFailure("header_invalid")
        result[key] = value
    return result


def _decode_body(headers: Mapping[str, str], body: bytes) -> dict[str, object]:
    if "transfer-encoding" in headers:
        raise ServiceFailure("transfer_encoding_forbidden")
    if headers.get("content-type") != "application/json":
        raise ServiceFailure("content_type_invalid")
    raw_length = headers.get("content-length")
    if not isinstance(raw_length, str) or not raw_length.isdigit() or int(raw_length) != len(body):
        raise ServiceFailure("content_length_mismatch")
    if type(body) is not bytes or len(body) > _MAX_BODY:
        raise ServiceFailure("body_too_large")

    def pairs(values):
        result = {}
        for key, value in values:
            if key in result:
                raise ServiceFailure("json_duplicate_key")
            result[key] = value
        return result

    try:
        value = json.loads(body.decode("utf-8"), object_pairs_hook=pairs)
    except ServiceFailure:
        raise
    except (UnicodeError, json.JSONDecodeError, RecursionError):
        raise ServiceFailure("json_invalid") from None
    if not isinstance(value, dict):
        raise ServiceFailure("json_shape_invalid")
    _check_structure(value, depth=0, nodes=[0])
    return value


def _check_structure(value: object, *, depth: int, nodes: list[int]) -> None:
    nodes[0] += 1
    if depth > 32 or nodes[0] > 4_096:
        raise ServiceFailure("json_structure_bound")
    if isinstance(value, dict):
        for item in value.values():
            _check_structure(item, depth=depth + 1, nodes=nodes)
    elif isinstance(value, list):
        for item in value:
            _check_structure(item, depth=depth + 1, nodes=nodes)
