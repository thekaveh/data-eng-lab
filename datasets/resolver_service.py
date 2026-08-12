"""Internal, read-only HTTP facade for verified dataset resolution."""

from __future__ import annotations

import json
import os
import re
import socket
import threading
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from socketserver import ThreadingMixIn
from urllib.parse import urlsplit

import boto3
from botocore.client import Config

from datasets.locking import canonical_json
from datasets.publication import resolve_active_dataset
from datasets.registry import Dataset, load_registry

_MAX_REQUEST_BYTES = 16 * 1024
_DATASET_RE = re.compile(r"^[a-z][a-z0-9_-]*$")
_CONTENT_LENGTH_RE = re.compile(r"^(?:0|[1-9][0-9]*)$")
_SCALES = frozenset({"tiny", "small", "medium"})
_JSON_CONTENT_TYPE = "application/json"
_GENERIC_RESOLUTION_ERROR = "dataset resolution failed"


class RequestError(ValueError):
    """A bounded, safe client request diagnostic."""


@dataclass
class ResolverServices:
    client: object
    registry: Mapping[str, Dataset]
    _closed: bool = field(default=False, init=False, repr=False)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        close = getattr(self.client, "close", None)
        if callable(close):
            close()


def _require_identifier(document: Mapping[str, object], field: str) -> str:
    value = document.get(field)
    if value is None:
        raise RequestError(f"{field} is required")
    if not isinstance(value, str) or _DATASET_RE.fullmatch(value) is None:
        raise RequestError(f"{field} must be a valid identifier")
    return value


def _require_scale(document: Mapping[str, object], field: str) -> str:
    value = document.get(field)
    if value is None:
        raise RequestError(f"{field} is required")
    if not isinstance(value, str) or value not in _SCALES:
        raise RequestError(f"{field} must be one of: tiny, small, medium")
    return value


def resolve_request(document: Mapping[str, object], services: ResolverServices) -> bytes:
    """Resolve one expected dataset generation and return canonical JSON bytes."""
    if set(document) != {"dataset", "expected_scale"}:
        if "dataset" not in document:
            raise RequestError("dataset is required")
        if "expected_scale" not in document:
            raise RequestError("expected_scale is required")
        raise RequestError("request fields are not exact")
    dataset = _require_identifier(document, "dataset")
    scale = _require_scale(document, "expected_scale")
    if dataset not in services.registry:
        raise RequestError("unknown dataset")
    resolved = resolve_active_dataset(services.client, services.registry, dataset, scale)
    return canonical_json(asdict(resolved))


def _unique_mapping(pairs: list[tuple[str, object]]) -> dict[str, object]:
    document: dict[str, object] = {}
    for key, value in pairs:
        if key in document:
            raise RequestError("request fields must be unique")
        document[key] = value
    return document


def _reject_constant(_value: str) -> object:
    raise ValueError("non-finite JSON value")


def _decode_request(body: bytes) -> Mapping[str, object]:
    try:
        document = json.loads(body, object_pairs_hook=_unique_mapping, parse_constant=_reject_constant)
    except RequestError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError) as error:
        raise RequestError("request body must be valid JSON") from error
    if not isinstance(document, Mapping):
        raise RequestError("request body must be a JSON mapping")
    return document


class _ResolverServer(ThreadingMixIn, HTTPServer):
    daemon_threads = False
    block_on_close = True

    def __init__(
        self,
        address: tuple[str, int],
        handler: type[BaseHTTPRequestHandler],
        services: ResolverServices,
        *,
        request_timeout: float,
        max_connections: int,
        max_resolver_work: int,
    ) -> None:
        if request_timeout <= 0 or max_connections <= 0 or max_resolver_work <= 0:
            raise ValueError("resolver server bounds must be positive")
        self.services = services
        self.request_timeout = request_timeout
        self.connection_slots = threading.BoundedSemaphore(max_connections)
        self.resolver_slots = threading.BoundedSemaphore(max_resolver_work)
        self._services_closed = False
        super().__init__(address, handler)

    def process_request(self, request: socket.socket, client_address: tuple[str, int]) -> None:
        if not self.connection_slots.acquire(blocking=False):
            body = canonical_json({"error": "server is busy"})
            response = (
                b"HTTP/1.0 503 Service Unavailable\r\n"
                b"Content-Type: application/json\r\n"
                + f"Content-Length: {len(body)}\r\n".encode("ascii")
                + b"Cache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n"
                + body
            )
            try:
                request.sendall(response)
            except OSError:
                pass
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self.connection_slots.release()
            raise

    def process_request_thread(self, request: socket.socket, client_address: tuple[str, int]) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.connection_slots.release()

    def handle_error(self, _request: socket.socket, _client_address: tuple[str, int]) -> None:
        return

    def server_close(self) -> None:
        super().server_close()
        if not self._services_closed:
            self._services_closed = True
            close = getattr(self.services, "close", None)
            if callable(close):
                close()


def _handler() -> type[BaseHTTPRequestHandler]:
    class ResolverHandler(BaseHTTPRequestHandler):
        server_version = "dataset-resolver"
        sys_version = ""

        def setup(self) -> None:
            super().setup()
            self.connection.settimeout(self.server.request_timeout)  # type: ignore[attr-defined]

        def log_message(self, _format: str, *_args: object) -> None:
            return

        def _send(self, status: HTTPStatus, body: bytes, *, allow: str | None = None) -> None:
            self.close_connection = True
            try:
                self.send_response(status.value)
                self.send_header("Content-Type", _JSON_CONTENT_TYPE)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.send_header("Connection", "close")
                if allow is not None:
                    self.send_header("Allow", allow)
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(body)
            except OSError:
                self.close_connection = True

        def _error(self, status: HTTPStatus, message: str, *, allow: str | None = None) -> None:
            self._send(status, canonical_json({"error": message}), allow=allow)

        def send_error(self, code: int, message: str | None = None, explain: str | None = None) -> None:
            del message, explain
            original_status = HTTPStatus(code) if code in HTTPStatus._value2member_map_ else HTTPStatus.BAD_REQUEST
            safe_messages = {
                HTTPStatus.REQUEST_URI_TOO_LONG: "request URI is too long",
                HTTPStatus.REQUEST_HEADER_FIELDS_TOO_LARGE: "request headers are too large",
                HTTPStatus.NOT_IMPLEMENTED: "method not allowed",
            }
            status = HTTPStatus.METHOD_NOT_ALLOWED if original_status is HTTPStatus.NOT_IMPLEMENTED else original_status
            self._error(status, safe_messages.get(original_status, "bad request"))

        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/healthz":
                self._send(HTTPStatus.OK, b'{"status":"ok"}')
            elif self.path == "/v1/resolve":
                self._error(HTTPStatus.METHOD_NOT_ALLOWED, "method not allowed", allow="POST")
            else:
                self._error(HTTPStatus.NOT_FOUND, "not found")

        def do_HEAD(self) -> None:  # noqa: N802
            if self.path == "/healthz":
                self._error(HTTPStatus.METHOD_NOT_ALLOWED, "method not allowed", allow="GET")
            elif self.path == "/v1/resolve":
                self._error(HTTPStatus.METHOD_NOT_ALLOWED, "method not allowed", allow="POST")
            else:
                self._error(HTTPStatus.NOT_FOUND, "not found")

        def do_POST(self) -> None:  # noqa: N802
            if self.path != "/v1/resolve":
                if self.path == "/healthz":
                    self._error(HTTPStatus.METHOD_NOT_ALLOWED, "method not allowed", allow="GET")
                else:
                    self._error(HTTPStatus.NOT_FOUND, "not found")
                return
            transfer_encoding = self.headers.get_all("Transfer-Encoding", [])
            if transfer_encoding:
                self._error(HTTPStatus.BAD_REQUEST, "transfer encoding is not supported")
                return
            content_types = self.headers.get_all("Content-Type", [])
            if len(content_types) != 1:
                message = "content type is required" if not content_types else "content type must be unique"
                self._error(HTTPStatus.UNSUPPORTED_MEDIA_TYPE if not content_types else HTTPStatus.BAD_REQUEST, message)
                return
            content_type = content_types[0].split(";", 1)[0].strip().lower()
            if content_type != _JSON_CONTENT_TYPE:
                self._error(HTTPStatus.UNSUPPORTED_MEDIA_TYPE, "content type must be application/json")
                return
            content_lengths = self.headers.get_all("Content-Length", [])
            if not content_lengths:
                self._error(HTTPStatus.BAD_REQUEST, "content length is required")
                return
            if len(content_lengths) != 1:
                self._error(HTTPStatus.BAD_REQUEST, "content length must be unique")
                return
            raw_length = content_lengths[0]
            if _CONTENT_LENGTH_RE.fullmatch(raw_length) is None:
                self._error(HTTPStatus.BAD_REQUEST, "content length is invalid")
                return
            if len(raw_length) > 6 or int(raw_length) > _MAX_REQUEST_BYTES:
                self._error(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "request body is too large")
                return
            content_length = int(raw_length)
            try:
                body = self.rfile.read(content_length)
            except (TimeoutError, socket.timeout):
                self._error(HTTPStatus.REQUEST_TIMEOUT, "request body timed out")
                return
            if len(body) != content_length:
                self._error(HTTPStatus.BAD_REQUEST, "request body is incomplete")
                return
            try:
                document = _decode_request(body)
            except RequestError as error:
                self._error(HTTPStatus.BAD_REQUEST, str(error))
                return
            resolver_slots = self.server.resolver_slots  # type: ignore[attr-defined]
            if not resolver_slots.acquire(blocking=False):
                self._error(HTTPStatus.SERVICE_UNAVAILABLE, "resolver is busy")
                return
            try:
                try:
                    response = resolve_request(document, self.server.services)  # type: ignore[attr-defined]
                except RequestError as error:
                    self._error(HTTPStatus.BAD_REQUEST, str(error))
                except Exception:
                    self._error(HTTPStatus.INTERNAL_SERVER_ERROR, _GENERIC_RESOLUTION_ERROR)
                else:
                    self._send(HTTPStatus.OK, response)
            finally:
                resolver_slots.release()

        def do_PUT(self) -> None:  # noqa: N802
            self._unsupported_method()

        def do_DELETE(self) -> None:  # noqa: N802
            self._unsupported_method()

        def do_PATCH(self) -> None:  # noqa: N802
            self._unsupported_method()

        def do_OPTIONS(self) -> None:  # noqa: N802
            self._unsupported_method()

        def do_TRACE(self) -> None:  # noqa: N802
            self._unsupported_method()

        def do_CONNECT(self) -> None:  # noqa: N802
            self._unsupported_method()

        def _unsupported_method(self) -> None:
            if self.path == "/healthz":
                self._error(HTTPStatus.METHOD_NOT_ALLOWED, "method not allowed", allow="GET")
            elif self.path == "/v1/resolve":
                self._error(HTTPStatus.METHOD_NOT_ALLOWED, "method not allowed", allow="POST")
            else:
                self._error(HTTPStatus.NOT_FOUND, "not found")

    return ResolverHandler


def create_server(
    services: ResolverServices,
    *,
    host: str = "0.0.0.0",  # noqa: S104 - intentionally bound only within the Compose network
    port: int = 8080,
    request_timeout: float = 5.0,
    max_connections: int = 32,
    max_resolver_work: int = 8,
) -> HTTPServer:
    return _ResolverServer(
        (host, port),
        _handler(),
        services,
        request_timeout=request_timeout,
        max_connections=max_connections,
        max_resolver_work=max_resolver_work,
    )


def _required_environment(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"required resolver environment is missing: {name}")
    return value


def container_s3_client():
    endpoint = _required_environment("MINIO_ENDPOINT")
    parsed = urlsplit(endpoint)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
        raise RuntimeError("MINIO_ENDPOINT is invalid")
    if parsed.query or parsed.fragment or parsed.path not in {"", "/"}:
        raise RuntimeError("MINIO_ENDPOINT is invalid")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=_required_environment("MINIO_ROOT_USER"),
        aws_secret_access_key=_required_environment("MINIO_ROOT_PASSWORD"),
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path"},
            retries={"total_max_attempts": 1, "mode": "legacy"},
            connect_timeout=3,
            read_timeout=30,
        ),
    )


def main() -> int:
    registry_path = Path(os.environ.get("DATASET_REGISTRY", "/workspace/datasets/registry.yaml"))
    services = ResolverServices(container_s3_client(), load_registry(registry_path))
    server = create_server(services)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
