"""Internal, read-only HTTP facade for verified dataset resolution."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlsplit

import boto3
from botocore.client import Config

from datasets.locking import canonical_json
from datasets.publication import resolve_active_dataset
from datasets.registry import Dataset, load_registry

_MAX_REQUEST_BYTES = 16 * 1024
_DATASET_RE = re.compile(r"^[a-z][a-z0-9-]*$")
_SCALES = frozenset({"tiny", "small", "medium"})
_JSON_CONTENT_TYPE = "application/json"
_GENERIC_RESOLUTION_ERROR = "dataset resolution failed"


class RequestError(ValueError):
    """A bounded, safe client request diagnostic."""


@dataclass(frozen=True)
class ResolverServices:
    client: object
    registry: Mapping[str, Dataset]


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
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise RequestError("request body must be valid JSON") from error
    if not isinstance(document, Mapping):
        raise RequestError("request body must be a JSON mapping")
    return document


def _handler(services: ResolverServices) -> type[BaseHTTPRequestHandler]:
    class ResolverHandler(BaseHTTPRequestHandler):
        server_version = "dataset-resolver"
        sys_version = ""

        def log_message(self, _format: str, *_args: object) -> None:
            return

        def _send(self, status: HTTPStatus, body: bytes, *, allow: str | None = None) -> None:
            self.send_response(status.value)
            self.send_header("Content-Type", _JSON_CONTENT_TYPE)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            if allow is not None:
                self.send_header("Allow", allow)
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def _error(self, status: HTTPStatus, message: str, *, allow: str | None = None) -> None:
            self._send(status, canonical_json({"error": message}), allow=allow)

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
            content_type = self.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
            if content_type != _JSON_CONTENT_TYPE:
                self._error(HTTPStatus.UNSUPPORTED_MEDIA_TYPE, "content type must be application/json")
                return
            raw_length = self.headers.get("Content-Length")
            try:
                content_length = int(raw_length) if raw_length is not None else -1
            except ValueError:
                content_length = -1
            if content_length < 0:
                self._error(HTTPStatus.BAD_REQUEST, "content length is required")
                return
            if content_length > _MAX_REQUEST_BYTES:
                self._error(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "request body is too large")
                return
            body = self.rfile.read(content_length)
            if len(body) != content_length:
                self._error(HTTPStatus.BAD_REQUEST, "request body is incomplete")
                return
            try:
                response = resolve_request(_decode_request(body), services)
            except RequestError as error:
                self._error(HTTPStatus.BAD_REQUEST, str(error))
            except Exception:
                self._error(HTTPStatus.INTERNAL_SERVER_ERROR, _GENERIC_RESOLUTION_ERROR)
            else:
                self._send(HTTPStatus.OK, response)

        def do_PUT(self) -> None:  # noqa: N802
            self._unsupported_method()

        def do_DELETE(self) -> None:  # noqa: N802
            self._unsupported_method()

        def do_PATCH(self) -> None:  # noqa: N802
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
) -> HTTPServer:
    return HTTPServer((host, port), _handler(services))


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
