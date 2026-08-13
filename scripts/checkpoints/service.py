"""Bounded authenticated HTTP adapter for checkpoint retention."""

from __future__ import annotations

import hmac
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Mapping, Sequence

from scripts.checkpoints.leases import AcquireRequest, HeartbeatRequest, LeaseManager, TerminalRequest
from scripts.checkpoints.metrics import render_metrics
from scripts.checkpoints.operations import ApplyRequest, OperationManager
from scripts.checkpoints.planner import RetentionPlanner
from scripts.checkpoints.policy import CheckpointPolicy, load_policy
from scripts.checkpoints.s3_gateway import S3Gateway, build_s3_client


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


class RuntimeBackend:
    """Typed composition adapter between HTTP routes and retention managers."""

    def __init__(
        self,
        *,
        gateway: S3Gateway,
        leases: LeaseManager,
        planner: RetentionPlanner,
        operations: OperationManager,
        policy: CheckpointPolicy,
        destructive_enabled: bool,
    ) -> None:
        self._gateway = gateway
        self._leases = leases
        self._planner = planner
        self._operations = operations
        self._policy = policy
        self._destructive_enabled = destructive_enabled
        self._client = getattr(gateway, "_client", None)

    def health(self) -> dict[str, object]:
        capabilities = self._gateway.probe_capabilities()
        if not isinstance(capabilities, Mapping) or capabilities.get("automatic_apply") is not False:
            raise ServiceFailure("capability_failed")
        return {
            "capability_profile": capabilities.get("profile", "manual-verified-readback"),
            "destructive_enabled": self._destructive_enabled,
            "ready": True,
        }

    def metrics(self) -> bytes:
        return render_metrics({})

    def invoke(self, action: str, payload: dict[str, object] | None, operation_id: str | None):
        if action == "lease_acquire":
            value = _exact_payload(payload, {"checkpoint_id", "owner_id", "prefix", "session_id", "workload"})
            result = self._leases.acquire(
                AcquireRequest(
                    value["checkpoint_id"],
                    value["prefix"],
                    value["workload"],
                    value["owner_id"],
                    value["session_id"],
                )
            )
            return _lease_response(result)
        if action == "lease_heartbeat":
            value = _exact_payload(payload, {"checkpoint_id", "epoch", "prefix"})
            result = self._leases.heartbeat(HeartbeatRequest(value["checkpoint_id"], value["prefix"], value["epoch"]))
            return _lease_response(result)
        if action == "lease_terminal":
            value = _exact_payload(payload, {"checkpoint_id", "epoch", "evidence", "prefix", "state"})
            if not isinstance(value["evidence"], Mapping):
                raise ServiceFailure("request_invalid")
            result = self._leases.terminal(
                TerminalRequest(
                    value["checkpoint_id"],
                    value["prefix"],
                    value["epoch"],
                    value["state"],
                    value["evidence"],
                )
            )
            return _lease_response(result)
        if action == "apply":
            if not self._destructive_enabled:
                return {"state": "refused", "refusal_codes": ["destructive_disabled"]}
            value = _exact_payload(payload, {"confirm_prefix", "plan_sha256"})
            status = self._operations.apply(
                ApplyRequest(operation_id or "", value["plan_sha256"], value["confirm_prefix"])
            )
            return json.loads(status.body)
        if action == "status":
            status = self._operations.status(operation_id or "")
            return json.loads(status.body)
        if action in {"plan", "prepare"}:
            raise ServiceFailure("route_not_ready")
        raise ServiceFailure("route_invalid")

    def close(self) -> None:
        close = getattr(self._client, "close", None)
        if callable(close):
            close()


def build_runtime() -> RuntimeBackend:
    access_key = _required_environment("MINIO_RETENTION_ACCESS_KEY")
    secret_key = _required_environment("MINIO_RETENTION_SECRET_KEY")
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    if endpoint != "http://minio:9000":
        raise ServiceFailure("configuration_invalid")
    destructive = os.environ.get("DESTRUCTIVE_ENABLED", "false")
    if destructive not in {"false", "true"}:
        raise ServiceFailure("configuration_invalid")
    policy_path = Path(os.environ.get("CHECKPOINT_RETENTION_POLICY", "/workspace/checkpoints/retention-policy.yaml"))
    try:
        policy = load_policy(policy_path)
        client = build_s3_client(access_key, secret_key)
        gateway = S3Gateway(client, policy)
        planner = RetentionPlanner(gateway, policy)
        operations = OperationManager(
            gateway,
            policy_sha256=_policy_digest(policy),
            now=_now,
            quiescence_seconds=policy.lease.quiescence_seconds,
            max_summary_bytes=policy.bounds.max_summary_bytes,
            max_delete_keys=policy.bounds.max_delete_keys,
        )
        return RuntimeBackend(
            gateway=gateway,
            leases=LeaseManager(gateway, policy, now=_now),
            planner=planner,
            operations=operations,
            policy=policy,
            destructive_enabled=destructive == "true",
        )
    except (KeyboardInterrupt, SystemExit, ServiceFailure):
        raise
    except BaseException:
        raise ServiceFailure("runtime_initialization_failed") from None


def _policy_digest(policy: CheckpointPolicy) -> str:
    from scripts.checkpoints.policy import _policy_sha256

    return _policy_sha256(policy)


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _required_environment(name: str) -> str:
    value = os.environ.get(name)
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 256:
        raise ServiceFailure("configuration_invalid")
    return value


def _exact_payload(payload: object, fields: set[str]) -> dict[str, object]:
    if not isinstance(payload, dict) or set(payload) != fields:
        raise ServiceFailure("request_invalid")
    if any(not isinstance(payload[field], str) for field in fields - {"evidence"}):
        raise ServiceFailure("request_invalid")
    return payload


def _lease_response(result: object) -> dict[str, object]:
    epoch = getattr(result, "epoch", None)
    etag = getattr(result, "etag", None)
    if not isinstance(epoch, str) or not isinstance(etag, str):
        raise ServiceFailure("response_invalid")
    return {"epoch": epoch, "etag": etag, "state": "accepted"}


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


def main() -> int:
    token = _required_environment("CHECKPOINT_RETENTION_API_TOKEN")
    runtime = None
    server = None
    primary: BaseException | None = None
    try:
        try:
            runtime = build_runtime()
        except (KeyboardInterrupt, SystemExit, ServiceFailure):
            raise
        except BaseException:
            raise ServiceFailure("runtime_initialization_failed") from None
        server = create_server(("0.0.0.0", 8080), RetentionApplication(runtime, token=token))
        server.serve_forever()
    except KeyboardInterrupt:
        return 0
    except BaseException as error:
        primary = error
        raise
    finally:
        for target in (server, runtime):
            close = getattr(target, "server_close" if target is server else "close", None)
            if callable(close):
                try:
                    close()
                except (KeyboardInterrupt, SystemExit):
                    if primary is None:
                        raise
                except BaseException:
                    if primary is None:
                        raise ServiceFailure("runtime_close_failed") from None
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
