"""Bounded authenticated HTTP adapter for checkpoint retention."""

from __future__ import annotations

import hmac
import json
import math
import os
import re
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import MappingProxyType
from typing import Mapping, Sequence

from scripts.checkpoints.leases import (
    AcquireRequest,
    CheckpointLockRegistry,
    HeartbeatRequest,
    LeaseManager,
    TerminalRequest,
)
from scripts.checkpoints.metrics import MetricsFailure, render_metrics
from scripts.checkpoints.operations import ApplyRequest, OperationFailure, OperationManager, PrepareRequest
from scripts.checkpoints.planner import PlanFailure, PlanRequest, RetentionPlanner
from scripts.checkpoints.policy import CheckpointPolicy, load_policy
from scripts.checkpoints.records import RecordFailure, canonical_json_bytes, decode_plan_artifact
from scripts.checkpoints.s3_gateway import GatewayFailure, S3Gateway, build_s3_client


class ServiceFailure(ValueError):
    def __init__(self, code: str, *, status: int = 400, state: str | None = None) -> None:
        super().__init__(code)
        self.code = code
        self.status = status
        self.state = state


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
_MAX_PLAN_BODY = 128 * 1024 * 1024
_MAX_PLAN_NODES = 600_128
_LEASE_ACTIONS = {"lease_acquire", "lease_heartbeat", "lease_terminal"}
_METRIC_OPERATION_CACHE = 4_096


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
        now=None,
        operation_id=None,
        monotonic=None,
        max_operation_seconds: int = 900,
    ) -> None:
        if type(max_operation_seconds) is not int or not 1 <= max_operation_seconds <= 900:
            raise ServiceFailure("runtime_config_invalid")
        self._gateway = gateway
        self._leases = leases
        self._planner = planner
        self._operations = operations
        self._policy = policy
        self._destructive_enabled = destructive_enabled
        self._now = now or (lambda: datetime.now(timezone.utc).replace(microsecond=0))
        self._operation_id = operation_id
        self._monotonic = monotonic or time.monotonic
        self._max_operation_seconds = max_operation_seconds
        self._client = getattr(gateway, "_client", None)
        self._metrics: dict[str, dict[tuple[str, ...], int]] = {}
        self._metrics_lock = threading.RLock()
        self._apply_metric_totals: dict[str, tuple[int, int]] = {}
        self._apply_metric_transitions: set[tuple[str, str, object]] = set()
        self._metric_saturated = False
        self._capabilities: Mapping[str, object] | None = None

    def health(self) -> dict[str, object]:
        if self._capabilities is None:
            observed = self._gateway.probe_capabilities()
            if isinstance(observed, Mapping):
                self._capabilities = MappingProxyType(dict(observed))
        capabilities = self._capabilities
        if (
            not isinstance(capabilities, Mapping)
            or capabilities.get("automatic_apply") is not False
            or capabilities.get("observed") is not True
            or capabilities.get("conditional_create") is not True
            or capabilities.get("conditional_create_conflict") is not True
            or capabilities.get("conditional_replace_verified_readback") is not True
            or capabilities.get("stale_replace_denied") is not True
            or capabilities.get("conditional_delete") is not False
            or capabilities.get("exact_leaf_list") is not True
            or capabilities.get("exact_leaf_get") is not True
            or capabilities.get("exact_leaf_delete") is not True
            or capabilities.get("multi_delete") is not True
            or capabilities.get("root_list_denied") is not True
            or capabilities.get("other_bucket_denied") is not True
            or capabilities.get("data_put_denied") is not True
            or capabilities.get("unknown_control_denied") is not True
        ):
            raise ServiceFailure("capability_failed")
        return {
            "capability_profile": capabilities.get("profile", "manual-verified-readback"),
            "destructive_enabled": self._destructive_enabled,
            "ready": True,
        }

    def metrics(self) -> bytes:
        try:
            with self._metrics_lock:
                snapshot = {name: dict(values) for name, values in self._metrics.items()}
            return render_metrics(snapshot)
        except MetricsFailure:
            fallback = {
                "checkpoint_retention_request_failures_total": {("backend_failure",): 1},
            }
            return render_metrics(fallback)

    def record_request_failure(self, outcome: str) -> None:
        if outcome not in {
            "backend_failure",
            "capability_failed",
            "invalid_request",
            "metrics_saturated",
            "timeout",
            "unauthorized",
        }:
            outcome = "backend_failure"
        self._increment("checkpoint_retention_request_failures_total", outcome)

    def _increment(self, metric: str, label: str, amount: int = 1) -> None:
        with self._metrics_lock:
            values = self._metrics.setdefault(metric, {})
            key = (label,)
            values[key] = values.get(key, 0) + amount

    def _set(self, metric: str, label: str, amount: int | float) -> None:
        with self._metrics_lock:
            self._metrics.setdefault(metric, {})[(label,)] = amount

    def invoke(self, action: str, payload: dict[str, object] | None, operation_id: str | None):
        try:
            started = self._monotonic()
        except BaseException:
            raise ServiceFailure("operation_deadline", status=504) from None
        if isinstance(started, bool) or not isinstance(started, (int, float)) or not math.isfinite(started):
            raise ServiceFailure("operation_deadline", status=504)

        def check_deadline() -> None:
            try:
                elapsed = self._monotonic() - started
            except BaseException:
                raise GatewayFailure("operation_deadline") from None
            if isinstance(elapsed, bool) or not isinstance(elapsed, (int, float)) or not math.isfinite(elapsed):
                raise GatewayFailure("operation_deadline")
            if elapsed < 0 or elapsed > self._max_operation_seconds:
                raise GatewayFailure("operation_deadline")

        deadline = getattr(self._gateway, "operation_deadline", None)
        try:
            if callable(deadline):
                with deadline(check_deadline):
                    return self._invoke(action, payload, operation_id)
            result = self._invoke(action, payload, operation_id)
            check_deadline()
            return result
        except GatewayFailure as error:
            if error.code == "operation_deadline":
                raise ServiceFailure("operation_deadline", status=504) from None
            raise
        except OperationFailure as error:
            if error.code == "operation_deadline":
                raise ServiceFailure("operation_deadline", status=504) from None
            raise

    def _invoke(self, action: str, payload: dict[str, object] | None, operation_id: str | None):
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
            response = _lease_response(result)
            self._record_lease_metrics(value["checkpoint_id"], result.body)
            return response
        if action == "lease_heartbeat":
            value = _exact_payload(payload, {"checkpoint_id", "epoch", "prefix"})
            result = self._leases.heartbeat(HeartbeatRequest(value["checkpoint_id"], value["prefix"], value["epoch"]))
            response = _lease_response(result)
            self._record_lease_metrics(value["checkpoint_id"], result.body)
            return response
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
            response = _lease_response(result)
            self._record_lease_metrics(value["checkpoint_id"], result.body)
            if value["evidence"].get("successful") is True:
                self._set(
                    "checkpoint_retention_last_success_unixtime",
                    value["checkpoint_id"],
                    int(self._now().timestamp()),
                )
            return response
        if action == "apply":
            if not self._destructive_enabled:
                return {"state": "refused", "refusal_codes": ["destructive_disabled"]}
            value = _exact_payload(payload, {"confirm_prefix", "plan_sha256"})
            self._require_disposable(value["confirm_prefix"])
            try:
                status = self._operations.apply(
                    ApplyRequest(operation_id or "", value["plan_sha256"], value["confirm_prefix"])
                )
            except OperationFailure as error:
                if error.partial:
                    try:
                        status = self._operations.status(operation_id or "")
                    except (KeyboardInterrupt, SystemExit):
                        raise
                    except BaseException:
                        status = None
                    if status is not None and status.state == "partial":
                        result = json.loads(status.body)
                        self._record_apply_metrics(result)
                        return result
                    self._increment("checkpoint_retention_partial_total", "partial")
                    raise ServiceFailure(error.code, status=409, state="partial") from None
                raise ServiceFailure(
                    error.code,
                    status=504 if error.code == "operation_deadline" else 409,
                    state=None if error.code == "operation_deadline" else "refused",
                ) from None
            result = json.loads(status.body)
            self._record_apply_metrics(result)
            return result
        if action == "status":
            status = self._operations.status(operation_id or "")
            return json.loads(status.body)
        if action == "plan":
            if isinstance(payload, dict) and set(payload) == {"actor", "checkpoint_ids"}:
                return self._bulk_plan(payload)
            value = _exact_payload(payload, {"actor", "checkpoint_id", "prefix"})
            artifact = self._planner.plan(
                PlanRequest(
                    value["checkpoint_id"],
                    value["prefix"],
                    value["actor"],
                    self._now(),
                )
            )
            result = json.loads(artifact.body)
            decision = result.get("summary", {}).get("decision")
            if decision in {"eligible", "refused"}:
                self._record_plan_metrics(result["summary"])
            return result
        if action == "prepare":
            if not isinstance(payload, dict) or set(payload) != {"actor", "plan", "plan_sha256", "review"}:
                raise ServiceFailure("request_invalid")
            if any(not isinstance(payload[field], str) for field in {"actor", "plan_sha256", "review"}):
                raise ServiceFailure("request_invalid")
            try:
                plan_body = canonical_json_bytes(
                    payload["plan"],
                    max_bytes=_MAX_PLAN_BODY,
                    max_nodes=_MAX_PLAN_NODES,
                )
                artifact = decode_plan_artifact(
                    plan_body,
                    max_body_bytes=_MAX_PLAN_BODY,
                    max_shard_bytes=getattr(
                        getattr(self._policy, "bounds", None),
                        "max_manifest_shard_bytes",
                        1_048_576,
                    ),
                    max_nodes=_MAX_PLAN_NODES,
                )
                self._require_disposable(artifact.summary.get("prefix"))
                identifier = (
                    self._operation_id()
                    if callable(self._operation_id)
                    else str(
                        uuid.uuid5(
                            uuid.NAMESPACE_URL,
                            f"checkpoint-retention:{payload['plan_sha256']}:{payload['actor']}:{payload['review']}",
                        )
                    )
                )
                status = self._operations.prepare(
                    PrepareRequest(identifier, artifact, payload["plan_sha256"], payload["review"], payload["actor"])
                )
                result = json.loads(status.body)
                self._increment("checkpoint_retention_prepared_total", "completed")
                return result
            except (KeyboardInterrupt, SystemExit):
                raise
            except ServiceFailure:
                raise
            except OperationFailure as error:
                if error.code == "operation_deadline":
                    raise ServiceFailure("operation_deadline", status=504) from None
                raise ServiceFailure(error.code, status=409, state="partial" if error.partial else "refused") from None
            except (RecordFailure, ValueError, TypeError):
                raise ServiceFailure("request_invalid") from None
        raise ServiceFailure("route_invalid")

    def _record_lease_metrics(self, checkpoint_id: object, body: bytes) -> None:
        allowed = getattr(
            __import__("scripts.checkpoints.metrics", fromlist=["_CHECKPOINT_IDS"]),
            "_CHECKPOINT_IDS",
        )
        if checkpoint_id not in allowed:
            return
        try:
            value = json.loads(body)
            heartbeat = _parse_utc(value["heartbeat_at"])
            age = max(0, int((self._now() - heartbeat).total_seconds()))
        except (KeyError, TypeError, ValueError, json.JSONDecodeError, ServiceFailure):
            raise ServiceFailure("lease_response_invalid") from None
        self._set("checkpoint_retention_lease_heartbeat_age_seconds", checkpoint_id, age)

    def _record_plan_metrics(self, summary: Mapping[str, object]) -> None:
        decision = summary.get("decision")
        if decision not in {"eligible", "refused"}:
            return
        self._increment("checkpoint_retention_plans_total", decision)
        checkpoint_id = summary.get("checkpoint_id")
        inventory = summary.get("inventory", {})
        allowed = getattr(
            __import__("scripts.checkpoints.metrics", fromlist=["_CHECKPOINT_IDS"]),
            "_CHECKPOINT_IDS",
        )
        if checkpoint_id in allowed and isinstance(inventory, Mapping):
            object_count = inventory.get("object_count", 0)
            total_bytes = inventory.get("total_bytes", 0)
            if type(object_count) is int and type(total_bytes) is int:
                self._set("checkpoint_retention_objects", checkpoint_id, object_count)
                self._set("checkpoint_retention_bytes", checkpoint_id, total_bytes)
                self._set(
                    "checkpoint_retention_eligible_bytes",
                    checkpoint_id,
                    total_bytes if decision == "eligible" else 0,
                )
        refusal_codes = summary.get("refusal_codes", ())
        if isinstance(refusal_codes, (list, tuple)):
            for code in refusal_codes:
                if isinstance(code, str):
                    self._increment("checkpoint_retention_refusals_total", code)

    def _record_apply_metrics(self, result: Mapping[str, object]) -> None:
        state = result.get("state")
        if state not in {"not_ready", "partial", "completed"}:
            return
        deleted_objects = result.get("deleted_objects", 0)
        deleted_bytes = result.get("deleted_bytes", 0)
        operation_id = result.get("operation_id")
        if (
            not isinstance(operation_id, str)
            or type(deleted_objects) is not int
            or deleted_objects < 0
            or type(deleted_bytes) is not int
            or deleted_bytes < 0
        ):
            return
        with self._metrics_lock:
            if (
                operation_id not in self._apply_metric_totals
                and len(self._apply_metric_totals) >= _METRIC_OPERATION_CACHE
            ):
                if not self._metric_saturated:
                    self._increment("checkpoint_retention_request_failures_total", "metrics_saturated")
                    self._metric_saturated = True
                return
            previous_objects, previous_bytes = self._apply_metric_totals.get(operation_id, (0, 0))
            if deleted_objects < previous_objects or deleted_bytes < previous_bytes:
                return
            object_delta = deleted_objects - previous_objects
            byte_delta = deleted_bytes - previous_bytes
            if object_delta:
                self._increment("checkpoint_retention_deleted_objects_total", state, object_delta)
            if byte_delta:
                self._increment("checkpoint_retention_deleted_bytes_total", state, byte_delta)
            self._apply_metric_totals[operation_id] = (deleted_objects, deleted_bytes)
            transition = (operation_id, state, result.get("attempt_sequence"))
            if (
                transition not in self._apply_metric_transitions
                and len(self._apply_metric_transitions) < _METRIC_OPERATION_CACHE * 4
            ):
                if state == "partial":
                    self._increment("checkpoint_retention_partial_total", "partial")
                self._apply_metric_transitions.add(transition)
        checkpoint_id = result.get("checkpoint_id")
        if state == "completed" and checkpoint_id in getattr(
            __import__("scripts.checkpoints.metrics", fromlist=["_CHECKPOINT_IDS"]),
            "_CHECKPOINT_IDS",
        ):
            self._set("checkpoint_retention_last_success_unixtime", checkpoint_id, int(self._now().timestamp()))

    def _require_disposable(self, prefix: object) -> None:
        if (
            not isinstance(prefix, str)
            or re.fullmatch(
                r"streaming_test/[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/", prefix
            )
            is None
        ):
            raise ServiceFailure("destructive_scope_invalid", status=409, state="refused")
        try:
            matched = self._policy.match_prefix(prefix)
            entry = self._policy.entries[matched.checkpoint_id]
        except BaseException:
            raise ServiceFailure("destructive_scope_invalid", status=409, state="refused") from None
        if matched.checkpoint_id != "go-live-streaming-test-v1" or entry.durability != "disposable_acceptance":
            raise ServiceFailure("destructive_scope_invalid", status=409, state="refused")

    def _bulk_plan(self, payload: dict[str, object]) -> dict[str, object]:
        actor = payload.get("actor")
        checkpoint_ids = payload.get("checkpoint_ids")
        if (
            not isinstance(actor, str)
            or not isinstance(checkpoint_ids, list)
            or any(not isinstance(item, str) for item in checkpoint_ids)
            or checkpoint_ids != list(self._policy.entries)
        ):
            raise ServiceFailure("request_invalid")
        evaluated_at = self._now()
        digest = _policy_digest(self._policy)
        summaries = []
        for checkpoint_id in checkpoint_ids:
            entry = self._policy.entries[checkpoint_id]
            if "{" in entry.prefix:
                summaries.append(_refused_summary(checkpoint_id, digest, "concrete_prefix_required"))
                self._record_plan_metrics(summaries[-1])
                continue
            try:
                artifact = self._planner.plan(PlanRequest(checkpoint_id, entry.prefix, actor, evaluated_at))
            except PlanFailure as error:
                summaries.append(_refused_summary(checkpoint_id, digest, error.code))
                self._record_plan_metrics(summaries[-1])
            else:
                summary = artifact.summary
                summaries.append(
                    {
                        "checkpoint_id": checkpoint_id,
                        "decision": summary["decision"],
                        "inventory": {
                            "object_count": summary["inventory"]["object_count"],
                            "total_bytes": summary["inventory"]["total_bytes"],
                        },
                        "policy_sha256": summary["policy_sha256"],
                        "refusal_codes": list(summary["refusal_codes"]),
                    }
                )
                self._record_plan_metrics(summaries[-1])
        return {"plans": summaries, "state": "accepted"}

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
        locks = CheckpointLockRegistry()

        def revalidate(prefix: str, _evaluated_at: datetime):
            matched = policy.match_prefix(prefix)
            return planner.plan(PlanRequest(matched.checkpoint_id, prefix, "retention-revalidation", _now()))

        operations = OperationManager(
            gateway,
            policy_sha256=_policy_digest(policy),
            now=_now,
            quiescence_seconds=policy.lease.quiescence_seconds,
            max_summary_bytes=policy.bounds.max_summary_bytes,
            max_delete_keys=policy.bounds.max_delete_keys,
            revalidate=revalidate,
            locks=locks,
            max_active_seconds=policy.bounds.max_active_seconds,
        )
        runtime = RuntimeBackend(
            gateway=gateway,
            leases=LeaseManager(gateway, policy, now=_now, locks=locks),
            planner=planner,
            operations=operations,
            policy=policy,
            destructive_enabled=destructive == "true",
            now=_now,
        )
        try:
            runtime.health()
        except BaseException as primary:
            try:
                runtime.close()
            except BaseException:
                primary.add_note("runtime_close_failed")
            raise
        return runtime
    except (KeyboardInterrupt, SystemExit, ServiceFailure):
        raise
    except BaseException:
        raise ServiceFailure("runtime_initialization_failed") from None


def _policy_digest(policy: CheckpointPolicy) -> str:
    from scripts.checkpoints.policy import _policy_sha256

    return _policy_sha256(policy)


def _parse_utc(value: object) -> datetime:
    if not isinstance(value, str) or re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", value) is None:
        raise ServiceFailure("request_invalid")
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        raise ServiceFailure("request_invalid") from None


def _refused_summary(checkpoint_id: str, policy_sha256: str, code: str) -> dict[str, object]:
    return {
        "checkpoint_id": checkpoint_id,
        "decision": "refused",
        "inventory": {"object_count": 0, "total_bytes": 0},
        "policy_sha256": policy_sha256,
        "refusal_codes": [code],
    }


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
    def __init__(
        self,
        backend: object,
        *,
        lease_token: str | None = None,
        operator_token: str | None = None,
        token: str | None = None,
    ) -> None:
        lease_token = token if lease_token is None else lease_token
        operator_token = token if operator_token is None else operator_token
        if any(
            not isinstance(value, str) or not value or len(value.encode("utf-8")) > 256
            for value in (lease_token, operator_token)
        ):
            raise ServiceFailure("token_invalid")
        self._backend = backend
        self._lease_token = lease_token
        self._operator_token = operator_token

    def dispatch(
        self,
        method: str,
        path: str,
        headers: Sequence[tuple[str, str]],
        body: bytes,
    ) -> HttpResponse:
        try:
            return self._dispatch(method, path, headers, body)
        except (KeyboardInterrupt, SystemExit):
            raise
        except ServiceFailure as error:
            outcome = (
                "unauthorized"
                if error.code == "unauthorized"
                else "timeout"
                if error.code in {"operation_deadline", "request_timeout"}
                else "capability_failed"
                if error.code == "capability_failed"
                else "invalid_request"
                if error.status < 500
                else "backend_failure"
            )
            recorder = getattr(self._backend, "record_request_failure", None)
            if callable(recorder):
                try:
                    recorder(outcome)
                except (KeyboardInterrupt, SystemExit):
                    raise
                except BaseException:
                    pass
            raise

    def _dispatch(
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
        if method == "POST":
            self._authenticate(normalized, _POST_ROUTES.get(path, "operator"))
            action = _POST_ROUTES.get(path)
            operation_id = None
            if action is None:
                match = _APPLY.fullmatch(path)
                if match:
                    action = "apply"
                    operation_id = match.group("operation_id")
            if action is None:
                raise ServiceFailure("route_invalid")
            payload = _decode_body(
                normalized,
                body,
                max_bytes=_MAX_PLAN_BODY if action == "prepare" else _MAX_BODY,
                max_nodes=_MAX_PLAN_NODES if action == "prepare" else 4_096,
            )
            return self._invoke(action, payload, operation_id)
        if method == "GET":
            if body:
                raise ServiceFailure("body_forbidden")
            match = _STATUS.fullmatch(path)
            if not match:
                raise ServiceFailure("route_invalid")
            self._authenticate(normalized, "status")
            return self._invoke("status", None, match.group("operation_id"))
        raise ServiceFailure("method_invalid")

    def _authenticate(self, headers: Mapping[str, str], action: str) -> None:
        authorization = headers.get("authorization")
        expected = f"Bearer {self._lease_token if action in _LEASE_ACTIONS else self._operator_token}"
        if not isinstance(authorization, str) or not hmac.compare_digest(authorization, expected):
            raise ServiceFailure("unauthorized")

    def _invoke(self, action: str, payload: dict[str, object] | None, operation_id: str | None) -> HttpResponse:
        try:
            value = self._backend.invoke(action, payload, operation_id)
        except (KeyboardInterrupt, SystemExit):
            raise
        except ServiceFailure as error:
            if error.state is not None:
                return self._response({"code": error.code, "state": error.state}, status=error.status)
            raise
        except BaseException:
            raise ServiceFailure("backend_failure", status=500) from None
        return self._response(value, max_bytes=_MAX_PLAN_BODY if action == "plan" else _MAX_BODY)

    @staticmethod
    def _response(value: object, *, status: int = 200, max_bytes: int = _MAX_BODY) -> HttpResponse:
        try:
            body = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
        except (TypeError, ValueError, UnicodeError):
            raise ServiceFailure("response_invalid") from None
        if len(body) > max_bytes:
            raise ServiceFailure("response_invalid")
        return HttpResponse(status, body)


def create_server(
    address: tuple[str, int],
    application: RetentionApplication,
    *,
    max_workers: int = 16,
    request_timeout_seconds: int = 30,
) -> ThreadingHTTPServer:
    if address != ("0.0.0.0", 8080) or not isinstance(application, RetentionApplication):
        raise ServiceFailure("server_config_invalid")

    if (
        type(max_workers) is not int
        or not 1 <= max_workers <= 64
        or type(request_timeout_seconds) is not int
        or not 1 <= request_timeout_seconds <= 60
    ):
        raise ServiceFailure("server_config_invalid")

    class Handler(BaseHTTPRequestHandler):
        server_version = "CheckpointRetention/1"
        sys_version = ""

        def setup(self):
            super().setup()
            self.connection.settimeout(self.server.request_timeout_seconds)

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
            request_bound = _MAX_PLAN_BODY if self.path == "/v1/operations/prepare" else _MAX_BODY
            if length < 0 or length > request_bound:
                self._send_failure(ServiceFailure("body_too_large"))
                return
            prepare_slot = self.path == "/v1/operations/prepare"
            if prepare_slot and not self.server.prepare_slots.acquire(blocking=False):
                self._send_failure(ServiceFailure("prepare_busy", status=503))
                return
            try:
                try:
                    body = self.rfile.read(length) if length else b""
                except TimeoutError:
                    self._send_failure(ServiceFailure("request_timeout", status=408))
                    return
                if len(body) != length:
                    self._send_failure(ServiceFailure("request_body_incomplete"))
                    return
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
            finally:
                if prepare_slot:
                    self.server.prepare_slots.release()

        def _send_failure(self, error: ServiceFailure):
            body = json.dumps({"code": error.code}, separators=(",", ":")).encode("ascii")
            self.send_response(401 if error.code == "unauthorized" else error.status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, _format, *_args):
            return

    class BoundedServer(ThreadingHTTPServer):
        def __init__(self, *args, **kwargs):
            self.max_workers = max_workers
            self.request_timeout_seconds = request_timeout_seconds
            self._work_slots = threading.BoundedSemaphore(max_workers)
            self.prepare_slots = threading.BoundedSemaphore(1)
            super().__init__(*args, **kwargs)

        def process_request(self, request, client_address):
            if not self._work_slots.acquire(blocking=False):
                self.shutdown_request(request)
                return
            try:
                super().process_request(request, client_address)
            except BaseException:
                self._work_slots.release()
                raise

        def process_request_thread(self, request, client_address):
            try:
                super().process_request_thread(request, client_address)
            finally:
                self._work_slots.release()

    server = BoundedServer(address, Handler)
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


def _decode_body(
    headers: Mapping[str, str],
    body: bytes,
    *,
    max_bytes: int = _MAX_BODY,
    max_nodes: int = 4_096,
) -> dict[str, object]:
    if "transfer-encoding" in headers:
        raise ServiceFailure("transfer_encoding_forbidden")
    if headers.get("content-type") != "application/json":
        raise ServiceFailure("content_type_invalid")
    raw_length = headers.get("content-length")
    if not isinstance(raw_length, str) or not raw_length.isdigit() or int(raw_length) != len(body):
        raise ServiceFailure("content_length_mismatch")
    if type(body) is not bytes or len(body) > max_bytes:
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
    _check_structure(value, depth=0, nodes=[0], max_nodes=max_nodes)
    return value


def _check_structure(value: object, *, depth: int, nodes: list[int], max_nodes: int = 4_096) -> None:
    nodes[0] += 1
    if depth > 32 or nodes[0] > max_nodes:
        raise ServiceFailure("json_structure_bound")
    if isinstance(value, dict):
        for item in value.values():
            _check_structure(item, depth=depth + 1, nodes=nodes, max_nodes=max_nodes)
    elif isinstance(value, list):
        for item in value:
            _check_structure(item, depth=depth + 1, nodes=nodes, max_nodes=max_nodes)


def main() -> int:
    lease_token = _required_environment("CHECKPOINT_RETENTION_LEASE_TOKEN")
    operator_token = _required_environment("CHECKPOINT_RETENTION_OPERATOR_TOKEN")
    if hmac.compare_digest(lease_token, operator_token):
        raise ServiceFailure("configuration_invalid")
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
        server = create_server(
            ("0.0.0.0", 8080),
            RetentionApplication(runtime, lease_token=lease_token, operator_token=operator_token),
        )
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
