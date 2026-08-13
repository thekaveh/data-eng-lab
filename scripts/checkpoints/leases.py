"""Single-replica writer lease lifecycle for streaming checkpoints."""

from __future__ import annotations

import re
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import MappingProxyType
from typing import Callable, Mapping

from scripts.checkpoints.policy import CheckpointPolicy, PolicyError
from scripts.checkpoints.records import canonical_json_bytes, decode_exact_json
from scripts.checkpoints.s3_gateway import GatewayFailure, S3Gateway


class LeaseFailure(ValueError):
    """A closed, sanitized lease lifecycle failure."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class AcquireRequest:
    checkpoint_id: str
    prefix: str
    workload: str
    owner_id: str
    session_id: str


@dataclass(frozen=True)
class HeartbeatRequest:
    checkpoint_id: str
    prefix: str
    epoch: str


@dataclass(frozen=True)
class TerminalRequest:
    checkpoint_id: str
    prefix: str
    epoch: str
    state: str
    evidence: Mapping[str, object]

    def __post_init__(self) -> None:
        if isinstance(self.evidence, Mapping):
            object.__setattr__(self, "evidence", _freeze_mapping(self.evidence))


@dataclass(frozen=True)
class LeaseResult:
    epoch: str
    etag: str
    body: bytes


_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}")
_IDENTITY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
_LEASE_SCHEMA = {
    "acquired_at": str,
    "checkpoint_id": str,
    "epoch": str,
    "expires_at": str,
    "heartbeat_at": str,
    "owner_id": str,
    "prefix": str,
    "schema_version": int,
    "session_id": str,
    "state": str,
    "terminal_evidence": (dict, type(None)),
    "workload": str,
}


class CheckpointLockRegistry:
    """One process-local re-entrant lock per exact checkpoint identity."""

    def __init__(self) -> None:
        self._guard = threading.Lock()
        self._values: dict[str, threading.RLock] = {}

    @contextmanager
    def hold(self, checkpoint_id: str):
        if not isinstance(checkpoint_id, str) or _IDENTITY.fullmatch(checkpoint_id) is None:
            raise LeaseFailure("lease_identity_mismatch")
        with self._guard:
            lock = self._values.setdefault(checkpoint_id, threading.RLock())
        with lock:
            yield


class LeaseManager:
    """Serialize and validate all lease transitions in one service process."""

    def __init__(
        self,
        gateway: S3Gateway,
        policy: CheckpointPolicy,
        *,
        now: Callable[[], datetime],
        uuid_factory: Callable[[], str] = lambda: str(uuid.uuid4()),
        locks: CheckpointLockRegistry | None = None,
    ) -> None:
        self._gateway = gateway
        self._policy = policy
        self._now = now
        self._uuid_factory = uuid_factory
        self._locks = locks or CheckpointLockRegistry()

    def acquire(self, request: AcquireRequest) -> LeaseResult:
        matched = self._validate_acquire(request)
        with self._locks.hold(request.checkpoint_id):
            now = self._exact_now()
            key = self._lease_key(request.checkpoint_id)
            existing: tuple[dict[str, object], str] | None
            try:
                body, etag = self._gateway.read_control(key, max_bytes=self._policy.bounds.max_summary_bytes)
            except GatewayFailure as error:
                if error.code != "control_missing":
                    raise LeaseFailure("lease_read_failed") from None
                existing = None
            else:
                existing = (self._decode_lease(body), etag)
            replace_etag: str | None = None
            if existing is not None:
                current, replace_etag = existing
                expires = _parse_utc(current["expires_at"])
                heartbeat = _parse_utc(current["heartbeat_at"])
                if heartbeat is None or expires is None:
                    raise LeaseFailure("lease_malformed")
                if heartbeat > now + timedelta(seconds=self._policy.lease.future_tolerance_seconds):
                    raise LeaseFailure("lease_future_clock")
                if current["state"] == "active":
                    self._require_identity(current, matched.checkpoint_id, request.prefix)
                    code = "lease_active" if expires >= now else "lease_expired_active_uncertain"
                    raise LeaseFailure(code)
                try:
                    prior = self._policy.match_prefix(current["prefix"])
                except (PolicyError, TypeError):
                    raise LeaseFailure("lease_identity_mismatch") from None
                if prior.checkpoint_id != matched.checkpoint_id:
                    raise LeaseFailure("lease_identity_mismatch")
            epoch = self._uuid_factory()
            if not isinstance(epoch, str) or _UUID.fullmatch(epoch) is None:
                raise LeaseFailure("epoch_invalid")
            value = {
                "acquired_at": _format_utc(now),
                "checkpoint_id": matched.checkpoint_id,
                "epoch": epoch,
                "expires_at": _format_utc(now + timedelta(seconds=self._policy.lease.ttl_seconds)),
                "heartbeat_at": _format_utc(now),
                "owner_id": request.owner_id,
                "prefix": request.prefix,
                "schema_version": 1,
                "session_id": request.session_id,
                "state": "active",
                "terminal_evidence": None,
                "workload": request.workload,
            }
            body = canonical_json_bytes(value, max_bytes=self._policy.bounds.max_summary_bytes)
            try:
                etag = (
                    self._gateway.create_control(key, body)
                    if replace_etag is None
                    else self._gateway.replace_lease(key, replace_etag, body)
                )
            except GatewayFailure:
                raise LeaseFailure("lease_create_failed" if replace_etag is None else "lease_replace_failed") from None
            return LeaseResult(epoch, etag, body)

    def heartbeat(self, request: HeartbeatRequest) -> LeaseResult:
        return self._transition(request, state="active", evidence=None)

    def terminal(self, request: TerminalRequest) -> LeaseResult:
        matched = self._validate_identity_request(request.checkpoint_id, request.prefix, request.epoch)
        entry = self._policy.entries[matched.checkpoint_id]
        expected_states = {
            "durable_stream": {"stopped", "retired"},
            "generation_reproducibility": {"completed", "stopped"},
            "disposable_acceptance": {"stopped"},
        }[entry.durability]
        if request.state not in expected_states:
            raise LeaseFailure("terminal_state_invalid")
        generation = request.evidence.get("generation") if isinstance(request.evidence, Mapping) else None
        if not isinstance(generation, Mapping) or dict(generation) != dict(matched.generation):
            raise LeaseFailure("generation_identity_mismatch")
        if entry.durability == "disposable_acceptance" and (
            request.evidence.get("successful") is not True or request.evidence.get("exclusive_run") is not True
        ):
            raise LeaseFailure("terminal_evidence_invalid")
        key = self._lease_key(request.checkpoint_id)
        try:
            current_body, current_etag = self._gateway.read_control(
                key, max_bytes=self._policy.bounds.max_summary_bytes
            )
        except GatewayFailure:
            raise LeaseFailure("lease_read_failed") from None
        current = self._decode_lease(current_body)
        self._require_identity(current, request.checkpoint_id, request.prefix)
        if current["epoch"] != request.epoch:
            raise LeaseFailure("lease_identity_mismatch")
        if current["state"] == "active":
            result = self._transition(request, state=request.state, evidence=request.evidence)
        elif current["state"] == request.state and current["terminal_evidence"] == dict(request.evidence):
            result = LeaseResult(request.epoch, current_etag, current_body)
        else:
            raise LeaseFailure("lease_not_active")
        self._persist_terminal(request, result.body)
        return result

    def _persist_terminal(self, request: TerminalRequest, lease_body: bytes) -> None:
        lease = self._decode_lease(lease_body)
        evidence = request.evidence
        value = {
            "checkpoint_id": request.checkpoint_id,
            "exclusive_run": evidence.get("exclusive_run", False),
            "generation": dict(evidence["generation"]),
            "occurred_at": lease["heartbeat_at"],
            "prefix": request.prefix,
            "recovery_approved": evidence.get("recovery_approved", False),
            "schema_version": 1,
            "sink_disposition_approved": evidence.get("sink_disposition_approved", False),
            "source_available": evidence.get("source_available", False),
            "state": request.state,
            "successful": evidence.get("successful", False),
        }
        if any(
            type(value[field]) is not bool
            for field in {
                "exclusive_run",
                "recovery_approved",
                "sink_disposition_approved",
                "source_available",
                "successful",
            }
        ):
            raise LeaseFailure("terminal_evidence_invalid")
        body = canonical_json_bytes(value, max_bytes=self._policy.bounds.max_summary_bytes)
        key = f"_retention/terminals/{request.checkpoint_id}.json"
        try:
            try:
                existing, etag = self._gateway.read_control(key, max_bytes=self._policy.bounds.max_summary_bytes)
            except GatewayFailure as error:
                if error.code != "control_missing":
                    raise
                self._gateway.create_control(key, body)
            else:
                if existing != body:
                    self._gateway.replace_lease(key, etag, body)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise LeaseFailure("terminal_record_failed") from None

    def _transition(
        self,
        request: HeartbeatRequest | TerminalRequest,
        *,
        state: str,
        evidence: Mapping[str, object] | None,
    ) -> LeaseResult:
        self._validate_identity_request(request.checkpoint_id, request.prefix, request.epoch)
        with self._locks.hold(request.checkpoint_id):
            now = self._exact_now()
            key = self._lease_key(request.checkpoint_id)
            try:
                body, etag = self._gateway.read_control(key, max_bytes=self._policy.bounds.max_summary_bytes)
            except GatewayFailure:
                raise LeaseFailure("lease_read_failed") from None
            current = self._decode_lease(body)
            self._require_identity(current, request.checkpoint_id, request.prefix)
            if current["epoch"] != request.epoch:
                raise LeaseFailure("lease_identity_mismatch")
            if current["state"] != "active":
                raise LeaseFailure("lease_not_active")
            current["heartbeat_at"] = _format_utc(now)
            current["expires_at"] = _format_utc(now + timedelta(seconds=self._policy.lease.ttl_seconds))
            current["state"] = state
            current["terminal_evidence"] = evidence
            next_body = canonical_json_bytes(current, max_bytes=self._policy.bounds.max_summary_bytes)
            try:
                next_etag = self._gateway.replace_lease(key, etag, next_body)
            except GatewayFailure:
                raise LeaseFailure("lease_ownership_lost") from None
            return LeaseResult(request.epoch, next_etag, next_body)

    def _validate_acquire(self, request: AcquireRequest):
        if not isinstance(request, AcquireRequest):
            raise LeaseFailure("request_invalid")
        matched = self._validate_identity_request(request.checkpoint_id, request.prefix, None)
        if self._policy.entries[matched.checkpoint_id].workload != request.workload:
            raise LeaseFailure("lease_identity_mismatch")
        for value in (request.owner_id, request.session_id):
            if not isinstance(value, str) or _IDENTITY.fullmatch(value) is None:
                raise LeaseFailure("request_invalid")
        return matched

    def _validate_identity_request(self, checkpoint_id: str, prefix: str, epoch: str | None):
        try:
            matched = self._policy.match_prefix(prefix)
        except PolicyError:
            raise LeaseFailure("lease_identity_mismatch") from None
        if checkpoint_id != matched.checkpoint_id:
            raise LeaseFailure("lease_identity_mismatch")
        if epoch is not None and (not isinstance(epoch, str) or _UUID.fullmatch(epoch) is None):
            raise LeaseFailure("lease_identity_mismatch")
        return matched

    def _decode_lease(self, body: bytes) -> dict[str, object]:
        try:
            value = decode_exact_json(body, _LEASE_SCHEMA, max_bytes=self._policy.bounds.max_summary_bytes)
        except ValueError:
            raise LeaseFailure("lease_malformed") from None
        if (
            value["schema_version"] != 1
            or value["state"] not in {"active", "stopped", "completed", "retired"}
            or not isinstance(value["epoch"], str)
            or _UUID.fullmatch(value["epoch"]) is None
            or any(_parse_utc(value[key]) is None for key in ("acquired_at", "heartbeat_at", "expires_at"))
            or not isinstance(value["workload"], str)
            or not isinstance(value["owner_id"], str)
            or not isinstance(value["session_id"], str)
        ):
            raise LeaseFailure("lease_malformed")
        acquired = _parse_utc(value["acquired_at"])
        heartbeat = _parse_utc(value["heartbeat_at"])
        expires = _parse_utc(value["expires_at"])
        if not acquired <= heartbeat <= expires or expires - heartbeat != timedelta(
            seconds=self._policy.lease.ttl_seconds
        ):
            raise LeaseFailure("lease_malformed")
        return value

    @staticmethod
    def _require_identity(current: Mapping[str, object], checkpoint_id: str, prefix: str) -> None:
        if current.get("checkpoint_id") != checkpoint_id or current.get("prefix") != prefix:
            raise LeaseFailure("lease_identity_mismatch")

    def _exact_now(self) -> datetime:
        try:
            value = self._now()
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise LeaseFailure("clock_failed") from None
        if (
            not isinstance(value, datetime)
            or value.tzinfo is None
            or value.utcoffset() != timedelta(0)
            or value.microsecond != 0
        ):
            raise LeaseFailure("clock_invalid")
        return value

    @staticmethod
    def _lease_key(checkpoint_id: str) -> str:
        return f"_retention/leases/{checkpoint_id}.json"


def _format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_utc(value: object) -> datetime | None:
    if not isinstance(value, str) or re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", value) is None:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _freeze_mapping(value: Mapping[str, object]) -> Mapping[str, object]:
    result: dict[str, object] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise LeaseFailure("terminal_evidence_invalid")
        if isinstance(item, Mapping):
            result[key] = _freeze_mapping(item)
        elif isinstance(item, (list, tuple)):
            result[key] = tuple(item)
        elif item is None or type(item) in (bool, int, str):
            result[key] = item
        else:
            raise LeaseFailure("terminal_evidence_invalid")
    return MappingProxyType(result)
