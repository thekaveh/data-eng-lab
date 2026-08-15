"""Pure, read-only checkpoint retention planning and local artifact writes."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scripts.checkpoints.policy import (
    CheckpointPolicy,
    EvaluationInput,
    InventorySummary,
    LeaseFacts,
    PolicyError,
    TerminalFacts,
    evaluate_retention,
)
from scripts.checkpoints.records import (
    PlanArtifact,
    RecordFailure,
    canonical_json_bytes,
    decode_exact_json,
    inventory_sha256,
    shard_inventory,
)
from scripts.checkpoints.s3_gateway import GatewayFailure, S3Gateway


class PlanFailure(ValueError):
    """A closed, sanitized dry-run planning failure."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class PlanRequest:
    checkpoint_id: str
    prefix: str
    actor: str
    evaluated_at: datetime


_ACTOR = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
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
_TERMINAL_SCHEMA = {
    "checkpoint_id": str,
    "exclusive_run": bool,
    "generation": dict,
    "occurred_at": str,
    "prefix": str,
    "recovery_approved": bool,
    "schema_version": int,
    "sink_disposition_approved": bool,
    "source_available": bool,
    "state": str,
    "successful": bool,
}
_RETIRED_TERMINAL_SCHEMA = {**_TERMINAL_SCHEMA, "retirement_review": str}
_MAX_PLAN_NODES = 600_128


class RetentionPlanner:
    """Inventory and evaluate one exact checkpoint without any remote write."""

    def __init__(self, gateway: S3Gateway, policy: CheckpointPolicy) -> None:
        self._gateway = gateway
        self._policy = policy

    def plan(self, request: PlanRequest) -> PlanArtifact:
        matched = self._validate_request(request)
        try:
            records = self._gateway.inventory(request.prefix)
        except GatewayFailure as error:
            if error.code == "operation_deadline":
                raise
            raise PlanFailure("inventory_failed") from None
        if not records:
            raise PlanFailure("inventory_empty")
        lease = self._optional_control(
            f"_retention/leases/{request.checkpoint_id}.json",
            lambda body, etag: _decode_lease(body, etag),
        )
        terminal = (
            None
            if lease is not None and lease.state == "active"
            else self._optional_control(
                f"_retention/terminals/{request.checkpoint_id}.json",
                lambda body, _etag: _decode_terminal(body, request.checkpoint_id, request.prefix),
            )
        )
        newest = max(record.last_modified for record in records)
        inventory = InventorySummary(
            object_count=len(records),
            total_bytes=sum(record.size_bytes for record in records),
            newest_last_modified=newest,
            inventory_sha256=inventory_sha256(records),
        )
        facts = EvaluationInput(request.prefix, request.evaluated_at, lease, terminal, inventory)
        decision = evaluate_retention(self._policy, facts)
        shards = shard_inventory(records, self._policy.bounds.max_manifest_shard_bytes)
        prefix_sha = hashlib.sha256(request.prefix.encode("ascii")).hexdigest()
        summary = {
            "actor": request.actor,
            "checkpoint_id": matched.checkpoint_id,
            "decision": "eligible" if decision.eligible else "refused",
            "eligible_after": _optional_utc(decision.eligible_after),
            "evaluated_at": _format_utc(request.evaluated_at),
            "inventory": {
                "newest_last_modified": _format_utc(newest),
                "object_count": len(records),
                "sha256": inventory.inventory_sha256,
                "total_bytes": inventory.total_bytes,
            },
            "manifest_shards": tuple(shard.sha256 for shard in shards),
            "policy_sha256": decision.policy_sha256,
            "prefix": request.prefix,
            "prefix_sha256": prefix_sha,
            "refusal_codes": decision.refusal_codes,
            "retention_anchor": _optional_utc(decision.retention_anchor),
            "schema_version": 1,
        }
        artifact_value = {
            "schema_version": 1,
            "summary": summary,
            "shards": [json.loads(shard.body) for shard in shards],
        }
        try:
            body = canonical_json_bytes(
                artifact_value,
                max_bytes=128 * 1024 * 1024,
                max_nodes=_MAX_PLAN_NODES,
            )
        except RecordFailure:
            raise PlanFailure("plan_body_invalid") from None
        return PlanArtifact(summary, shards, body, hashlib.sha256(body).hexdigest())

    def _optional_control(self, key: str, decode):
        try:
            body, etag = self._gateway.read_control(key, max_bytes=self._policy.bounds.max_summary_bytes)
        except GatewayFailure as error:
            if error.code == "control_missing":
                return None
            if error.code == "operation_deadline":
                raise
            raise PlanFailure("control_read_failed") from None
        return decode(body, etag)

    def _validate_request(self, request: PlanRequest):
        if not isinstance(request, PlanRequest):
            raise PlanFailure("request_invalid")
        try:
            matched = self._policy.match_prefix(request.prefix)
        except PolicyError:
            raise PlanFailure("identity_invalid") from None
        if request.checkpoint_id != matched.checkpoint_id:
            raise PlanFailure("identity_invalid")
        if not isinstance(request.actor, str) or _ACTOR.fullmatch(request.actor) is None:
            raise PlanFailure("actor_invalid")
        if not _exact_utc(request.evaluated_at):
            raise PlanFailure("clock_invalid")
        return matched


def write_plan_exclusive(path: Path, artifact: PlanArtifact) -> None:
    """Atomically create one mode-0600 local artifact without overwriting."""

    if not isinstance(path, Path) or not isinstance(artifact, PlanArtifact):
        raise PlanFailure("plan_write_invalid")
    parent = path.parent
    if path.exists():
        raise PlanFailure("plan_target_exists")
    temporary: Path | None = None
    try:
        descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=parent)
        temporary = Path(name)
        os.fchmod(descriptor, 0o600)
        try:
            with os.fdopen(descriptor, "wb", closefd=True) as handle:
                handle.write(artifact.body)
                handle.flush()
                os.fsync(handle.fileno())
        except BaseException:
            try:
                os.close(descriptor)
            except OSError:
                pass
            raise
        try:
            os.link(temporary, path)
        except FileExistsError:
            raise PlanFailure("plan_target_exists") from None
        directory = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except PlanFailure:
        raise
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        raise PlanFailure("plan_write_failed") from None
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass


def _decode_lease(body: bytes, etag: str) -> LeaseFacts:
    try:
        value = decode_exact_json(body, _LEASE_SCHEMA)
        if value["schema_version"] != 1:
            raise ValueError
        acquired = _parse_utc(value["acquired_at"])
        heartbeat = _parse_utc(value["heartbeat_at"])
        expires = _parse_utc(value["expires_at"])
        if None in (acquired, heartbeat, expires):
            raise ValueError
        return LeaseFacts(
            checkpoint_id=value["checkpoint_id"],
            prefix=value["prefix"],
            state=value["state"],
            acquired_at=acquired,
            heartbeat_at=heartbeat,
            expires_at=expires,
            etag=etag,
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError, RecordFailure):
        raise PlanFailure("lease_malformed") from None


def _decode_terminal(body: bytes, checkpoint_id: str, prefix: str) -> TerminalFacts:
    try:
        try:
            value = decode_exact_json(body, _TERMINAL_SCHEMA)
        except RecordFailure:
            value = decode_exact_json(body, _RETIRED_TERMINAL_SCHEMA)
        if value["schema_version"] != 1:
            raise ValueError
        occurred = _parse_utc(value["occurred_at"])
        if occurred is None:
            raise ValueError
        if value["checkpoint_id"] != checkpoint_id or value["prefix"] != prefix:
            raise PlanFailure("terminal_identity_mismatch")
        return TerminalFacts(
            state=value["state"],
            occurred_at=occurred,
            recovery_approved=value["recovery_approved"],
            source_available=value["source_available"],
            sink_disposition_approved=value["sink_disposition_approved"],
            retirement_review=value.get("retirement_review"),
            generation=value["generation"],
            exclusive_run=value["exclusive_run"],
            successful=value["successful"],
        )
    except PlanFailure:
        raise
    except (KeyError, TypeError, ValueError, json.JSONDecodeError, RecordFailure):
        raise PlanFailure("terminal_malformed") from None


def _parse_utc(value: object) -> datetime | None:
    if not isinstance(value, str) or re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", value) is None:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _optional_utc(value: datetime | None) -> str | None:
    return _format_utc(value) if value is not None else None


def _exact_utc(value: object) -> bool:
    return (
        isinstance(value, datetime)
        and value.tzinfo is not None
        and value.utcoffset() == timedelta(0)
        and value.microsecond == 0
    )
