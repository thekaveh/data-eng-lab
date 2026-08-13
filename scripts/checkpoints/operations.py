"""Immutable checkpoint-retention operation state transitions."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable

from scripts.checkpoints.records import ObjectRecord, PlanArtifact, RecordFailure, canonical_json_bytes


class OperationFailure(ValueError):
    """A bounded operation-state failure category."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class PrepareRequest:
    operation_id: str
    artifact: PlanArtifact
    plan_sha256: str
    review: str
    actor: str


@dataclass(frozen=True)
class ApplyRequest:
    operation_id: str
    plan_sha256: str
    confirm_prefix: str


@dataclass(frozen=True)
class OperationStatus:
    operation_id: str
    state: str
    body: bytes


_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
_SHA256 = re.compile(r"[0-9a-f]{64}")


class OperationManager:
    """Prepare immutable evidence and apply only its original object set."""

    def __init__(
        self,
        gateway: object,
        *,
        policy_sha256: str,
        now: Callable[[], datetime],
        quiescence_seconds: int = 900,
        max_summary_bytes: int = 65_536,
        max_delete_keys: int = 1_000,
        revalidate: Callable[[str, datetime], PlanArtifact] | None = None,
    ) -> None:
        if not isinstance(policy_sha256, str) or _SHA256.fullmatch(policy_sha256) is None:
            raise OperationFailure("policy_digest_invalid")
        self._gateway = gateway
        self._policy_sha256 = policy_sha256
        self._now = now
        self._quiescence_seconds = quiescence_seconds
        self._max_summary_bytes = max_summary_bytes
        self._max_delete_keys = max_delete_keys
        self._revalidate = revalidate
        self._statuses: dict[str, OperationStatus] = {}

    def prepare(self, request: PrepareRequest) -> OperationStatus:
        artifact = self._validate_prepare(request)
        summary = artifact.summary
        base = f"_retention/tombstones/{request.operation_id}"
        for shard in artifact.shards:
            key = f"{base}/manifest/{shard.index}-{shard.sha256}.json"
            self._create_identical(key, shard.body, len(shard.body))
        prepared = {
            "actor": request.actor,
            "checkpoint_id": summary["checkpoint_id"],
            "evaluated_at": summary["evaluated_at"],
            "inventory_sha256": summary["inventory"]["sha256"],
            "manifest_shards": tuple(shard.sha256 for shard in artifact.shards),
            "operation_id": request.operation_id,
            "plan_sha256": request.plan_sha256,
            "policy_sha256": summary["policy_sha256"],
            "prefix": summary["prefix"],
            "prefix_sha256": summary["prefix_sha256"],
            "prepared_at": _format_utc(self._exact_now()),
            "review": request.review,
            "schema_version": 1,
        }
        try:
            body = canonical_json_bytes(prepared, max_bytes=self._max_summary_bytes)
        except RecordFailure:
            raise OperationFailure("prepared_body_invalid") from None
        self._create_identical(f"{base}/prepared.json", body, self._max_summary_bytes)
        return OperationStatus(request.operation_id, "prepared", body)

    def apply(self, request: ApplyRequest) -> OperationStatus:
        if not isinstance(request, ApplyRequest) or _UUID.fullmatch(request.operation_id or "") is None:
            raise OperationFailure("operation_id_invalid")
        if _SHA256.fullmatch(request.plan_sha256 or "") is None:
            raise OperationFailure("plan_digest_mismatch")
        base = f"_retention/tombstones/{request.operation_id}"
        prepared_body = self._read_control(f"{base}/prepared.json", self._max_summary_bytes, "prepared_read_failed")
        prepared = self._decode_prepared(prepared_body, request.operation_id)
        if request.plan_sha256 != prepared["plan_sha256"] or request.confirm_prefix != prepared["prefix"]:
            raise OperationFailure("confirmation_mismatch")
        prepared_at = _parse_utc(prepared["prepared_at"])
        evaluated_at = _parse_utc(prepared.get("evaluated_at"))
        now = self._exact_now()
        if prepared_at is None or evaluated_at is None:
            raise OperationFailure("prepared_invalid")
        if now < prepared_at + timedelta(seconds=self._quiescence_seconds):
            body = canonical_json_bytes(
                {
                    "operation_id": request.operation_id,
                    "plan_sha256": request.plan_sha256,
                    "schema_version": 1,
                    "state": "not_ready",
                },
                max_bytes=self._max_summary_bytes,
            )
            return OperationStatus(request.operation_id, "not_ready", body)
        records = self._read_manifest(base, prepared["manifest_shards"])
        prior_status = self._statuses.get(request.operation_id)
        prior_deleted: set[str] = set()
        if prior_status is not None:
            try:
                prior_value = json.loads(prior_status.body)
                if prior_value["state"] == "completed":
                    return prior_status
                prior_deleted = set(prior_value["deleted_record_sha256s"])
            except (KeyError, TypeError, json.JSONDecodeError):
                raise OperationFailure("status_invalid") from None
        remaining_records = tuple(record for record in records if _record_sha256(record) not in prior_deleted)
        if self._revalidate is None:
            raise OperationFailure("revalidation_unavailable")
        try:
            current = self._revalidate(request.confirm_prefix, evaluated_at)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise OperationFailure("revalidation_failed") from None
        if (
            not isinstance(current, PlanArtifact)
            or current.summary.get("decision") != "eligible"
            or current.summary.get("policy_sha256") != self._policy_sha256
            or current.summary.get("prefix") != request.confirm_prefix
            or current.summary.get("inventory", {}).get("sha256") != prepared["inventory_sha256"]
            or tuple(shard.sha256 for shard in current.shards) != tuple(prepared["manifest_shards"])
        ):
            raise OperationFailure("revalidation_mismatch")
        try:
            for record in remaining_records:
                self._gateway.head_record(record)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise OperationFailure("head_mismatch") from None
        deleted: list[ObjectRecord] = [record for record in records if _record_sha256(record) in prior_deleted]
        try:
            for offset in range(0, len(remaining_records), self._max_delete_keys):
                batch = remaining_records[offset : offset + self._max_delete_keys]
                self._gateway.delete_records(batch)
                deleted.extend(batch)
            remaining = self._gateway.inventory(request.confirm_prefix)
            if remaining:
                raise OperationFailure("postflight_not_empty")
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException as error:
            primary = error if isinstance(error, OperationFailure) else OperationFailure("delete_partial")
            self._record_status(request.operation_id, "partial", request.plan_sha256, deleted, records)
            raise primary from None
        status = self._record_status(request.operation_id, "completed", request.plan_sha256, deleted, records)
        audit_body = canonical_json_bytes(
            {
                "attempt_id": request.operation_id,
                "decision": "completed",
                "deleted_objects": len(deleted),
                "operation_id": request.operation_id,
                "plan_sha256": request.plan_sha256,
                "schema_version": 1,
            },
            max_bytes=self._max_summary_bytes,
        )
        self._create_identical(
            f"_retention/audits/{request.operation_id}/{request.operation_id}.json",
            audit_body,
            self._max_summary_bytes,
        )
        return status

    def status(self, operation_id: str) -> OperationStatus:
        if _UUID.fullmatch(operation_id or "") is None:
            raise OperationFailure("operation_id_invalid")
        status = self._statuses.get(operation_id)
        if status is not None:
            return status
        body = self._read_control(
            f"_retention/tombstones/{operation_id}/status.json",
            self._max_summary_bytes,
            "status_missing",
        )
        try:
            value = json.loads(body)
            state = value["state"]
        except (KeyError, TypeError, json.JSONDecodeError):
            raise OperationFailure("status_invalid") from None
        if state not in {"partial", "completed", "refused"}:
            raise OperationFailure("status_invalid")
        return OperationStatus(operation_id, state, body)

    def _read_manifest(self, base: str, shard_sha256s: object) -> tuple[ObjectRecord, ...]:
        invalid_digests = not isinstance(shard_sha256s, list) or any(
            _SHA256.fullmatch(value or "") is None for value in shard_sha256s
        )
        if invalid_digests:
            raise OperationFailure("prepared_invalid")
        records: list[ObjectRecord] = []
        for index, digest in enumerate(shard_sha256s):
            body = self._read_control(
                f"{base}/manifest/{index}-{digest}.json",
                1_048_576,
                "manifest_read_failed",
            )
            if hashlib.sha256(body).hexdigest() != digest:
                raise OperationFailure("manifest_digest_mismatch")
            try:
                values = json.loads(body)
                if not isinstance(values, list):
                    raise ValueError
                for value in values:
                    records.append(
                        ObjectRecord(
                            value["key"],
                            value["etag"],
                            value["size_bytes"],
                            _parse_utc(value["last_modified"]),
                        )
                    )
            except (KeyError, TypeError, ValueError, json.JSONDecodeError, RecordFailure):
                raise OperationFailure("manifest_invalid") from None
        if not records:
            raise OperationFailure("manifest_empty")
        return tuple(records)

    def _decode_prepared(self, body: bytes, operation_id: str) -> dict[str, object]:
        try:
            value = json.loads(body)
            if value["schema_version"] != 1 or value["operation_id"] != operation_id:
                raise ValueError
            if value["policy_sha256"] != self._policy_sha256:
                raise OperationFailure("policy_drift")
            return value
        except OperationFailure:
            raise
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            raise OperationFailure("prepared_invalid") from None

    def _read_control(self, key: str, bound: int, code: str) -> bytes:
        try:
            body, _etag = self._gateway.read_control(key, max_bytes=bound)
            return body
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise OperationFailure(code) from None

    def _record_status(
        self,
        operation_id: str,
        state: str,
        plan_sha256: str,
        deleted: list[ObjectRecord],
        planned: tuple[ObjectRecord, ...],
    ):
        body = canonical_json_bytes(
            {
                "deleted_objects": len(deleted),
                "deleted_record_sha256s": tuple(_record_sha256(record) for record in deleted),
                "operation_id": operation_id,
                "plan_sha256": plan_sha256,
                "planned_objects": len(planned),
                "schema_version": 1,
                "state": state,
            },
            max_bytes=self._max_summary_bytes,
        )
        status = OperationStatus(operation_id, state, body)
        self._statuses[operation_id] = status
        key = f"_retention/tombstones/{operation_id}/status.json"
        try:
            try:
                _current, etag = self._gateway.read_control(key, max_bytes=self._max_summary_bytes)
            except BaseException:
                self._create_identical(key, body, self._max_summary_bytes)
            else:
                self._gateway.replace_lease(key, etag, body)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            pass
        return status

    def _validate_prepare(self, request: PrepareRequest) -> PlanArtifact:
        if not isinstance(request, PrepareRequest) or _UUID.fullmatch(request.operation_id or "") is None:
            raise OperationFailure("operation_id_invalid")
        if not isinstance(request.artifact, PlanArtifact):
            raise OperationFailure("plan_invalid")
        if request.plan_sha256 != request.artifact.sha256 or _SHA256.fullmatch(request.plan_sha256 or "") is None:
            raise OperationFailure("plan_digest_mismatch")
        if _IDENTIFIER.fullmatch(request.review or "") is None or _IDENTIFIER.fullmatch(request.actor or "") is None:
            raise OperationFailure("review_identity_invalid")
        artifact = request.artifact
        try:
            decoded = json.loads(artifact.body)
            if set(decoded) != {"schema_version", "summary", "shards"} or decoded["schema_version"] != 1:
                raise ValueError
            if canonical_json_bytes(decoded["summary"]) != canonical_json_bytes(artifact.summary):
                raise ValueError
            if len(decoded["shards"]) != len(artifact.shards):
                raise ValueError
            for value, shard in zip(decoded["shards"], artifact.shards):
                if canonical_json_bytes(value, max_bytes=len(shard.body)) != shard.body:
                    raise ValueError
                if hashlib.sha256(shard.body).hexdigest() != shard.sha256:
                    raise ValueError
        except (KeyError, TypeError, ValueError, json.JSONDecodeError, RecordFailure):
            raise OperationFailure("plan_invalid") from None
        if artifact.summary.get("decision") != "eligible":
            raise OperationFailure("plan_refused")
        if artifact.summary.get("policy_sha256") != self._policy_sha256:
            raise OperationFailure("policy_drift")
        if artifact.summary.get("actor") != request.actor:
            raise OperationFailure("actor_mismatch")
        return artifact

    def _create_identical(self, key: str, body: bytes, bound: int) -> None:
        try:
            self._gateway.create_control(key, body)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            try:
                existing, _etag = self._gateway.read_control(key, max_bytes=bound)
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException:
                raise OperationFailure("control_create_failed") from None
            if existing != body:
                raise OperationFailure("control_conflict")
        try:
            readback, _etag = self._gateway.read_control(key, max_bytes=bound)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise OperationFailure("control_readback_failed") from None
        if readback != body:
            raise OperationFailure("control_readback_mismatch")

    def _exact_now(self) -> datetime:
        try:
            value = self._now()
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise OperationFailure("clock_failed") from None
        if (
            not isinstance(value, datetime)
            or value.tzinfo is None
            or value.utcoffset() != timedelta(0)
            or value.microsecond != 0
        ):
            raise OperationFailure("clock_invalid")
        return value


def _format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_utc(value: object) -> datetime | None:
    if not isinstance(value, str) or re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", value) is None:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _record_sha256(record: ObjectRecord) -> str:
    return hashlib.sha256(canonical_json_bytes(record.as_json())).hexdigest()
