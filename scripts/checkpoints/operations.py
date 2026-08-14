"""Immutable checkpoint-retention operation state transitions."""

from __future__ import annotations

import hashlib
import json
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable

from scripts.checkpoints.records import (
    ObjectRecord,
    PlanArtifact,
    RecordFailure,
    canonical_json_bytes,
    inventory_sha256,
)
from scripts.checkpoints.s3_gateway import GatewayFailure


class OperationFailure(ValueError):
    """A bounded operation-state failure category."""

    def __init__(self, code: str, *, partial: bool = False) -> None:
        super().__init__(code)
        self.code = code
        self.partial = partial


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
_DISPOSABLE_PREFIX = re.compile(
    r"streaming_test/[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/"
)
_STATUS_PRIMARY_CATEGORIES = {
    "not_ready": {"quiescence_not_ready"},
    "partial": {"delete_partial", "head_mismatch", "operation_deadline", "postflight_not_empty"},
    "refused": {
        "delete_partial",
        "head_mismatch",
        "operation_deadline",
        "revalidation_failed",
        "revalidation_mismatch",
    },
}


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
        max_result_shard_bytes: int = 1_048_576,
        max_delete_keys: int = 1_000,
        revalidate: Callable[[str, datetime], PlanArtifact] | None = None,
        locks: object | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        max_active_seconds: int = 900,
    ) -> None:
        if not isinstance(policy_sha256, str) or _SHA256.fullmatch(policy_sha256) is None:
            raise OperationFailure("policy_digest_invalid")
        self._gateway = gateway
        self._policy_sha256 = policy_sha256
        self._now = now
        self._quiescence_seconds = quiescence_seconds
        self._max_summary_bytes = max_summary_bytes
        self._max_result_shard_bytes = max_result_shard_bytes
        self._max_delete_keys = max_delete_keys
        self._revalidate = revalidate
        self._locks = locks
        self._monotonic = monotonic
        self._max_active_seconds = max_active_seconds

    def prepare(self, request: PrepareRequest) -> OperationStatus:
        started = self._start_deadline()
        artifact = self._validate_prepare(request)
        summary = artifact.summary
        base = f"_retention/tombstones/{request.operation_id}"
        prepared_key = f"{base}/prepared.json"
        for shard in artifact.shards:
            key = f"{base}/manifest/{shard.index}-{shard.sha256}.json"
            self._create_identical(key, shard.body, len(shard.body))
        prepared_identity = {
            "actor": request.actor,
            "checkpoint_id": summary["checkpoint_id"],
            "evaluated_at": summary["evaluated_at"],
            "inventory_sha256": summary["inventory"]["sha256"],
            "manifest_shards": [shard.sha256 for shard in artifact.shards],
            "operation_id": request.operation_id,
            "plan_sha256": request.plan_sha256,
            "policy_sha256": summary["policy_sha256"],
            "prefix": summary["prefix"],
            "prefix_sha256": summary["prefix_sha256"],
            "review": request.review,
            "schema_version": 1,
        }
        records = tuple(record for shard in artifact.shards for record in shard.records)
        prepared = self._existing_prepared(prepared_key, request.operation_id)
        if prepared is not None:
            if {key: value for key, value in prepared.items() if key != "prepared_at"} != prepared_identity:
                raise OperationFailure("control_conflict")
            prior = self._latest_status(
                request.operation_id,
                required=False,
                prepared=prepared,
                records=records,
            )
            if prior is not None:
                return prior
        else:
            prepared = {**prepared_identity, "prepared_at": _format_utc(self._exact_now())}
            try:
                body = canonical_json_bytes(prepared, max_bytes=self._max_summary_bytes)
            except RecordFailure:
                raise OperationFailure("prepared_body_invalid") from None
            self._create_identical(prepared_key, body, self._max_summary_bytes)
        status = self._record_status(
            request.operation_id,
            prepared["checkpoint_id"],
            "prepared",
            request.plan_sha256,
            [],
            records,
            None,
            started,
            head_requests=0,
            delete_requests=0,
            postflight_inventory_sha256=None,
        )
        self._ensure_audit(request.operation_id, status, prepared)
        return status

    def apply(self, request: ApplyRequest) -> OperationStatus:
        started = self._start_deadline()
        deadline = getattr(self._gateway, "operation_deadline", None)
        if callable(deadline):
            with deadline(lambda: self.check_deadline(started)):
                return self._apply(request, started)
        return self._apply(request, started)

    def _apply(self, request: ApplyRequest, started: float) -> OperationStatus:
        if not isinstance(request, ApplyRequest) or _UUID.fullmatch(request.operation_id or "") is None:
            raise OperationFailure("operation_id_invalid")
        if _SHA256.fullmatch(request.plan_sha256 or "") is None:
            raise OperationFailure("plan_digest_mismatch")
        if not isinstance(request.confirm_prefix, str) or _DISPOSABLE_PREFIX.fullmatch(request.confirm_prefix) is None:
            raise OperationFailure("destructive_scope_invalid")
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
            records = self._read_bound_manifest(base, prepared)
            self._latest_status(
                request.operation_id,
                required=False,
                prepared=prepared,
                records=records,
            )
            status = self._record_status(
                request.operation_id,
                prepared["checkpoint_id"],
                "not_ready",
                request.plan_sha256,
                [],
                records,
                "quiescence_not_ready",
                started,
                head_requests=0,
                delete_requests=0,
                postflight_inventory_sha256=None,
            )
            self._ensure_audit(request.operation_id, status, prepared)
            return status
        checkpoint_id = prepared.get("checkpoint_id")
        if not isinstance(checkpoint_id, str) or _IDENTIFIER.fullmatch(checkpoint_id) is None:
            raise OperationFailure("prepared_invalid")
        if self._locks is None or not hasattr(self._locks, "hold"):
            return self._apply_locked(request, prepared, evaluated_at, base, started)
        with self._locks.hold(checkpoint_id):
            return self._apply_locked(request, prepared, evaluated_at, base, started)

    def _apply_locked(self, request, prepared, evaluated_at, base, started) -> OperationStatus:
        self.check_deadline(started)
        records = self._read_bound_manifest(base, prepared)
        self.check_deadline(started)
        prior_status = self._latest_status(
            request.operation_id,
            required=False,
            prepared=prepared,
            records=records,
        )
        prior_deleted: set[str] = set()
        if prior_status is not None:
            try:
                prior_value = json.loads(prior_status.body)
                if prior_value["state"] == "completed":
                    self._read_result_classification(base, prior_value, records)
                    remaining = self._gateway.inventory(request.confirm_prefix)
                    self.check_deadline(started)
                    if remaining or prior_value.get("postflight_inventory_sha256") != inventory_sha256(()):
                        raise OperationFailure("status_invalid")
                    self._ensure_audit(request.operation_id, prior_status, prepared)
                    return prior_status
                prior_deleted = {
                    digest
                    for digest, outcome in self._read_result_classification(base, prior_value, records).items()
                    if outcome == "deleted"
                }
            except (KeyError, TypeError, json.JSONDecodeError, RecordFailure):
                raise OperationFailure("status_invalid") from None
        remaining_records = tuple(record for record in records if _record_sha256(record) not in prior_deleted)
        if remaining_records:
            if self._revalidate is None:
                raise OperationFailure("revalidation_unavailable")
            try:
                current = self._revalidate(request.confirm_prefix, self._exact_now())
            except (KeyboardInterrupt, SystemExit):
                raise
            except (OperationFailure, GatewayFailure) as error:
                if error.code == "operation_deadline":
                    raise OperationFailure("operation_deadline") from None
                raise OperationFailure("revalidation_failed") from None
            except BaseException:
                status = self._record_status(
                    request.operation_id,
                    prepared["checkpoint_id"],
                    "refused",
                    request.plan_sha256,
                    [record for record in records if _record_sha256(record) in prior_deleted],
                    records,
                    "revalidation_failed",
                    started,
                    head_requests=0,
                    delete_requests=0,
                    postflight_inventory_sha256=None,
                )
                self._ensure_audit(request.operation_id, status, prepared)
                raise OperationFailure("revalidation_failed") from None
            self.check_deadline(started)
            current_records = (
                tuple(record for shard in current.shards for record in shard.records)
                if isinstance(current, PlanArtifact)
                else ()
            )
            if (
                not isinstance(current, PlanArtifact)
                or current.summary.get("decision") != "eligible"
                or current.summary.get("policy_sha256") != self._policy_sha256
                or current.summary.get("prefix") != request.confirm_prefix
                or current.summary.get("inventory", {}).get("sha256") != inventory_sha256(remaining_records)
                or current_records != remaining_records
            ):
                status = self._record_status(
                    request.operation_id,
                    prepared["checkpoint_id"],
                    "refused",
                    request.plan_sha256,
                    [record for record in records if _record_sha256(record) in prior_deleted],
                    records,
                    "revalidation_mismatch",
                    started,
                    head_requests=0,
                    delete_requests=0,
                    postflight_inventory_sha256=None,
                )
                self._ensure_audit(request.operation_id, status, prepared)
                raise OperationFailure("revalidation_mismatch")
        else:
            try:
                if self._gateway.inventory(request.confirm_prefix):
                    raise OperationFailure("revalidation_mismatch")
            except OperationFailure as error:
                status = self._record_status(
                    request.operation_id,
                    prepared["checkpoint_id"],
                    "refused",
                    request.plan_sha256,
                    list(records),
                    records,
                    error.code,
                    started,
                    head_requests=0,
                    delete_requests=0,
                    postflight_inventory_sha256=None,
                )
                self._ensure_audit(request.operation_id, status, prepared)
                raise
            except GatewayFailure as error:
                if error.code == "operation_deadline":
                    raise OperationFailure("operation_deadline") from None
                raise OperationFailure("revalidation_failed") from None
            except BaseException:
                raise OperationFailure("revalidation_failed") from None
        head_requests = 0
        deleted: list[ObjectRecord] = [record for record in records if _record_sha256(record) in prior_deleted]
        delete_requests = 0
        postflight_sha256: str | None = None
        current_batch: tuple[ObjectRecord, ...] = ()
        current_record: ObjectRecord | None = None
        try:
            for offset in range(0, len(remaining_records), self._max_delete_keys):
                self.check_deadline(started)
                current_batch = remaining_records[offset : offset + self._max_delete_keys]
                for record in current_batch:
                    current_record = record
                    head_requests += 1
                    self._gateway.head_record(record)
                    self.check_deadline(started)
                delete_requests += 1
                self._gateway.delete_records(current_batch)
                deleted.extend(current_batch)
                self.check_deadline(started)
            remaining = self._gateway.inventory(request.confirm_prefix)
            postflight_sha256 = inventory_sha256(remaining)
            self.check_deadline(started)
            if remaining:
                raise OperationFailure("postflight_not_empty")
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException as error:
            if isinstance(error, GatewayFailure) and error.deleted_keys:
                by_key = {record.key: record for record in remaining_records}
                if any(key not in by_key for key in error.deleted_keys):
                    raise OperationFailure("delete_response_invalid") from None
                deleted.extend(by_key[key] for key in error.deleted_keys)
            primary = (
                error
                if isinstance(error, OperationFailure)
                else OperationFailure(
                    "head_mismatch"
                    if isinstance(error, GatewayFailure) and error.code == "head_mismatch"
                    else "delete_partial"
                )
            )
            primary.partial = bool(deleted)
            try:
                cleanup_started = self._start_deadline()

                def persist_failure() -> None:
                    status = self._record_status(
                        request.operation_id,
                        prepared["checkpoint_id"],
                        "partial" if deleted else "refused",
                        request.plan_sha256,
                        deleted,
                        records,
                        primary.code,
                        cleanup_started,
                        head_requests=head_requests,
                        delete_requests=delete_requests,
                        postflight_inventory_sha256=postflight_sha256,
                        deadline_seconds=30,
                        failed=(current_record,)
                        if primary.code == "head_mismatch" and current_record is not None
                        else tuple(record for record in current_batch if record not in deleted),
                    )
                    self._ensure_audit(request.operation_id, status, prepared)

                cleanup_deadline = getattr(self._gateway, "operation_deadline", None)
                if callable(cleanup_deadline):
                    with cleanup_deadline(lambda: self.check_deadline(cleanup_started, max_seconds=30)):
                        persist_failure()
                else:
                    persist_failure()
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException:
                primary.add_note("partial_status_persist_failed")
            raise primary from None
        try:
            status = self._record_status(
                request.operation_id,
                prepared["checkpoint_id"],
                "completed",
                request.plan_sha256,
                deleted,
                records,
                None,
                started,
                head_requests=head_requests,
                delete_requests=delete_requests,
                postflight_inventory_sha256=postflight_sha256,
            )
            self._ensure_audit(request.operation_id, status, prepared)
            self.check_deadline(started)
        except (KeyboardInterrupt, SystemExit):
            raise
        except OperationFailure as error:
            error.partial = bool(deleted)
            raise
        except BaseException:
            raise OperationFailure("evidence_write_failed", partial=bool(deleted)) from None
        return status

    def _ensure_audit(
        self,
        operation_id: str,
        status: OperationStatus,
        prepared: dict[str, object],
    ) -> None:
        value = json.loads(status.body)
        attempt_id = str(
            uuid.uuid5(
                uuid.UUID(operation_id),
                f"{value['attempt_sequence']}:{hashlib.sha256(status.body).hexdigest()}",
            )
        )
        audit_body = canonical_json_bytes(
            {
                "actor": prepared["actor"],
                "attempt_id": attempt_id,
                "attempt_sequence": value["attempt_sequence"],
                "capability_profile": "minio-2025-09-manual-verified-readback",
                "checkpoint_id": prepared["checkpoint_id"],
                "decision": value["state"],
                "deleted_bytes": value["deleted_bytes"],
                "deleted_objects": value["deleted_objects"],
                "delete_requests": value["delete_requests"],
                "head_requests": value["head_requests"],
                "manifest_shards": prepared["manifest_shards"],
                "evaluated_at": prepared["evaluated_at"],
                "occurred_at": value["occurred_at"],
                "operation_id": operation_id,
                "plan_sha256": value["plan_sha256"],
                "planned_objects": value["planned_objects"],
                "planned_bytes": value["deleted_bytes"] + value["remaining_bytes"],
                "policy_sha256": prepared["policy_sha256"],
                "postflight_inventory_sha256": value["postflight_inventory_sha256"],
                "prepared_at": prepared["prepared_at"],
                "prefix_sha256": prepared["prefix_sha256"],
                "primary_category": value["primary_category"],
                "refusal_codes": [value["primary_category"]] if value["primary_category"] is not None else [],
                "remaining_bytes": value["remaining_bytes"],
                "remaining_objects": value["remaining_objects"],
                "review": prepared["review"],
                "result_sha256": hashlib.sha256(status.body).hexdigest(),
                "result_shards": value["result_shards"],
                "schema_version": 1,
            },
            max_bytes=self._max_summary_bytes,
        )
        key = f"_retention/audits/{operation_id}/{attempt_id}.json"
        try:
            existing, _etag = self._gateway.read_control(key, max_bytes=self._max_summary_bytes)
        except GatewayFailure as error:
            if error.code == "operation_deadline":
                raise OperationFailure("operation_deadline") from None
            if error.code != "control_missing":
                raise OperationFailure("audit_read_failed") from None
        except KeyError:
            pass
        else:
            if existing != audit_body:
                raise OperationFailure("control_conflict")
            return
        self._create_identical(key, audit_body, self._max_summary_bytes)

    def status(self, operation_id: str) -> OperationStatus:
        if _UUID.fullmatch(operation_id or "") is None:
            raise OperationFailure("operation_id_invalid")
        base = f"_retention/tombstones/{operation_id}"
        prepared_body = self._read_control(f"{base}/prepared.json", self._max_summary_bytes, "prepared_read_failed")
        prepared = self._decode_prepared(prepared_body, operation_id)
        records = self._read_bound_manifest(base, prepared)
        status = self._latest_status(
            operation_id,
            required=True,
            prepared=prepared,
            records=records,
        )
        assert status is not None
        return status

    def _latest_status(
        self,
        operation_id: str,
        *,
        required: bool,
        prepared: dict[str, object],
        records: tuple[ObjectRecord, ...],
    ) -> OperationStatus | None:
        try:
            keys = self._gateway.list_controls(
                f"_retention/tombstones/{operation_id}/results/attempts/",
                max_keys=1_024,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except GatewayFailure as error:
            if error.code == "operation_deadline":
                raise OperationFailure("operation_deadline") from None
            raise OperationFailure("status_read_failed") from None
        except BaseException as error:
            if getattr(error, "code", None) == "operation_deadline":
                raise OperationFailure("operation_deadline") from None
            raise OperationFailure("status_read_failed") from None
        statuses: list[tuple[int, dict[str, object], OperationStatus]] = []
        for key in keys:
            body = self._read_control(key, self._max_summary_bytes, "status_invalid")
            try:
                value = json.loads(body)
                state = value["state"]
                deleted_objects = value["deleted_objects"]
                canonical = canonical_json_bytes(value, max_bytes=self._max_summary_bytes) == body
            except (KeyError, TypeError, json.JSONDecodeError):
                raise OperationFailure("status_invalid") from None
            if (
                state not in {"prepared", "not_ready", "partial", "completed", "refused"}
                or set(value)
                != {
                    "checkpoint_id",
                    "attempt_sequence",
                    "deleted_bytes",
                    "deleted_objects",
                    "delete_requests",
                    "head_requests",
                    "occurred_at",
                    "operation_id",
                    "plan_sha256",
                    "planned_objects",
                    "postflight_inventory_sha256",
                    "primary_category",
                    "remaining_bytes",
                    "remaining_objects",
                    "result_shards",
                    "schema_version",
                    "state",
                }
                or not canonical
                or value.get("schema_version") != 1
                or not isinstance(value.get("checkpoint_id"), str)
                or _IDENTIFIER.fullmatch(value["checkpoint_id"]) is None
                or not isinstance(value.get("plan_sha256"), str)
                or _SHA256.fullmatch(value["plan_sha256"]) is None
                or type(deleted_objects) is not int
                or deleted_objects < 0
                or type(value.get("deleted_bytes")) is not int
                or value["deleted_bytes"] < 0
                or type(value.get("planned_objects")) is not int
                or value["planned_objects"] < 1
                or type(value.get("remaining_objects")) is not int
                or value["remaining_objects"] < 0
                or deleted_objects + value["remaining_objects"] != value["planned_objects"]
                or type(value.get("remaining_bytes")) is not int
                or value["remaining_bytes"] < 0
                or type(value.get("attempt_sequence")) is not int
                or value["attempt_sequence"] < 1
                or _parse_utc(value.get("occurred_at")) is None
                or type(value.get("head_requests")) is not int
                or value["head_requests"] < 0
                or type(value.get("delete_requests")) is not int
                or value["delete_requests"] < 0
                or not isinstance(value.get("result_shards"), list)
                or not value["result_shards"]
                or len(set(value["result_shards"])) != len(value["result_shards"])
                or any(
                    not isinstance(digest, str) or _SHA256.fullmatch(digest) is None
                    for digest in value["result_shards"]
                )
                or (state in {"prepared", "completed"} and value.get("primary_category") is not None)
                or (
                    state in {"not_ready", "partial", "refused"}
                    and (
                        not isinstance(value.get("primary_category"), str)
                        or value["primary_category"] not in _STATUS_PRIMARY_CATEGORIES[state]
                    )
                )
                or (
                    value.get("postflight_inventory_sha256") is not None
                    and _SHA256.fullmatch(value["postflight_inventory_sha256"] or "") is None
                )
                or value.get("operation_id") != operation_id
                or value.get("checkpoint_id") != prepared.get("checkpoint_id")
                or value.get("plan_sha256") != prepared.get("plan_sha256")
                or not key.endswith(f"/{value['attempt_sequence']:06d}-{hashlib.sha256(body).hexdigest()}.json")
            ):
                raise OperationFailure("status_invalid")
            outcomes = self._read_result_classification(
                f"_retention/tombstones/{operation_id}",
                value,
                records,
            )
            deleted = {digest for digest, outcome in outcomes.items() if outcome == "deleted"}
            observed_outcomes = set(outcomes.values())
            if (
                (
                    state in {"prepared", "not_ready"}
                    and (
                        observed_outcomes != {"unattempted"}
                        or deleted
                        or value["deleted_bytes"] != 0
                        or value["head_requests"] != 0
                        or value["delete_requests"] != 0
                        or value["postflight_inventory_sha256"] is not None
                    )
                )
                or (state == "not_ready" and value["primary_category"] != "quiescence_not_ready")
                or (state == "partial" and not deleted)
                or (
                    state == "completed"
                    and (
                        observed_outcomes != {"deleted"}
                        or len(deleted) != len(records)
                        or value["remaining_objects"] != 0
                        or value["remaining_bytes"] != 0
                        or value["postflight_inventory_sha256"] != inventory_sha256(())
                    )
                )
            ):
                raise OperationFailure("status_invalid")
            statuses.append((value["attempt_sequence"], value, deleted, OperationStatus(operation_id, state, body)))
        if not statuses:
            if required:
                raise OperationFailure("status_missing")
            return None
        statuses.sort(key=lambda item: item[0])
        if tuple(item[0] for item in statuses) != tuple(range(1, len(statuses) + 1)):
            raise OperationFailure("status_ambiguous")
        if statuses[0][1]["state"] != "prepared":
            raise OperationFailure("status_ambiguous")
        last_deleted: set[str] = set()
        last_occurred_at: datetime | None = None
        last_state_rank = -1
        state_ranks = {"prepared": 0, "not_ready": 1, "refused": 2, "partial": 2, "completed": 3}
        for index, (_sequence, value, deleted, _status) in enumerate(statuses):
            occurred_at = _parse_utc(value["occurred_at"])
            state = value["state"]
            state_rank = state_ranks[state]
            if (
                (index > 0 and state == "prepared")
                or state_rank < last_state_rank
                or not last_deleted.issubset(deleted)
                or (last_occurred_at is not None and occurred_at < last_occurred_at)
                or (state == "not_ready" and deleted)
                or (state == "partial" and not deleted)
                or (state == "completed" and len(deleted) != len(records))
            ):
                raise OperationFailure("status_ambiguous")
            last_deleted = deleted
            last_occurred_at = occurred_at
            last_state_rank = state_rank
        if any(item[1]["state"] == "completed" for item in statuses[:-1]):
            raise OperationFailure("status_ambiguous")
        for _sequence, _value, _deleted, status in statuses:
            self._ensure_audit(operation_id, status, prepared)
        return statuses[-1][3]

    def _read_bound_manifest(
        self,
        base: str,
        prepared: dict[str, object],
    ) -> tuple[ObjectRecord, ...]:
        records = self._read_manifest(base, prepared.get("manifest_shards"))
        if prepared.get("inventory_sha256") != inventory_sha256(records):
            raise OperationFailure("status_invalid")
        return records

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
                    if not isinstance(value, dict) or set(value) != {
                        "etag",
                        "key",
                        "last_modified",
                        "size_bytes",
                    }:
                        raise ValueError
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
            try:
                if (
                    canonical_json_bytes(
                        values,
                        max_bytes=self._max_result_shard_bytes,
                        max_nodes=max(4_096, len(values) * 8 + 8),
                    )
                    != body
                ):
                    raise OperationFailure("manifest_invalid")
            except RecordFailure:
                raise OperationFailure("manifest_invalid") from None
        if not records:
            raise OperationFailure("manifest_empty")
        return tuple(records)

    def _decode_prepared(self, body: bytes, operation_id: str) -> dict[str, object]:
        try:
            value = json.loads(body)
            if (
                not isinstance(value, dict)
                or set(value)
                != {
                    "actor",
                    "checkpoint_id",
                    "evaluated_at",
                    "inventory_sha256",
                    "manifest_shards",
                    "operation_id",
                    "plan_sha256",
                    "policy_sha256",
                    "prefix",
                    "prefix_sha256",
                    "prepared_at",
                    "review",
                    "schema_version",
                }
                or canonical_json_bytes(value, max_bytes=self._max_summary_bytes) != body
                or value["schema_version"] != 1
                or value["operation_id"] != operation_id
            ):
                raise ValueError
            evaluated_at = _parse_utc(value["evaluated_at"])
            prepared_at = _parse_utc(value["prepared_at"])
            manifest_shards = value["manifest_shards"]
            prefix = value["prefix"]
            if (
                _IDENTIFIER.fullmatch(value["actor"] or "") is None
                or _IDENTIFIER.fullmatch(value["review"] or "") is None
                or value["checkpoint_id"] != "go-live-streaming-test-v1"
                or not isinstance(prefix, str)
                or not prefix.isascii()
                or _DISPOSABLE_PREFIX.fullmatch(prefix) is None
                or value["prefix_sha256"] != hashlib.sha256(prefix.encode("ascii")).hexdigest()
                or _SHA256.fullmatch(value["plan_sha256"] or "") is None
                or _SHA256.fullmatch(value["policy_sha256"] or "") is None
                or _SHA256.fullmatch(value["inventory_sha256"] or "") is None
                or not isinstance(manifest_shards, list)
                or not manifest_shards
                or len(manifest_shards) != len(set(manifest_shards))
                or any(not isinstance(digest, str) or _SHA256.fullmatch(digest) is None for digest in manifest_shards)
                or evaluated_at is None
                or prepared_at is None
                or prepared_at < evaluated_at
            ):
                raise ValueError
            if value["policy_sha256"] != self._policy_sha256:
                raise OperationFailure("policy_drift")
            return value
        except OperationFailure:
            raise
        except (KeyError, TypeError, ValueError, json.JSONDecodeError, RecordFailure):
            raise OperationFailure("prepared_invalid") from None

    def _read_control(self, key: str, bound: int, code: str) -> bytes:
        try:
            body, _etag = self._gateway.read_control(key, max_bytes=bound)
            return body
        except (KeyboardInterrupt, SystemExit):
            raise
        except GatewayFailure as error:
            if error.code == "operation_deadline":
                raise OperationFailure("operation_deadline") from None
            raise OperationFailure(code) from None
        except BaseException as error:
            if getattr(error, "code", None) == "operation_deadline":
                raise OperationFailure("operation_deadline") from None
            raise OperationFailure(code) from None

    def _existing_prepared(self, key: str, operation_id: str) -> dict[str, object] | None:
        try:
            body, _etag = self._gateway.read_control(key, max_bytes=self._max_summary_bytes)
        except GatewayFailure as error:
            if error.code == "control_missing":
                return None
            if error.code == "operation_deadline":
                raise OperationFailure("operation_deadline") from None
            raise OperationFailure("prepared_read_failed") from None
        except KeyError:
            return None
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException as error:
            if getattr(error, "code", None) == "operation_deadline":
                raise OperationFailure("operation_deadline") from None
            raise OperationFailure("prepared_read_failed") from None
        return self._decode_prepared(body, operation_id)

    def _record_status(
        self,
        operation_id: str,
        checkpoint_id: str,
        state: str,
        plan_sha256: str,
        deleted: list[ObjectRecord],
        planned: tuple[ObjectRecord, ...],
        primary_category: str | None,
        started: float,
        *,
        head_requests: int,
        delete_requests: int,
        postflight_inventory_sha256: str | None,
        deadline_seconds: int | None = None,
        failed: tuple[ObjectRecord, ...] = (),
    ):
        deleted_sha256s = {_record_sha256(record) for record in deleted}
        failed_sha256s = {_record_sha256(record) for record in failed}
        classifications = tuple(
            {
                "object_sha256": _record_sha256(record),
                "outcome": (
                    "deleted"
                    if _record_sha256(record) in deleted_sha256s
                    else "failed"
                    if _record_sha256(record) in failed_sha256s
                    else "unattempted"
                ),
            }
            for record in planned
        )
        result_shards = self._write_result_shards(
            operation_id,
            classifications,
            started,
            deadline_seconds=deadline_seconds,
        )
        deleted_records = tuple(record for record in planned if _record_sha256(record) in deleted_sha256s)
        remaining_records = tuple(record for record in planned if _record_sha256(record) not in deleted_sha256s)
        try:
            existing_keys = self._gateway.list_controls(
                f"_retention/tombstones/{operation_id}/results/attempts/",
                max_keys=1_024,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except GatewayFailure as error:
            if error.code == "operation_deadline":
                raise OperationFailure("operation_deadline") from None
            raise OperationFailure("status_read_failed") from None
        except BaseException as error:
            if getattr(error, "code", None) == "operation_deadline":
                raise OperationFailure("operation_deadline") from None
            raise OperationFailure("status_read_failed") from None
        attempt_sequence = len(existing_keys) + 1
        if attempt_sequence > 1_024:
            raise OperationFailure("attempt_bound")
        body = canonical_json_bytes(
            {
                "attempt_sequence": attempt_sequence,
                "deleted_bytes": sum(record.size_bytes for record in deleted_records),
                "deleted_objects": len(deleted_records),
                "delete_requests": delete_requests,
                "checkpoint_id": checkpoint_id,
                "head_requests": head_requests,
                "occurred_at": _format_utc(self._exact_now()),
                "operation_id": operation_id,
                "plan_sha256": plan_sha256,
                "planned_objects": len(planned),
                "postflight_inventory_sha256": postflight_inventory_sha256,
                "primary_category": primary_category,
                "remaining_bytes": sum(record.size_bytes for record in remaining_records),
                "remaining_objects": len(remaining_records),
                "result_shards": result_shards,
                "schema_version": 1,
                "state": state,
            },
            max_bytes=self._max_summary_bytes,
        )
        status = OperationStatus(operation_id, state, body)
        digest = hashlib.sha256(body).hexdigest()
        key = f"_retention/tombstones/{operation_id}/results/attempts/{attempt_sequence:06d}-{digest}.json"
        self._create_identical(key, body, self._max_summary_bytes)
        self.check_deadline(started, max_seconds=deadline_seconds)
        return status

    def _write_result_shards(
        self,
        operation_id: str,
        values: tuple[dict[str, str], ...],
        started: float,
        *,
        deadline_seconds: int | None = None,
    ) -> tuple[str, ...]:
        chunks: list[list[dict[str, str]]] = []
        current: list[dict[str, str]] = []
        current_bytes = 2
        for value in values:
            encoded = canonical_json_bytes(value, max_bytes=256)
            added = len(encoded) + (1 if current else 0)
            if current and current_bytes + added > self._max_result_shard_bytes:
                chunks.append(current)
                current = []
                current_bytes = 2
                added = len(encoded)
            if current_bytes + added > self._max_result_shard_bytes:
                raise OperationFailure("result_shard_bound")
            current.append(value)
            current_bytes += added
        if current:
            chunks.append(current)
        digests: list[str] = []
        for chunk in chunks:
            body = canonical_json_bytes(
                chunk,
                max_bytes=self._max_result_shard_bytes,
                max_nodes=len(chunk) * 4 + 8,
            )
            digest = hashlib.sha256(body).hexdigest()
            self._create_identical(
                f"_retention/tombstones/{operation_id}/results/shards/{digest}.json",
                body,
                self._max_result_shard_bytes,
            )
            self.check_deadline(started, max_seconds=deadline_seconds)
            digests.append(digest)
        return tuple(digests)

    def _read_result_classification(
        self,
        base: str,
        status: dict[str, object],
        records: tuple[ObjectRecord, ...],
    ) -> dict[str, str]:
        digests = status.get("result_shards")
        if (
            not isinstance(digests, list)
            or not digests
            or any(not isinstance(value, str) or _SHA256.fullmatch(value) is None for value in digests)
        ):
            raise OperationFailure("status_invalid")
        expected = {_record_sha256(record) for record in records}
        observed: dict[str, str] = {}
        for digest in digests:
            body = self._read_control(
                f"{base}/results/shards/{digest}.json",
                self._max_result_shard_bytes,
                "status_invalid",
            )
            if hashlib.sha256(body).hexdigest() != digest:
                raise OperationFailure("status_invalid")
            try:
                values = json.loads(body)
            except json.JSONDecodeError:
                raise OperationFailure("status_invalid") from None
            if not isinstance(values, list):
                raise OperationFailure("status_invalid")
            try:
                if (
                    canonical_json_bytes(
                        values,
                        max_bytes=self._max_result_shard_bytes,
                        max_nodes=max(4_096, len(values) * 4 + 8),
                    )
                    != body
                ):
                    raise OperationFailure("status_invalid")
            except RecordFailure:
                raise OperationFailure("status_invalid") from None
            for value in values:
                if (
                    not isinstance(value, dict)
                    or set(value) != {"object_sha256", "outcome"}
                    or not isinstance(value["object_sha256"], str)
                    or _SHA256.fullmatch(value["object_sha256"]) is None
                    or value["outcome"] not in {"deleted", "failed", "unattempted"}
                    or value["object_sha256"] in observed
                ):
                    raise OperationFailure("status_invalid")
                observed[value["object_sha256"]] = value["outcome"]
        if set(observed) != expected:
            raise OperationFailure("status_invalid")
        deleted = {digest for digest, outcome in observed.items() if outcome == "deleted"}
        by_digest = {_record_sha256(record): record for record in records}
        if (
            len(deleted) != status.get("deleted_objects")
            or len(records) != status.get("planned_objects")
            or sum(by_digest[digest].size_bytes for digest in deleted) != status.get("deleted_bytes")
            or sum(record.size_bytes for digest, record in by_digest.items() if digest not in deleted)
            != status.get("remaining_bytes")
        ):
            raise OperationFailure("status_invalid")
        return observed

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
                if (
                    canonical_json_bytes(
                        value,
                        max_bytes=len(shard.body),
                        max_nodes=max(4_096, len(shard.records) * 8 + 8),
                    )
                    != shard.body
                ):
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
        if (
            artifact.summary.get("checkpoint_id") != "go-live-streaming-test-v1"
            or not isinstance(artifact.summary.get("prefix"), str)
            or _DISPOSABLE_PREFIX.fullmatch(artifact.summary["prefix"]) is None
        ):
            raise OperationFailure("destructive_scope_invalid")
        return artifact

    def _create_identical(self, key: str, body: bytes, bound: int) -> None:
        try:
            self._gateway.create_control(key, body)
        except (KeyboardInterrupt, SystemExit):
            raise
        except GatewayFailure as error:
            if error.code == "operation_deadline":
                raise OperationFailure("operation_deadline") from None
            try:
                existing, _etag = self._gateway.read_control(key, max_bytes=bound)
            except (KeyboardInterrupt, SystemExit):
                raise
            except GatewayFailure as read_error:
                if read_error.code == "operation_deadline":
                    raise OperationFailure("operation_deadline") from None
                raise OperationFailure("control_create_failed") from None
            if existing != body:
                raise OperationFailure("control_conflict")
        except BaseException as error:
            if getattr(error, "code", None) == "operation_deadline":
                raise OperationFailure("operation_deadline") from None
            try:
                existing, _etag = self._gateway.read_control(key, max_bytes=bound)
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException as read_error:
                if getattr(read_error, "code", None) == "operation_deadline":
                    raise OperationFailure("operation_deadline") from None
                raise OperationFailure("control_create_failed") from None
            if existing != body:
                raise OperationFailure("control_conflict")
        try:
            readback, _etag = self._gateway.read_control(key, max_bytes=bound)
        except (KeyboardInterrupt, SystemExit):
            raise
        except GatewayFailure as error:
            if error.code == "operation_deadline":
                raise OperationFailure("operation_deadline") from None
            raise OperationFailure("control_readback_failed") from None
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

    def _start_deadline(self) -> float:
        try:
            started = self._monotonic()
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise OperationFailure("clock_failed") from None
        if type(started) not in (int, float):
            raise OperationFailure("clock_invalid")
        return float(started)

    def check_deadline(self, started: float, *, max_seconds: int | None = None) -> None:
        try:
            elapsed = self._monotonic() - started
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise OperationFailure("clock_failed") from None
        if elapsed < 0 or elapsed > (self._max_active_seconds if max_seconds is None else max_seconds):
            raise OperationFailure("operation_deadline")


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
