"""Canonical, bounded records for checkpoint-retention operations."""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import MappingProxyType
from typing import Iterable, Mapping


class RecordFailure(ValueError):
    """A bounded category for an invalid retention record."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


_ETAG = re.compile(r"[0-9a-f]{32}(?:-[1-9][0-9]{0,9})?")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_MAX_JSON_BYTES = 65_536
_MAX_JSON_DEPTH = 16
_MAX_JSON_NODES = 4_096


@dataclass(frozen=True)
class ObjectRecord:
    key: str
    etag: str
    size_bytes: int
    last_modified: datetime

    def __post_init__(self) -> None:
        if not _safe_key(self.key):
            raise RecordFailure("object_key_invalid")
        if not isinstance(self.etag, str) or _ETAG.fullmatch(self.etag) is None:
            raise RecordFailure("object_etag_invalid")
        if type(self.size_bytes) is not int or self.size_bytes <= 0:
            raise RecordFailure("object_size_invalid")
        if not _exact_utc(self.last_modified):
            raise RecordFailure("object_timestamp_invalid")

    def as_json(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "etag": self.etag,
                "key": self.key,
                "last_modified": _format_utc(self.last_modified),
                "size_bytes": self.size_bytes,
            }
        )


@dataclass(frozen=True)
class ManifestShard:
    index: int
    records: tuple[ObjectRecord, ...]
    body: bytes
    sha256: str


@dataclass(frozen=True)
class PlanArtifact:
    summary: Mapping[str, object]
    shards: tuple[ManifestShard, ...]
    body: bytes
    sha256: str

    def __post_init__(self) -> None:
        if type(self.shards) is not tuple or any(not isinstance(shard, ManifestShard) for shard in self.shards):
            raise RecordFailure("plan_shards_invalid")
        if type(self.body) is not bytes or not isinstance(self.sha256, str) or _SHA256.fullmatch(self.sha256) is None:
            raise RecordFailure("plan_body_invalid")
        if hashlib.sha256(self.body).hexdigest() != self.sha256:
            raise RecordFailure("plan_digest_mismatch")
        object.__setattr__(self, "summary", _freeze_mapping(self.summary))


@dataclass(frozen=True)
class PreparedRecord:
    operation_id: str
    plan_sha256: str
    shard_sha256s: tuple[str, ...]
    prepared_at: datetime


@dataclass(frozen=True)
class AttemptRecord:
    operation_id: str
    attempt_id: str
    decision: str
    object_sha256s: tuple[str, ...]
    occurred_at: datetime
    primary_category: str | None = None


@dataclass(frozen=True)
class AuditRecord:
    operation_id: str
    attempt_id: str
    decision: str
    counts: Mapping[str, int]
    occurred_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "counts", MappingProxyType(dict(self.counts)))


def canonical_json_bytes(value: object, *, max_bytes: int = _MAX_JSON_BYTES) -> bytes:
    """Encode the supported JSON subset deterministically and within a byte bound."""

    if type(max_bytes) is not int or max_bytes < 2:
        raise RecordFailure("json_bound_invalid")
    normalized = _normalize_json(value, depth=0, nodes=[0])
    try:
        body = json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    except (TypeError, ValueError, UnicodeError):
        raise RecordFailure("json_value_invalid") from None
    if len(body) > max_bytes:
        raise RecordFailure("json_body_too_large")
    return body


def decode_exact_json(
    body: bytes,
    schema: Mapping[str, type | tuple[type, ...]],
    *,
    max_bytes: int = _MAX_JSON_BYTES,
    max_depth: int = _MAX_JSON_DEPTH,
) -> dict[str, object]:
    """Decode one exact JSON object with duplicate, shape, and resource checks."""

    if type(body) is not bytes or type(max_bytes) is not int or len(body) > max_bytes:
        raise RecordFailure("json_body_too_large")
    if type(max_depth) is not int or max_depth < 1:
        raise RecordFailure("json_bound_invalid")

    def pairs(values: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in values:
            if key in result:
                raise RecordFailure("json_duplicate_key")
            result[key] = value
        return result

    try:
        decoded = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=pairs,
            parse_constant=lambda _value: (_ for _ in ()).throw(RecordFailure("json_number_invalid")),
        )
    except RecordFailure:
        raise
    except (UnicodeError, json.JSONDecodeError, RecursionError):
        raise RecordFailure("json_invalid") from None
    _check_decoded_bounds(decoded, max_depth=max_depth, depth=0, nodes=[0])
    if not isinstance(decoded, dict):
        raise RecordFailure("json_shape_invalid")
    if frozenset(decoded) != frozenset(schema):
        raise RecordFailure("json_unknown_or_missing_field")
    for key, expected_type in schema.items():
        accepted_types = expected_type if isinstance(expected_type, tuple) else (expected_type,)
        if not accepted_types or any(not isinstance(item, type) for item in accepted_types):
            raise RecordFailure("json_schema_invalid")
        if type(decoded[key]) not in accepted_types:
            raise RecordFailure("json_field_type_invalid")
    return decoded


def canonical_records(records: Iterable[ObjectRecord]) -> tuple[ObjectRecord, ...]:
    materialized = tuple(records)
    if any(not isinstance(record, ObjectRecord) for record in materialized):
        raise RecordFailure("object_record_invalid")
    ordered = tuple(sorted(materialized, key=lambda record: record.key.encode("utf-8")))
    if any(left.key == right.key for left, right in zip(ordered, ordered[1:])):
        raise RecordFailure("duplicate_object")
    return ordered


def inventory_sha256(records: Iterable[ObjectRecord]) -> str:
    ordered = canonical_records(records)
    digest = hashlib.sha256()
    digest.update(b"[")
    for index, record in enumerate(ordered):
        if index:
            digest.update(b",")
        digest.update(canonical_json_bytes(record.as_json()))
    digest.update(b"]")
    return digest.hexdigest()


def shard_inventory(records: Iterable[ObjectRecord], max_bytes: int) -> tuple[ManifestShard, ...]:
    if type(max_bytes) is not int or max_bytes < 2:
        raise RecordFailure("shard_bound_invalid")
    ordered = canonical_records(records)
    encoded = tuple(canonical_json_bytes(record.as_json()) for record in ordered)
    groups: list[tuple[ObjectRecord, ...]] = []
    current: list[ObjectRecord] = []
    current_size = 2
    for record, record_body in zip(ordered, encoded):
        added_size = len(record_body) + (1 if current else 0)
        if len(record_body) + 2 > max_bytes:
            raise RecordFailure("record_exceeds_shard_bound")
        if current and current_size + added_size > max_bytes:
            groups.append(tuple(current))
            current = []
            current_size = 2
            added_size = len(record_body)
        current.append(record)
        current_size += added_size
    if current:
        groups.append(tuple(current))

    shards: list[ManifestShard] = []
    for index, group in enumerate(groups):
        body = canonical_json_bytes([record.as_json() for record in group], max_bytes=max_bytes)
        shards.append(ManifestShard(index, group, body, hashlib.sha256(body).hexdigest()))
    return tuple(shards)


def _normalize_json(value: object, *, depth: int, nodes: list[int]) -> object:
    nodes[0] += 1
    if nodes[0] > _MAX_JSON_NODES or depth > _MAX_JSON_DEPTH:
        raise RecordFailure("json_structure_bound")
    if value is None or type(value) in (bool, int, str):
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise RecordFailure("json_number_invalid")
        raise RecordFailure("json_number_invalid")
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise RecordFailure("json_key_invalid")
        return {key: _normalize_json(item, depth=depth + 1, nodes=nodes) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_json(item, depth=depth + 1, nodes=nodes) for item in value]
    raise RecordFailure("json_value_invalid")


def _check_decoded_bounds(value: object, *, max_depth: int, depth: int, nodes: list[int]) -> None:
    nodes[0] += 1
    if nodes[0] > _MAX_JSON_NODES or depth > max_depth:
        raise RecordFailure("json_structure_bound")
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise RecordFailure("json_key_invalid")
            _check_decoded_bounds(item, max_depth=max_depth, depth=depth + 1, nodes=nodes)
    elif isinstance(value, list):
        for item in value:
            _check_decoded_bounds(item, max_depth=max_depth, depth=depth + 1, nodes=nodes)
    elif value is None or type(value) in (bool, int, str):
        return
    else:
        raise RecordFailure("json_value_invalid")


def _safe_key(value: object) -> bool:
    if not isinstance(value, str) or not value or value.startswith("/") or value.endswith("/"):
        return False
    try:
        encoded = value.encode("utf-8")
    except UnicodeError:
        return False
    parts = value.split("/")
    return (
        len(encoded) <= 1_024
        and all(part not in {"", ".", ".."} for part in parts)
        and "\\" not in value
        and all(ord(character) >= 0x20 and ord(character) != 0x7F for character in value)
    )


def _exact_utc(value: object) -> bool:
    return (
        isinstance(value, datetime)
        and value.tzinfo is not None
        and value.utcoffset() == timedelta(0)
        and value.microsecond == 0
    )


def _format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _freeze_mapping(value: Mapping[str, object]) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise RecordFailure("mapping_invalid")
    if any(not isinstance(key, str) for key in value):
        raise RecordFailure("mapping_invalid")
    return MappingProxyType({key: _deep_freeze(item) for key, item in value.items()})


def _deep_freeze(value: object) -> object:
    if isinstance(value, Mapping):
        return _freeze_mapping(value)
    if isinstance(value, (list, tuple)):
        return tuple(_deep_freeze(item) for item in value)
    if value is None or type(value) in (bool, int, str):
        return value
    raise RecordFailure("mapping_invalid")
