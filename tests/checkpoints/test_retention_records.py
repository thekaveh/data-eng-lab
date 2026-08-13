from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
from types import MappingProxyType

import pytest

from scripts.checkpoints.records import (
    ObjectRecord,
    PlanArtifact,
    RecordFailure,
    canonical_json_bytes,
    canonical_records,
    decode_exact_json,
    inventory_sha256,
    shard_inventory,
)

NOW = datetime(2026, 8, 13, 12, 0, 0, tzinfo=timezone.utc)
PREFIX = "streaming_test/550e8400-e29b-41d4-a716-446655440000/"


def _record(suffix: str, etag: str, size_bytes: int) -> ObjectRecord:
    return ObjectRecord(f"{PREFIX}{suffix}", etag, size_bytes, NOW)


def test_object_records_validate_exact_types_and_are_frozen():
    record = _record("offsets/0", "a" * 32, 7)

    assert record.key == f"{PREFIX}offsets/0"
    with pytest.raises(FrozenInstanceError):
        record.size_bytes = 8

    bad_values = (
        ("../escape", "a" * 32, 1, NOW),
        (f"{PREFIX}offsets/0", "not-an-etag", 1, NOW),
        (f"{PREFIX}offsets/0", "a" * 32, 0, NOW),
        (f"{PREFIX}offsets/0", "a" * 32, True, NOW),
        (f"{PREFIX}offsets/0", "a" * 32, 1, NOW.replace(microsecond=1)),
        (f"{PREFIX}offsets/0", "a" * 32, 1, NOW.replace(tzinfo=None)),
    )
    for values in bad_values:
        with pytest.raises(RecordFailure):
            ObjectRecord(*values)


def test_canonical_json_is_compact_sorted_utf8_and_rejects_ambiguous_values():
    value = MappingProxyType({"z": ("é",), "a": {"enabled": True, "count": 2}})

    assert canonical_json_bytes(value) == (b'{"a":{"count":2,"enabled":true},"z":["\\u00e9"]}')

    for invalid in (
        {"value": float("nan")},
        {"value": float("inf")},
        {"value": b"payload"},
        {1: "non-string-key"},
        {"value": object()},
    ):
        with pytest.raises(RecordFailure):
            canonical_json_bytes(invalid)


def test_plan_artifact_deeply_freezes_caller_owned_summary():
    summary = {"decision": "eligible", "inventory": {"codes": ["accepted"]}}
    body = canonical_json_bytes(summary)
    plan = PlanArtifact(summary, (), body, __import__("hashlib").sha256(body).hexdigest())

    summary["inventory"]["codes"].append("mutated")

    assert plan.summary["inventory"]["codes"] == ("accepted",)
    with pytest.raises(TypeError):
        plan.summary["decision"] = "refused"


def test_exact_json_decode_rejects_duplicates_unknown_fields_depth_and_body_bounds():
    schema = {"count": int, "name": str, "enabled": bool}
    assert decode_exact_json(b'{"name":"safe","enabled":true,"count":2}', schema) == {
        "count": 2,
        "name": "safe",
        "enabled": True,
    }

    failures = (
        b'{"count":2,"count":3,"name":"safe","enabled":true}',
        b'{"count":2,"name":"safe","enabled":true,"extra":0}',
        b'{"count":true,"name":"safe","enabled":true}',
        b'{"count":2,"name":"safe","enabled":true} trailing',
        b'{"count":2,"name":"safe"}',
        b'{"count":2,"name":"safe","enabled":true,"nested":{"a":{"b":1}}}',
        b"x" * 65_537,
    )
    for body in failures:
        with pytest.raises(RecordFailure):
            decode_exact_json(body, schema, max_bytes=65_536, max_depth=2)


def test_inventory_order_digest_and_duplicate_rejection_are_deterministic():
    records = (
        _record("z-last", "b" * 32, 2),
        _record("a-first", "a" * 32, 1),
    )

    ordered = canonical_records(records)
    assert tuple(record.key for record in ordered) == (
        f"{PREFIX}a-first",
        f"{PREFIX}z-last",
    )
    assert inventory_sha256(records) == "2bc7396a7cab6a680e860bbb1b380f41ff3ab38d4bac448f2249b201375cb93c"
    assert inventory_sha256(reversed(records)) == inventory_sha256(records)
    with pytest.raises(RecordFailure, match="duplicate_object"):
        canonical_records((*records, records[0]))


def test_manifest_shards_are_bounded_complete_and_stable():
    records = tuple(_record(f"state/{index:04d}", f"{index:032x}", index + 1) for index in range(20))

    shards = shard_inventory(records, max_bytes=512)

    assert len(shards) > 1
    assert tuple(shard.index for shard in shards) == tuple(range(len(shards)))
    assert all(len(shard.body) <= 512 for shard in shards)
    assert all(shard.sha256 == __import__("hashlib").sha256(shard.body).hexdigest() for shard in shards)
    assert tuple(record for shard in shards for record in shard.records) == canonical_records(records)
    assert shard_inventory(reversed(records), max_bytes=512) == shards

    with pytest.raises(RecordFailure, match="record_exceeds_shard_bound"):
        shard_inventory((_record("x" * 400, "a" * 32, 1),), max_bytes=128)
