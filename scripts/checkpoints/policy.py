"""Strict, non-networked checkpoint ownership and retention-policy parsing."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

import yaml


class PolicyError(ValueError):
    """A bounded, categorized policy validation failure."""

    def __init__(self, code: str, detail: str = "policy rejected") -> None:
        super().__init__(f"{code}: {detail[:256]}")
        self.code = code


@dataclass(frozen=True)
class LeasePolicy:
    heartbeat_seconds: int
    ttl_seconds: int
    future_tolerance_seconds: int
    quiescence_seconds: int


@dataclass(frozen=True)
class OperationBounds:
    max_pages: int
    max_objects: int
    max_bytes: int
    max_delete_keys: int
    max_active_seconds: int
    max_summary_bytes: int
    max_manifest_shard_bytes: int


@dataclass(frozen=True)
class CheckpointEntry:
    checkpoint_id: str
    prefix: str
    owner: str
    workload: str
    source: str
    sink: str
    lifecycle: str
    durability: str
    terminal_states: tuple[str, ...]
    retention_seconds: int
    recovery_class: str
    sink_disposition: str
    concurrent_writers: str
    retirement_authorization: str
    scales: tuple[str, ...] = ()


@dataclass(frozen=True)
class MatchedCheckpoint:
    checkpoint_id: str
    prefix: str
    generation: Mapping[str, str]


@dataclass(frozen=True)
class CheckpointPolicy:
    version: int
    bucket: str
    control_prefix: str
    lease: LeasePolicy
    bounds: OperationBounds
    entries: Mapping[str, CheckpointEntry]

    def match_prefix(self, prefix: str) -> MatchedCheckpoint:
        _validate_concrete_prefix(prefix, self.control_prefix)
        for entry in self.entries.values():
            if not entry.scales:
                if prefix == entry.prefix:
                    return MatchedCheckpoint(entry.checkpoint_id, prefix, {})
                continue
            match = _GENERATION_PATTERN.fullmatch(prefix)
            if match and match.group("scale") in entry.scales:
                return MatchedCheckpoint(
                    entry.checkpoint_id,
                    prefix,
                    MappingProxyType(match.groupdict()),
                )
        raise PolicyError("unknown_prefix")


class _UniqueKeyLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(loader: _UniqueKeyLoader, node: yaml.MappingNode, deep: bool = False) -> dict[Any, Any]:
    result: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in result
        except TypeError as error:
            raise PolicyError("invalid_key") from error
        if duplicate:
            raise PolicyError("duplicate_key")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_UniqueKeyLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_unique_mapping)

_TOP_KEYS = frozenset({"version", "bucket", "control_prefix", "lease", "bounds", "checkpoints"})
_LEASE_VALUES = {
    "heartbeat_seconds": 60,
    "ttl_seconds": 600,
    "future_tolerance_seconds": 300,
    "quiescence_seconds": 900,
}
_BOUND_VALUES = {
    "max_pages": 100,
    "max_objects": 100_000,
    "max_bytes": 10_737_418_240,
    "max_delete_keys": 1_000,
    "max_active_seconds": 900,
    "max_summary_bytes": 65_536,
    "max_manifest_shard_bytes": 1_048_576,
}
_ENTRY_KEYS = frozenset(
    {
        "checkpoint_id",
        "prefix",
        "owner",
        "workload",
        "source",
        "sink",
        "lifecycle",
        "durability",
        "terminal_states",
        "retention_seconds",
        "recovery_class",
        "sink_disposition",
        "concurrent_writers",
        "retirement_authorization",
    }
)
_GENERATION_ENTRY_KEYS = _ENTRY_KEYS | {"scales"}
_GENERATION_TEMPLATE = "gh_events_file/{scale}/{publication_id}/{manifest_sha256}/"
_GENERATION_PATTERN = re.compile(
    r"gh_events_file/(?P<scale>tiny|small|medium)/"
    r"(?P<publication_id>[0-9a-f]{32})/"
    r"(?P<manifest_sha256>[0-9a-f]{64})/"
)
_SAFE_FIXED_PREFIX = re.compile(r"(?:[a-z0-9][a-z0-9_-]*/)+")

_EXPECTED_ENTRIES: Mapping[str, Mapping[str, Any]] = {
    "streaming-events-v1": {
        "prefix": "events/",
        "owner": "Streaming Data Engineering",
        "workload": "streaming_ingest-events-spark-iceberg",
        "source": "redpanda:events",
        "sink": "lakehouse.bronze.events",
        "durability": "durable_stream",
        "terminal_states": ("stopped", "retired"),
        "retention_seconds": 2_592_000,
        "recovery_class": "coordinated_replay",
        "sink_disposition": "snapshot_or_reset",
        "concurrent_writers": "forbidden",
        "retirement_authorization": "required",
    },
    "streaming-event-windows-v1": {
        "prefix": "event_windows/",
        "owner": "Streaming Data Engineering",
        "workload": "streaming_windows-events-spark-iceberg",
        "source": "redpanda:events",
        "sink": "lakehouse.gold.event_windows",
        "durability": "durable_stream",
        "terminal_states": ("stopped", "retired"),
        "retention_seconds": 2_592_000,
        "recovery_class": "coordinated_replay",
        "sink_disposition": "snapshot_or_reset",
        "concurrent_writers": "forbidden",
        "retirement_authorization": "required",
    },
    "streaming-online-retail-cdc-v1": {
        "prefix": "online_retail_cdc/",
        "owner": "Streaming Data Engineering",
        "workload": "cdc_streaming-online_retail-spark-iceberg",
        "source": "redpanda:online_retail_cdc",
        "sink": "lakehouse.silver.online_retail_cdc",
        "durability": "durable_stream",
        "terminal_states": ("stopped", "retired"),
        "retention_seconds": 2_592_000,
        "recovery_class": "coordinated_replay",
        "sink_disposition": "snapshot_or_reset",
        "concurrent_writers": "forbidden",
        "retirement_authorization": "required",
    },
    "streaming-gh-archive-file-v1": {
        "prefix": _GENERATION_TEMPLATE,
        "owner": "Streaming Data Engineering Education",
        "workload": "streaming_ingest-gh_archive-spark-iceberg",
        "source": "resolver:gh_archive",
        "sink": "lakehouse.bronze.gh_events_stream",
        "durability": "generation_reproducibility",
        "terminal_states": ("completed", "stopped"),
        "retention_seconds": 1_209_600,
        "recovery_class": "exact_generation_replay",
        "sink_disposition": "reset_required",
        "concurrent_writers": "forbidden",
        "retirement_authorization": "not_applicable",
        "scales": ("tiny", "small", "medium"),
    },
    "go-live-streaming-test-v1": {
        "prefix": "streaming_test/",
        "owner": "Lab Acceptance Engineering",
        "workload": "go-live-streaming-test",
        "source": "bounded_synthetic",
        "sink": "s3a://lakehouse/bronze/streaming_test",
        "durability": "disposable_acceptance",
        "terminal_states": ("successful", "stopped"),
        "retention_seconds": 86_400,
        "recovery_class": "disposable_recreate",
        "sink_disposition": "reset_required",
        "concurrent_writers": "forbidden",
        "retirement_authorization": "not_applicable",
    },
}


def load_policy(path: Path) -> CheckpointPolicy:
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise PolicyError("policy_read_failed") from error
    return parse_policy(text)


def parse_policy(text: str) -> CheckpointPolicy:
    if not isinstance(text, str):
        raise PolicyError("invalid_type")
    try:
        raw = yaml.load(text, Loader=_UniqueKeyLoader)
    except PolicyError:
        raise
    except yaml.YAMLError as error:
        raise PolicyError("invalid_yaml") from error
    root = _require_mapping(raw, "invalid_type")
    _require_exact_keys(root, _TOP_KEYS)
    _require_exact(root["version"], 1, "invalid_type")
    _require_exact(root["bucket"], "checkpoints", "invalid_bucket")
    _require_exact(root["control_prefix"], "_retention/", "invalid_control_prefix")

    lease_values = _require_mapping(root["lease"], "invalid_type")
    _require_exact_keys(lease_values, frozenset(_LEASE_VALUES))
    for key, expected in _LEASE_VALUES.items():
        _require_exact_int(lease_values[key], expected, "invalid_lease_policy")

    bound_values = _require_mapping(root["bounds"], "invalid_type")
    _require_exact_keys(bound_values, frozenset(_BOUND_VALUES))
    for key, expected in _BOUND_VALUES.items():
        _require_exact_int(bound_values[key], expected, "invalid_bound")

    items = root["checkpoints"]
    if not isinstance(items, list):
        raise PolicyError("invalid_type")
    item_mappings = [_require_mapping(item, "invalid_type") for item in items]
    seen_ids: set[str] = set()
    raw_prefixes: list[str] = []
    for item_mapping in item_mappings:
        checkpoint_id = item_mapping.get("checkpoint_id")
        if isinstance(checkpoint_id, str):
            if checkpoint_id in seen_ids:
                raise PolicyError("duplicate_checkpoint_id")
            seen_ids.add(checkpoint_id)
        prefix = item_mapping.get("prefix")
        _validate_registry_prefix(prefix, "_retention/")
        raw_prefixes.append(prefix)
    _reject_overlapping_prefixes(tuple(raw_prefixes))

    entries: dict[str, CheckpointEntry] = {}
    for item_mapping in item_mappings:
        entry = _parse_entry(item_mapping)
        entries[entry.checkpoint_id] = entry
    if tuple(entries) != tuple(_EXPECTED_ENTRIES):
        raise PolicyError("invalid_checkpoint_inventory")
    _reject_overlaps(tuple(entries.values()))

    return CheckpointPolicy(
        version=1,
        bucket="checkpoints",
        control_prefix="_retention/",
        lease=LeasePolicy(**lease_values),
        bounds=OperationBounds(**bound_values),
        entries=MappingProxyType(entries),
    )


def _parse_entry(value: object) -> CheckpointEntry:
    raw = _require_mapping(value, "invalid_type")
    checkpoint_id = raw.get("checkpoint_id")
    if not isinstance(checkpoint_id, str):
        raise PolicyError("invalid_type")
    expected = _EXPECTED_ENTRIES.get(checkpoint_id)
    if expected is None:
        raise PolicyError("invalid_checkpoint_id")
    allowed_keys = _GENERATION_ENTRY_KEYS if "scales" in expected else _ENTRY_KEYS
    _require_exact_keys(raw, allowed_keys)

    for key in _ENTRY_KEYS - {"terminal_states", "retention_seconds", "lifecycle"}:
        if not isinstance(raw[key], str) or not raw[key]:
            raise PolicyError("invalid_type")
    lifecycle = raw["lifecycle"]
    allowed_lifecycle = {"active", "retired"} if expected["durability"] == "durable_stream" else {"active"}
    if lifecycle not in allowed_lifecycle:
        raise PolicyError("invalid_lifecycle")

    states_value = raw["terminal_states"]
    if not isinstance(states_value, list) or any(not isinstance(state, str) for state in states_value):
        raise PolicyError("invalid_type")
    terminal_states = tuple(states_value)
    if terminal_states != expected["terminal_states"]:
        raise PolicyError("invalid_terminal_states")
    _require_exact_int(raw["retention_seconds"], expected["retention_seconds"], "invalid_retention")

    scales: tuple[str, ...] = ()
    if "scales" in expected:
        scales_value = raw["scales"]
        if not isinstance(scales_value, list) or any(not isinstance(scale, str) for scale in scales_value):
            raise PolicyError("invalid_type")
        scales = tuple(scales_value)
        if scales != expected["scales"]:
            raise PolicyError("invalid_scales")

    for key, expected_value in expected.items():
        if key in {"terminal_states", "retention_seconds", "scales"}:
            continue
        actual = raw[key]
        if actual != expected_value:
            if key == "prefix":
                _validate_registry_prefix(actual, "_retention/")
                raise PolicyError("invalid_prefix")
            if key == "durability":
                raise PolicyError("invalid_durability")
            raise PolicyError(f"invalid_{key}")
    _validate_registry_prefix(raw["prefix"], "_retention/")

    return CheckpointEntry(
        checkpoint_id=checkpoint_id,
        prefix=raw["prefix"],
        owner=raw["owner"],
        workload=raw["workload"],
        source=raw["source"],
        sink=raw["sink"],
        lifecycle=lifecycle,
        durability=raw["durability"],
        terminal_states=terminal_states,
        retention_seconds=raw["retention_seconds"],
        recovery_class=raw["recovery_class"],
        sink_disposition=raw["sink_disposition"],
        concurrent_writers=raw["concurrent_writers"],
        retirement_authorization=raw["retirement_authorization"],
        scales=scales,
    )


def _require_mapping(value: object, code: str) -> dict[str, Any]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise PolicyError(code)
    return value


def _require_exact_keys(value: Mapping[str, Any], expected: frozenset[str]) -> None:
    actual = frozenset(value)
    if actual != expected:
        raise PolicyError("unknown_field" if actual - expected else "missing_field")


def _require_exact(value: object, expected: object, code: str) -> None:
    if type(value) is not type(expected) or value != expected:
        raise PolicyError(code)


def _require_exact_int(value: object, expected: int, code: str) -> None:
    if type(value) is not int:
        raise PolicyError("invalid_type")
    if value != expected:
        raise PolicyError(code)


def _validate_registry_prefix(prefix: object, control_prefix: str) -> None:
    if not isinstance(prefix, str):
        raise PolicyError("invalid_type")
    if prefix == _GENERATION_TEMPLATE:
        return
    _validate_concrete_prefix(prefix, control_prefix)
    if _SAFE_FIXED_PREFIX.fullmatch(prefix) is None:
        raise PolicyError("unsafe_prefix")


def _validate_concrete_prefix(prefix: object, control_prefix: str) -> None:
    if (
        not isinstance(prefix, str)
        or not prefix
        or not prefix.endswith("/")
        or prefix.startswith("/")
        or prefix.startswith(control_prefix)
        or "://" in prefix
        or "\\" in prefix
        or "//" in prefix
        or any(part in {"", ".", ".."} for part in prefix[:-1].split("/"))
        or not prefix.isascii()
    ):
        code = "control_prefix" if isinstance(prefix, str) and prefix.startswith(control_prefix) else "unsafe_prefix"
        raise PolicyError(code)


def _reject_overlaps(entries: tuple[CheckpointEntry, ...]) -> None:
    _reject_overlapping_prefixes(tuple(entry.prefix for entry in entries))


def _reject_overlapping_prefixes(prefixes: tuple[str, ...]) -> None:
    roots = [prefix.split("{", 1)[0] for prefix in prefixes]
    for index, root in enumerate(roots):
        for other in roots[index + 1 :]:
            if root.startswith(other) or other.startswith(root):
                raise PolicyError("overlapping_prefix")
