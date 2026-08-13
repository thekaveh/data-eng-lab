"""Strict, non-networked checkpoint ownership and retention-policy parsing."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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
    retired_at: datetime | None = None
    retirement_review: str | None = None
    scales: tuple[str, ...] = ()


@dataclass(frozen=True)
class MatchedCheckpoint:
    checkpoint_id: str
    prefix: str
    generation: Mapping[str, str]


@dataclass(frozen=True)
class LeaseFacts:
    checkpoint_id: str
    prefix: str
    state: str
    acquired_at: datetime
    heartbeat_at: datetime
    expires_at: datetime
    etag: str
    conflicting: bool = False
    malformed: bool = False


@dataclass(frozen=True)
class TerminalFacts:
    state: str
    occurred_at: datetime
    recovery_approved: bool
    source_available: bool
    sink_disposition_approved: bool
    retirement_review: str | None
    generation: Mapping[str, str]
    exclusive_run: bool = False
    successful: bool = False

    def __post_init__(self) -> None:
        if isinstance(self.generation, Mapping):
            object.__setattr__(self, "generation", MappingProxyType(dict(self.generation)))


@dataclass(frozen=True)
class InventorySummary:
    object_count: int
    total_bytes: int
    newest_last_modified: datetime
    inventory_sha256: str
    changed_since_plan: bool = False
    partial_retry_confined: bool = True


@dataclass(frozen=True)
class EvaluationInput:
    prefix: str
    evaluated_at: datetime
    lease: LeaseFacts | None
    terminal: TerminalFacts | None
    inventory: InventorySummary


@dataclass(frozen=True)
class RetentionDecision:
    eligible: bool
    refusal_codes: tuple[str, ...]
    retention_anchor: datetime | None
    eligible_after: datetime | None
    policy_sha256: str
    plan_json: str
    plan_sha256: str


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
    def __init__(self, stream: str) -> None:
        super().__init__(stream)
        self._policy_node_count = 0
        self._policy_depth = 0

    def compose_node(self, parent: yaml.Node | None, index: object) -> yaml.Node:
        if self.check_event(yaml.AliasEvent):
            raise PolicyError("yaml_alias_forbidden")
        self._policy_node_count += 1
        if self._policy_node_count > _MAX_YAML_NODES:
            raise PolicyError("yaml_node_limit")
        self._policy_depth += 1
        if self._policy_depth > _MAX_YAML_DEPTH:
            raise PolicyError("yaml_depth_exceeded")
        try:
            return super().compose_node(parent, index)
        finally:
            self._policy_depth -= 1


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
_DURABLE_ENTRY_KEYS = _ENTRY_KEYS | {"retired_at", "retirement_review"}
_GENERATION_TEMPLATE = "gh_events_file/{scale}/{publication_id}/{manifest_sha256}/"
_GENERATION_PATTERN = re.compile(
    r"gh_events_file/(?P<scale>tiny|small|medium)/"
    r"(?P<publication_id>[0-9a-f]{32})/"
    r"(?P<manifest_sha256>[0-9a-f]{64})/"
)
_SAFE_FIXED_PREFIX = re.compile(r"(?:[a-z0-9][a-z0-9_-]*/)+")
_SAFE_REVIEW = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,127}")
_MAX_POLICY_BYTES = 262_144
_MAX_YAML_NODES = 4_096
_MAX_YAML_DEPTH = 32

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
        with path.open("rb") as handle:
            payload = handle.read(_MAX_POLICY_BYTES + 1)
    except (OSError, UnicodeError) as error:
        raise PolicyError("policy_read_failed") from error
    if len(payload) > _MAX_POLICY_BYTES:
        raise PolicyError("policy_too_large")
    try:
        text = payload.decode("utf-8")
    except UnicodeError as error:
        raise PolicyError("policy_read_failed") from error
    return parse_policy(text)


def parse_policy(text: str) -> CheckpointPolicy:
    if not isinstance(text, str):
        raise PolicyError("invalid_type")
    if len(text) > _MAX_POLICY_BYTES:
        raise PolicyError("policy_too_large")
    try:
        encoded_size = len(text.encode("utf-8"))
    except UnicodeError as error:
        raise PolicyError("invalid_yaml") from error
    if encoded_size > _MAX_POLICY_BYTES:
        raise PolicyError("policy_too_large")
    try:
        raw = yaml.load(text, Loader=_UniqueKeyLoader)
    except PolicyError:
        raise
    except yaml.YAMLError:
        raise PolicyError("invalid_yaml") from None
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


def evaluate_retention(policy: CheckpointPolicy, facts: EvaluationInput) -> RetentionDecision:
    matched = policy.match_prefix(facts.prefix)
    entry = policy.entries[matched.checkpoint_id]
    refusal_codes: list[str] = []

    evaluated_at_valid = _is_exact_utc(facts.evaluated_at)
    if not evaluated_at_valid:
        refusal_codes.append("invalid_utc_timestamp")

    lease = facts.lease
    terminal = facts.terminal
    inventory = facts.inventory
    if lease is None:
        refusal_codes.append("lease_missing")
    else:
        _evaluate_lease(policy, facts, matched, entry, evaluated_at_valid, lease, terminal, refusal_codes)
    _evaluate_inventory(policy, facts, evaluated_at_valid, inventory, refusal_codes)

    if terminal is None:
        refusal_codes.append("terminal_missing")
    else:
        _evaluate_terminal(policy, facts, entry, matched, terminal, refusal_codes)
        if (
            _is_exact_utc(terminal.occurred_at)
            and _is_exact_utc(inventory.newest_last_modified)
            and inventory.newest_last_modified > terminal.occurred_at
        ):
            refusal_codes.append("object_after_terminal")

    anchor = _retention_anchor(entry, lease, terminal, inventory)
    eligible_after = _safe_add_seconds(anchor, entry.retention_seconds)
    if anchor is not None and eligible_after is None:
        refusal_codes.append("clock_overflow")

    if entry.durability == "durable_stream" and entry.lifecycle == "active":
        refusal_codes.append("registry_active_durable")
    elif evaluated_at_valid and eligible_after is not None and facts.evaluated_at < eligible_after:
        refusal_codes.append("retention_quarantine")

    ordered_codes = tuple(dict.fromkeys(refusal_codes))
    eligible = not ordered_codes
    policy_sha256 = _policy_sha256(policy)
    plan_payload = {
        "checkpoint_id": matched.checkpoint_id,
        "decision": "eligible" if eligible else "refused",
        "eligible_after": _format_utc(eligible_after),
        "evaluated_at": _format_utc(facts.evaluated_at),
        "inventory": {
            "newest_last_modified": _format_utc(inventory.newest_last_modified),
            "object_count": inventory.object_count if type(inventory.object_count) is int else None,
            "sha256": (
                inventory.inventory_sha256
                if isinstance(inventory.inventory_sha256, str)
                and re.fullmatch(r"[0-9a-f]{64}", inventory.inventory_sha256)
                else None
            ),
            "total_bytes": inventory.total_bytes if type(inventory.total_bytes) is int else None,
        },
        "policy_sha256": policy_sha256,
        "prefix": matched.prefix,
        "refusal_codes": list(ordered_codes),
        "retention_anchor": _format_utc(anchor),
    }
    plan_json = json.dumps(plan_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    if len(plan_json.encode("utf-8")) > policy.bounds.max_summary_bytes:
        raise PolicyError("summary_bound_exceeded")
    return RetentionDecision(
        eligible=eligible,
        refusal_codes=ordered_codes,
        retention_anchor=anchor,
        eligible_after=eligible_after,
        policy_sha256=policy_sha256,
        plan_json=plan_json,
        plan_sha256=hashlib.sha256(plan_json.encode("utf-8")).hexdigest(),
    )


def _parse_entry(value: object) -> CheckpointEntry:
    raw = _require_mapping(value, "invalid_type")
    checkpoint_id = raw.get("checkpoint_id")
    if not isinstance(checkpoint_id, str):
        raise PolicyError("invalid_type")
    expected = _EXPECTED_ENTRIES.get(checkpoint_id)
    if expected is None:
        raise PolicyError("invalid_checkpoint_id")
    if expected["durability"] == "durable_stream":
        allowed_keys = _DURABLE_ENTRY_KEYS
    elif "scales" in expected:
        allowed_keys = _GENERATION_ENTRY_KEYS
    else:
        allowed_keys = _ENTRY_KEYS
    _require_exact_keys(raw, allowed_keys)

    for key in _ENTRY_KEYS - {"terminal_states", "retention_seconds", "lifecycle"}:
        if not isinstance(raw[key], str) or not raw[key]:
            raise PolicyError("invalid_type")
    lifecycle = raw["lifecycle"]
    allowed_lifecycle = {"active", "retired"} if expected["durability"] == "durable_stream" else {"active"}
    if lifecycle not in allowed_lifecycle:
        raise PolicyError("invalid_lifecycle")

    retired_at: datetime | None = None
    retirement_review: str | None = None
    if expected["durability"] == "durable_stream":
        raw_retired_at = raw["retired_at"]
        raw_retirement_review = raw["retirement_review"]
        if lifecycle == "active":
            if raw_retired_at is not None or raw_retirement_review is not None:
                raise PolicyError("invalid_retirement_transition")
        else:
            retired_at = _parse_utc_string(raw_retired_at)
            if (
                retired_at is None
                or not isinstance(raw_retirement_review, str)
                or _SAFE_REVIEW.fullmatch(raw_retirement_review) is None
            ):
                raise PolicyError("invalid_retirement_transition")
            retirement_review = raw_retirement_review

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
        retired_at=retired_at,
        retirement_review=retirement_review,
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


def _evaluate_lease(
    policy: CheckpointPolicy,
    facts: EvaluationInput,
    matched: MatchedCheckpoint,
    entry: CheckpointEntry,
    evaluated_at_valid: bool,
    lease: LeaseFacts,
    terminal: TerminalFacts | None,
    refusal_codes: list[str],
) -> None:
    if type(lease.malformed) is not bool or type(lease.conflicting) is not bool:
        refusal_codes.append("invalid_fact_type")
    if lease.malformed is True:
        refusal_codes.append("lease_malformed")
    if lease.conflicting is True:
        refusal_codes.append("lease_conflicting")
    if (
        not isinstance(lease.checkpoint_id, str)
        or not isinstance(lease.prefix, str)
        or lease.checkpoint_id != matched.checkpoint_id
        or lease.prefix != facts.prefix
    ):
        refusal_codes.append("lease_identity_mismatch")
    if not isinstance(lease.etag, str) or not lease.etag or len(lease.etag) > 128:
        refusal_codes.append("lease_etag_invalid")
    for value in (lease.acquired_at, lease.heartbeat_at, lease.expires_at):
        if not _is_exact_utc(value):
            refusal_codes.append("invalid_utc_timestamp")
        elif evaluated_at_valid and _is_future_clock(value, facts.evaluated_at, policy.lease.future_tolerance_seconds):
            refusal_codes.append("future_clock")
    if all(_is_exact_utc(value) for value in (lease.acquired_at, lease.heartbeat_at, lease.expires_at)):
        if not (
            lease.acquired_at <= lease.heartbeat_at <= lease.expires_at
            and lease.expires_at - lease.heartbeat_at == timedelta(seconds=policy.lease.ttl_seconds)
        ):
            refusal_codes.append("lease_clock_invalid")
    lease_state = lease.state if isinstance(lease.state, str) else None
    if lease_state is None:
        refusal_codes.append("invalid_fact_type")
    if lease_state == "active":
        if evaluated_at_valid and _is_exact_utc(lease.expires_at):
            refusal_codes.append(
                "lease_active" if lease.expires_at >= facts.evaluated_at else "lease_expired_active_uncertain"
            )
        else:
            refusal_codes.append("lease_active")
    elif lease_state not in ("stopped", "completed", "retired"):
        refusal_codes.append("lease_state_invalid")

    expected_states = {
        "durable_stream": {"stopped", "retired"},
        "generation_reproducibility": {"completed", "stopped"},
        "disposable_acceptance": {"stopped"},
    }[entry.durability]
    terminal_state = terminal.state if terminal is not None and isinstance(terminal.state, str) else None
    if lease_state not in expected_states or terminal_state not in expected_states or lease_state != terminal_state:
        refusal_codes.append("invalid_lease_terminal_state")
    if (
        terminal is not None
        and _is_exact_utc(terminal.occurred_at)
        and _is_exact_utc(lease.heartbeat_at)
        and lease.heartbeat_at > terminal.occurred_at
    ):
        refusal_codes.append("lease_terminal_clock_conflict")


def _evaluate_inventory(
    policy: CheckpointPolicy,
    facts: EvaluationInput,
    evaluated_at_valid: bool,
    inventory: InventorySummary,
    refusal_codes: list[str],
) -> None:
    if (
        type(inventory.object_count) is not int
        or inventory.object_count < 1
        or inventory.object_count > policy.bounds.max_objects
        or type(inventory.total_bytes) is not int
        or inventory.total_bytes < 1
        or inventory.total_bytes > policy.bounds.max_bytes
        or not isinstance(inventory.inventory_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", inventory.inventory_sha256) is None
    ):
        refusal_codes.append("inventory_invalid")
    if not _is_exact_utc(inventory.newest_last_modified):
        refusal_codes.append("invalid_utc_timestamp")
    elif evaluated_at_valid and _is_future_clock(
        inventory.newest_last_modified, facts.evaluated_at, policy.lease.future_tolerance_seconds
    ):
        refusal_codes.append("future_clock")
    if type(inventory.changed_since_plan) is not bool or type(inventory.partial_retry_confined) is not bool:
        refusal_codes.append("invalid_fact_type")
    if inventory.changed_since_plan is True:
        refusal_codes.append("inventory_changed")
    if inventory.partial_retry_confined is not True:
        refusal_codes.append("partial_retry_broadened")


def _evaluate_terminal(
    policy: CheckpointPolicy,
    facts: EvaluationInput,
    entry: CheckpointEntry,
    matched: MatchedCheckpoint,
    terminal: TerminalFacts,
    refusal_codes: list[str],
) -> None:
    if not _is_exact_utc(terminal.occurred_at):
        refusal_codes.append("invalid_utc_timestamp")
    elif _is_future_clock(terminal.occurred_at, facts.evaluated_at, policy.lease.future_tolerance_seconds):
        refusal_codes.append("future_clock")
    if not isinstance(terminal.state, str):
        refusal_codes.append("invalid_fact_type")
    if terminal.state not in entry.terminal_states:
        refusal_codes.append("invalid_terminal_state")
    for value in (
        terminal.recovery_approved,
        terminal.source_available,
        terminal.sink_disposition_approved,
        terminal.exclusive_run,
        terminal.successful,
    ):
        if type(value) is not bool:
            refusal_codes.append("invalid_fact_type")
    if terminal.recovery_approved is not True:
        refusal_codes.append("recovery_not_approved")
    if terminal.source_available is not True:
        refusal_codes.append("source_unavailable")
    if terminal.sink_disposition_approved is not True:
        refusal_codes.append("sink_disposition_not_approved")

    if entry.durability == "durable_stream" and entry.lifecycle == "retired":
        if not isinstance(terminal.retirement_review, str) or not terminal.retirement_review:
            refusal_codes.append("retirement_review_missing")
        if not _is_exact_utc(entry.retired_at):
            refusal_codes.append("retirement_clock_missing")
        elif _is_future_clock(entry.retired_at, facts.evaluated_at, policy.lease.future_tolerance_seconds):
            refusal_codes.append("future_clock")
        if not isinstance(entry.retirement_review, str) or not entry.retirement_review:
            refusal_codes.append("registry_retirement_review_missing")
        elif _SAFE_REVIEW.fullmatch(entry.retirement_review) is None:
            refusal_codes.append("registry_retirement_review_invalid")
        elif terminal.retirement_review != entry.retirement_review:
            refusal_codes.append("retirement_review_mismatch")
    elif entry.durability == "generation_reproducibility":
        if not isinstance(terminal.generation, Mapping) or dict(terminal.generation) != dict(matched.generation):
            refusal_codes.append("generation_identity_mismatch")
    elif entry.durability == "disposable_acceptance":
        if terminal.exclusive_run is not True:
            refusal_codes.append("exclusive_run_required")
        if terminal.successful is not True:
            refusal_codes.append("successful_run_required")


def _retention_anchor(
    entry: CheckpointEntry,
    lease: LeaseFacts | None,
    terminal: TerminalFacts | None,
    inventory: InventorySummary,
) -> datetime | None:
    values = [inventory.newest_last_modified]
    if lease is not None:
        values.append(lease.heartbeat_at)
    if terminal is not None:
        values.append(terminal.occurred_at)
    if entry.durability == "durable_stream" and entry.lifecycle == "retired":
        values.append(entry.retired_at)
    if not all(_is_exact_utc(value) for value in values):
        return None
    return max(values)


def _is_exact_utc(value: object) -> bool:
    return (
        isinstance(value, datetime)
        and value.tzinfo is not None
        and value.utcoffset() == timedelta(0)
        and value.microsecond == 0
    )


def _format_utc(value: datetime | None) -> str | None:
    if value is None or not _is_exact_utc(value):
        return None
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_utc_string(value: object) -> datetime | None:
    if not isinstance(value, str) or re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", value) is None:
        return None
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return parsed if _is_exact_utc(parsed) else None


def _is_future_clock(value: datetime, evaluated_at: datetime, tolerance_seconds: int) -> bool:
    if not _is_exact_utc(value) or not _is_exact_utc(evaluated_at):
        return False
    return (value - evaluated_at).total_seconds() > tolerance_seconds


def _safe_add_seconds(value: datetime | None, seconds: int) -> datetime | None:
    if value is None:
        return None
    try:
        return value + timedelta(seconds=seconds)
    except OverflowError:
        return None


def _policy_sha256(policy: CheckpointPolicy) -> str:
    payload = {
        "version": policy.version,
        "bucket": policy.bucket,
        "control_prefix": policy.control_prefix,
        "lease": {key: getattr(policy.lease, key) for key in _LEASE_VALUES},
        "bounds": {key: getattr(policy.bounds, key) for key in _BOUND_VALUES},
        "checkpoints": [
            {
                "checkpoint_id": entry.checkpoint_id,
                "prefix": entry.prefix,
                "owner": entry.owner,
                "workload": entry.workload,
                "source": entry.source,
                "sink": entry.sink,
                "lifecycle": entry.lifecycle,
                "durability": entry.durability,
                "terminal_states": list(entry.terminal_states),
                "retention_seconds": entry.retention_seconds,
                "recovery_class": entry.recovery_class,
                "sink_disposition": entry.sink_disposition,
                "concurrent_writers": entry.concurrent_writers,
                "retirement_authorization": entry.retirement_authorization,
                **(
                    {
                        "retired_at": _format_utc(entry.retired_at),
                        "retirement_review": entry.retirement_review,
                    }
                    if entry.durability == "durable_stream"
                    else {}
                ),
                **({"scales": list(entry.scales)} if entry.scales else {}),
            }
            for entry in policy.entries.values()
        ],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
