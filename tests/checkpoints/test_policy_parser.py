from __future__ import annotations

import importlib
import io
import traceback
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
POLICY_PATH = ROOT / "checkpoints" / "retention-policy.yaml"

EXPECTED_IDS = (
    "streaming-events-v1",
    "streaming-event-windows-v1",
    "streaming-online-retail-cdc-v1",
    "streaming-gh-archive-file-v1",
    "go-live-streaming-test-v1",
)


def _api():
    """Load the wished-for production boundary inside each behavioral test."""
    return importlib.import_module("scripts.checkpoints.policy")


VALID_POLICY = """\
version: 1
bucket: checkpoints
control_prefix: _retention/
lease:
  heartbeat_seconds: 60
  ttl_seconds: 600
  future_tolerance_seconds: 300
  quiescence_seconds: 900
bounds:
  max_pages: 100
  max_objects: 100000
  max_bytes: 10737418240
  max_delete_keys: 1000
  max_active_seconds: 900
  max_summary_bytes: 65536
  max_manifest_shard_bytes: 1048576
checkpoints:
  - checkpoint_id: streaming-events-v1
    prefix: events/
    owner: Streaming Data Engineering
    workload: streaming_ingest-events-spark-iceberg
    source: redpanda:events
    sink: lakehouse.bronze.events
    lifecycle: active
    retired_at: null
    retirement_review: null
    durability: durable_stream
    terminal_states: [stopped, retired]
    retention_seconds: 2592000
    recovery_class: coordinated_replay
    sink_disposition: snapshot_or_reset
    concurrent_writers: forbidden
    retirement_authorization: required
  - checkpoint_id: streaming-event-windows-v1
    prefix: event_windows/
    owner: Streaming Data Engineering
    workload: streaming_windows-events-spark-iceberg
    source: redpanda:events
    sink: lakehouse.gold.event_windows
    lifecycle: active
    retired_at: null
    retirement_review: null
    durability: durable_stream
    terminal_states: [stopped, retired]
    retention_seconds: 2592000
    recovery_class: coordinated_replay
    sink_disposition: snapshot_or_reset
    concurrent_writers: forbidden
    retirement_authorization: required
  - checkpoint_id: streaming-online-retail-cdc-v1
    prefix: online_retail_cdc/
    owner: Streaming Data Engineering
    workload: cdc_streaming-online_retail-spark-iceberg
    source: redpanda:online_retail_cdc
    sink: lakehouse.silver.online_retail_cdc
    lifecycle: active
    retired_at: null
    retirement_review: null
    durability: durable_stream
    terminal_states: [stopped, retired]
    retention_seconds: 2592000
    recovery_class: coordinated_replay
    sink_disposition: snapshot_or_reset
    concurrent_writers: forbidden
    retirement_authorization: required
  - checkpoint_id: streaming-gh-archive-file-v1
    prefix: gh_events_file/{scale}/{publication_id}/{manifest_sha256}/
    scales: [tiny, small, medium]
    owner: Streaming Data Engineering Education
    workload: streaming_ingest-gh_archive-spark-iceberg
    source: resolver:gh_archive
    sink: lakehouse.bronze.gh_events_stream
    lifecycle: active
    durability: generation_reproducibility
    terminal_states: [completed, stopped]
    retention_seconds: 1209600
    recovery_class: exact_generation_replay
    sink_disposition: reset_required
    concurrent_writers: forbidden
    retirement_authorization: not_applicable
  - checkpoint_id: go-live-streaming-test-v1
    prefix: streaming_test/{run_uuid}/
    owner: Lab Acceptance Engineering
    workload: go-live-streaming-test
    source: bounded_synthetic
    sink: s3a://lakehouse/bronze/streaming_test
    lifecycle: active
    durability: disposable_acceptance
    terminal_states: [successful, stopped]
    retention_seconds: 86400
    recovery_class: disposable_recreate
    sink_disposition: reset_required
    concurrent_writers: forbidden
    retirement_authorization: not_applicable
"""


def test_canonical_policy_file_exists_and_loads_exact_contract():
    api = _api()
    policy = api.load_policy(POLICY_PATH)

    assert policy.version == 1
    assert policy.bucket == "checkpoints"
    assert policy.control_prefix == "_retention/"
    assert tuple(policy.entries) == EXPECTED_IDS
    assert policy.lease.heartbeat_seconds == 60
    assert policy.lease.ttl_seconds == 600
    assert policy.lease.future_tolerance_seconds == 300
    assert policy.lease.quiescence_seconds == 900
    assert policy.bounds.max_pages == 100
    assert policy.bounds.max_objects == 100_000
    assert policy.bounds.max_bytes == 10_737_418_240
    assert policy.bounds.max_delete_keys == 1_000
    assert policy.bounds.max_active_seconds == 900
    assert policy.bounds.max_summary_bytes == 65_536
    assert policy.bounds.max_manifest_shard_bytes == 1_048_576


def test_parser_preserves_exact_owner_source_sink_and_recovery_contract():
    policy = _api().parse_policy(VALID_POLICY)

    assert {
        checkpoint_id: (
            entry.prefix,
            entry.owner,
            entry.durability,
            entry.source,
            entry.sink,
            entry.retention_seconds,
            entry.recovery_class,
            entry.sink_disposition,
        )
        for checkpoint_id, entry in policy.entries.items()
    } == {
        "streaming-events-v1": (
            "events/",
            "Streaming Data Engineering",
            "durable_stream",
            "redpanda:events",
            "lakehouse.bronze.events",
            2_592_000,
            "coordinated_replay",
            "snapshot_or_reset",
        ),
        "streaming-event-windows-v1": (
            "event_windows/",
            "Streaming Data Engineering",
            "durable_stream",
            "redpanda:events",
            "lakehouse.gold.event_windows",
            2_592_000,
            "coordinated_replay",
            "snapshot_or_reset",
        ),
        "streaming-online-retail-cdc-v1": (
            "online_retail_cdc/",
            "Streaming Data Engineering",
            "durable_stream",
            "redpanda:online_retail_cdc",
            "lakehouse.silver.online_retail_cdc",
            2_592_000,
            "coordinated_replay",
            "snapshot_or_reset",
        ),
        "streaming-gh-archive-file-v1": (
            "gh_events_file/{scale}/{publication_id}/{manifest_sha256}/",
            "Streaming Data Engineering Education",
            "generation_reproducibility",
            "resolver:gh_archive",
            "lakehouse.bronze.gh_events_stream",
            1_209_600,
            "exact_generation_replay",
            "reset_required",
        ),
        "go-live-streaming-test-v1": (
            "streaming_test/{run_uuid}/",
            "Lab Acceptance Engineering",
            "disposable_acceptance",
            "bounded_synthetic",
            "s3a://lakehouse/bronze/streaming_test",
            86_400,
            "disposable_recreate",
            "reset_required",
        ),
    }


@pytest.mark.parametrize(
    ("broken", "replacement", "code"),
    [
        ("version: 1\n", "version: true\n", "invalid_type"),
        ("bucket: checkpoints\n", "bucket: checkpoints\nunknown: value\n", "unknown_field"),
        ("max_pages: 100\n", "max_pages: 0\n", "invalid_bound"),
        ("ttl_seconds: 600\n", "ttl_seconds: 599\n", "invalid_lease_policy"),
        ("prefix: events/\n", "prefix: /events/\n", "unsafe_prefix"),
        ("prefix: events/\n", "prefix: _retention/events/\n", "control_prefix"),
        ("durability: durable_stream\n", "durability: ephemeral\n", "invalid_durability"),
        ("lifecycle: active\n", "lifecycle: forgotten\n", "invalid_lifecycle"),
        ("retention_seconds: 2592000\n", "retention_seconds: 42\n", "invalid_retention"),
        ("terminal_states: [stopped, retired]\n", "terminal_states: [completed]\n", "invalid_terminal_states"),
    ],
)
def test_parser_rejects_invalid_types_bounds_paths_and_class_combinations(broken: str, replacement: str, code: str):
    api = _api()
    text = VALID_POLICY.replace(broken, replacement, 1)

    with pytest.raises(api.PolicyError, match=code):
        api.parse_policy(text)


def test_parser_rejects_duplicate_yaml_mapping_keys_before_construction():
    api = _api()
    text = VALID_POLICY.replace("version: 1\n", "version: 1\nversion: 1\n", 1)

    with pytest.raises(api.PolicyError, match="duplicate_key"):
        api.parse_policy(text)


def test_parser_rejects_duplicate_checkpoint_ids_and_overlapping_prefixes():
    api = _api()
    duplicate_id = VALID_POLICY.replace(
        "checkpoint_id: streaming-event-windows-v1",
        "checkpoint_id: streaming-events-v1",
        1,
    )
    overlap = VALID_POLICY.replace("prefix: event_windows/", "prefix: events/child/", 1)

    with pytest.raises(api.PolicyError, match="duplicate_checkpoint_id"):
        api.parse_policy(duplicate_id)
    with pytest.raises(api.PolicyError, match="overlapping_prefix"):
        api.parse_policy(overlap)


def test_retired_durable_registry_requires_exact_reviewed_transition_facts():
    api = _api()
    retired = VALID_POLICY.replace(
        "lifecycle: active\n    retired_at: null\n    retirement_review: null\n",
        "lifecycle: retired\n"
        "    retired_at: '2026-07-01T12:00:00Z'\n"
        "    retirement_review: issue-85-reviewed-transition\n",
        1,
    )

    entry = api.parse_policy(retired).entries["streaming-events-v1"]
    assert entry.retired_at.isoformat() == "2026-07-01T12:00:00+00:00"
    assert entry.retirement_review == "issue-85-reviewed-transition"

    for broken in (
        retired.replace("retired_at: '2026-07-01T12:00:00Z'", "retired_at: null", 1),
        retired.replace("retirement_review: issue-85-reviewed-transition", "retirement_review: null", 1),
        retired.replace("2026-07-01T12:00:00Z", "2026-07-01T12:00:00.001Z", 1),
        retired.replace("issue-85-reviewed-transition", "contains spaces", 1),
    ):
        with pytest.raises(api.PolicyError, match="invalid_retirement_transition"):
            api.parse_policy(broken)


def test_active_durable_registry_rejects_premature_retirement_facts():
    api = _api()
    for broken in (
        VALID_POLICY.replace("retired_at: null", "retired_at: '2026-07-01T12:00:00Z'", 1),
        VALID_POLICY.replace("retirement_review: null", "retirement_review: issue-85", 1),
    ):
        with pytest.raises(api.PolicyError, match="invalid_retirement_transition"):
            api.parse_policy(broken)


@pytest.mark.parametrize(
    ("text", "code"),
    [
        ("x: " + "a" * 262_145, "policy_too_large"),
        ("root: &shared [1]\ncopy: *shared\n", "yaml_alias_forbidden"),
        (
            "root:\n" + "".join("  " * depth + "child:\n" for depth in range(1, 40)) + "  " * 40 + "value: 1\n",
            "yaml_depth_exceeded",
        ),
        ("root:\n" + "  - value: 1\n" * 4_100, "yaml_node_limit"),
    ],
)
def test_parser_rejects_bounded_yaml_resource_exhaustion_before_materialization(text: str, code: str):
    with pytest.raises(_api().PolicyError, match=code):
        _api().parse_policy(text)


def test_policy_file_reader_stops_at_the_same_byte_bound():
    class BoundedReadOnlyPath:
        def read_text(self, **_kwargs):
            raise AssertionError("unbounded read_text must not be called")

        def open(self, mode):
            assert mode == "rb"
            return io.BytesIO(b"x" * 262_145)

    with pytest.raises(_api().PolicyError, match="policy_too_large"):
        _api().load_policy(BoundedReadOnlyPath())


def test_malformed_yaml_exception_chain_never_contains_source_payload():
    secret = "AKIAIOSFODNN7EXAMPLE"

    with pytest.raises(_api().PolicyError, match="invalid_yaml") as failure:
        _api().parse_policy(f'version: "{secret}\n')

    assert secret not in "".join(traceback.format_exception(failure.value))
    assert failure.value.__cause__ is None


def test_direct_string_size_guard_rejects_before_utf8_encoding(monkeypatch):
    class OversizedString(str):
        def encode(self, *_args, **_kwargs):
            raise AssertionError("oversized input must reject before encoding")

    with pytest.raises(_api().PolicyError, match="policy_too_large"):
        _api().parse_policy(OversizedString("x" * 262_145))


def test_multibyte_policy_is_still_bounded_by_encoded_bytes():
    with pytest.raises(_api().PolicyError, match="policy_too_large"):
        _api().parse_policy("é" * 131_073)


@pytest.mark.parametrize(
    "prefix",
    [
        "",
        "/",
        "events",
        "s3a://checkpoints/events/",
        "../events/",
        "events/../other/",
        "events\\child/",
        "events//child/",
        "_retention/",
        "_retention/leases/streaming-events-v1.json",
        "events_stream/",
        "redpanda/atlas_stream_events/",
        "gh_events_file/",
        "gh_events_file/large/" + "a" * 32 + "/" + "b" * 64 + "/",
        "gh_events_file/tiny/" + "A" * 32 + "/" + "b" * 64 + "/",
        "gh_events_file/tiny/" + "a" * 31 + "/" + "b" * 64 + "/",
        "gh_events_file/tiny/" + "a" * 32 + "/" + "b" * 63 + "/",
        "gh_events_file/tiny/" + "a" * 32 + "/" + "b" * 64 + "/extra/",
        "streaming_test/",
        "streaming_test/550E8400-E29B-41D4-A716-446655440000/",
        "streaming_test/{550e8400-e29b-41d4-a716-446655440000}/",
        "streaming_test/550e8400e29b41d4a716446655440000/",
        "streaming_test/550e8400-e29b-41d4-a716-446655440000/extra/",
        "streaming_test/../550e8400-e29b-41d4-a716-446655440000/",
        "streaming_test//550e8400-e29b-41d4-a716-446655440000/",
        "streaming_test/550e8400-e29b-41d4-a716-44665544000é/",
        "streaming_test_sibling/550e8400-e29b-41d4-a716-446655440000/",
    ],
)
def test_match_prefix_rejects_unknown_root_control_and_malformed_generation(prefix: str):
    policy = _api().parse_policy(VALID_POLICY)

    with pytest.raises(_api().PolicyError, match="unknown_prefix|unsafe_prefix|control_prefix"):
        policy.match_prefix(prefix)


def test_match_prefix_accepts_only_exact_fixed_and_generation_leaves():
    policy = _api().parse_policy(VALID_POLICY)
    generation = "gh_events_file/tiny/" + "a" * 32 + "/" + "b" * 64 + "/"
    run_uuid = "550e8400-e29b-41d4-a716-446655440000"

    assert policy.match_prefix("events/").checkpoint_id == "streaming-events-v1"
    assert policy.match_prefix("event_windows/").checkpoint_id == ("streaming-event-windows-v1")
    assert policy.match_prefix("online_retail_cdc/").checkpoint_id == ("streaming-online-retail-cdc-v1")
    scratch = policy.match_prefix(f"streaming_test/{run_uuid}/")
    assert scratch.checkpoint_id == "go-live-streaming-test-v1"
    assert scratch.prefix == f"streaming_test/{run_uuid}/"
    assert scratch.generation == {"run_uuid": run_uuid}
    match = policy.match_prefix(generation)
    assert match.checkpoint_id == "streaming-gh-archive-file-v1"
    assert match.prefix == generation
    assert match.generation == {
        "scale": "tiny",
        "publication_id": "a" * 32,
        "manifest_sha256": "b" * 64,
    }
