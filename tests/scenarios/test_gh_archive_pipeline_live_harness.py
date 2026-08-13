from __future__ import annotations

import gzip
import hashlib
import importlib.util
import io
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "gh_archive_flatten_sessionization_live_harness",
    ROOT / "tests/scenarios/test_gh_archive_pipeline_live.py",
)
live = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(live)


def _paged_api(runs, *, total_entries=None):
    calls = []

    def api(_method, path, _body=None):
        query = parse_qs(urlparse(path).query)
        offset = int(query.get("offset", [0])[0])
        limit = int(query.get("limit", [100])[0])
        calls.append((offset, limit))
        return {
            "dag_runs": runs[offset:offset + limit],
            "total_entries": len(runs) if total_entries is None else total_entries,
        }

    return api, calls


def test_owned_stack_rejects_preexisting_project_without_mutation():
    commands = []

    def runner(*command, **_kwargs):
        commands.append(command)

    with pytest.raises(RuntimeError, match="already exists"):
        with live._owned_stack(runner=runner, probe=lambda: ("data-eng-lab-minio",)):
            raise AssertionError("must not enter")
    assert commands == []


def test_owned_stack_rejects_stopped_project_container_before_any_mutation():
    commands = []

    def runner(*command, **_kwargs):
        commands.append(command)
        if command[:2] == ("docker", "ps"):
            stdout = "data-eng-lab-minio\n" if "--all" in command else ""
            return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    with pytest.raises(RuntimeError, match="already exists"):
        with live._owned_stack(runner=runner, probe=lambda: live._stack_containers(runner=runner)):
            commands.append(("AIRFLOW_PAUSE_MUTATION",))

    assert len(commands) == 1
    assert commands[0][:3] == ("docker", "ps", "--all")


def test_owned_stack_cleans_up_failure_and_preserves_primary_diagnostic():
    commands = []

    def runner(*command, **_kwargs):
        commands.append(command)
        if command == ("./scripts/stop-all.sh",):
            raise RuntimeError("cleanup diagnostic")

    with pytest.raises(ValueError, match="primary") as caught:
        with live._owned_stack(runner=runner, probe=tuple):
            raise ValueError("primary")
    assert commands == [("./scripts/start-all.sh",), ("./scripts/stop-all.sh",)]
    assert any("cleanup diagnostic" in note for note in caught.value.__notes__)


def test_owned_stack_rejects_lingering_project_container_after_cleanup():
    probes = iter([(), ("data-eng-lab-minio",)])

    with pytest.raises(RuntimeError, match="cleanup left project containers"):
        with live._owned_stack(
            runner=lambda *command, **_kwargs: subprocess.CompletedProcess(
                command, 0, stdout="", stderr=""
            ),
            probe=lambda: next(probes),
        ):
            pass


def test_paused_acceptance_records_and_restores_initial_state_without_unpausing_during_body():
    calls = []

    def api(method, path, body=None):
        calls.append((method, path, body))
        if method == "GET":
            return {"is_paused": False}
        return {}

    with live._paused_dag(api=api) as initial:
        assert initial is False
        assert calls[-1][2] == {"is_paused": True}
        assert not any(call[2] == {"is_paused": False} for call in calls)
    assert calls[-1][2] == {"is_paused": False}


def test_owned_run_guard_rejects_any_unexpected_acceptance_window_run():
    runs = [
        {"dag_run_id": "owned-first", "state": "success", "start_date": "2026-08-12T20:00:01Z"},
        {"dag_run_id": "scheduled-third", "state": "queued", "start_date": "2026-08-12T20:00:02Z"},
    ]

    with pytest.raises(AssertionError, match="scheduled-third"):
        live._assert_owned_runs(
            lambda *_args, **_kwargs: {"dag_runs": runs, "total_entries": len(runs)},
            "2026-08-12T20:00:00Z",
            {"owned-first"},
        )


def test_owned_run_guard_accepts_exact_expected_runs_and_requires_terminal_final_state():
    runs = [
        {"dag_run_id": "historical", "state": "failed", "start_date": "2026-08-12T19:00:00Z"},
        {"dag_run_id": "owned-first", "state": "success", "start_date": "2026-08-12T20:00:01Z"},
        {"dag_run_id": "owned-second", "state": "success", "start_date": "2026-08-12T20:00:02Z"},
    ]
    def api(*_args, **_kwargs):
        return {"dag_runs": runs, "total_entries": len(runs)}

    found = live._assert_owned_runs(
        api,
        "2026-08-12T20:00:00Z",
        {"owned-first", "owned-second"},
        require_terminal=True,
    )
    assert set(found) == {"owned-first", "owned-second"}


def test_owned_run_guard_uses_pre_acceptance_id_baseline_not_skewed_logical_date():
    historical_probe = {
        "dag_run_id": "historical-future-logical-date",
        "state": "success",
        "start_date": None,
        "logical_date": "2026-08-12T20:30:00Z",
    }
    found = live._assert_owned_runs(
        lambda *_args, **_kwargs: {"dag_runs": [historical_probe], "total_entries": 1},
        "2026-08-12T20:28:00Z",
        set(),
        baseline={"historical-future-logical-date"},
    )
    assert found == {}


def test_run_inventory_rejects_unexpected_active_run_only_present_on_second_page():
    historical = [
        {"dag_run_id": f"historical-{index}", "state": "success", "start_date": "2026-08-11T20:00:00Z"}
        for index in range(100)
    ]
    unexpected = {
        "dag_run_id": "unexpected-page-two",
        "state": "queued",
        "start_date": "2026-08-12T20:00:01Z",
    }
    api, calls = _paged_api(historical + [unexpected])

    with pytest.raises(AssertionError, match="unexpected-page-two"):
        live._assert_owned_runs(api, "2026-08-12T20:00:00Z", set())
    assert calls == [(0, 100), (100, 100)]


def test_run_inventory_returns_complete_valid_multipage_collection():
    runs = [
        {"dag_run_id": f"run-{index}", "state": "success", "start_date": "2026-08-11T20:00:00Z"}
        for index in range(205)
    ]
    api, calls = _paged_api(runs)

    found = live._list_runs(api)
    assert [run["dag_run_id"] for run in found] == [run["dag_run_id"] for run in runs]
    assert calls == [(0, 100), (100, 100), (200, 100)]


@pytest.mark.parametrize(
    ("documents", "message"),
    [
        ([{"dag_runs": []}], "total_entries"),
        ([{"dag_runs": "not-a-list", "total_entries": 1}], "dag_runs"),
        ([{"dag_runs": [], "total_entries": 1}], "progress"),
        ([{"dag_runs": [{"dag_run_id": "same"}], "total_entries": 2}] * 2, "duplicate"),
        ([{"dag_runs": [], "total_entries": 1001}], "safe maximum"),
    ],
)
def test_run_inventory_fails_closed_on_malformed_nonprogress_duplicate_or_over_limit(documents, message):
    responses = iter(documents)
    with pytest.raises(AssertionError, match=message):
        live._list_runs(lambda *_args, **_kwargs: next(responses))


def test_run_inventory_rejects_total_changes_and_enforces_request_bound():
    changing = iter([
        {"dag_runs": [{"dag_run_id": "first"}], "total_entries": 2},
        {"dag_runs": [{"dag_run_id": "second"}], "total_entries": 3},
    ])
    with pytest.raises(AssertionError, match="changed during pagination"):
        live._list_runs(lambda *_args, **_kwargs: next(changing))

    pages = iter([
        {"dag_runs": [{"dag_run_id": f"run-{index}"}], "total_entries": 11}
        for index in range(10)
    ])
    with pytest.raises(AssertionError, match="safe request count 10"):
        live._list_runs(lambda *_args, **_kwargs: next(pages))


def test_resolver_failure_propagates_without_any_dataset_mutation_or_retry():
    calls = []

    def runner(*command, **_kwargs):
        calls.append(command)
        raise subprocess.CalledProcessError(1, command, output="generic resolver failure")

    with pytest.raises(subprocess.CalledProcessError, match="resolve_dataset.py"):
        live._resolve_or_publish_tiny(runner=runner)
    assert calls == [
        ("uv", "run", "python", "scripts/resolve_dataset.py", "gh_archive", "--scale", "tiny")
    ]


def test_resolver_does_not_refresh_an_existing_verified_publication():
    calls = []
    resolved = '{"dataset":"gh_archive","scale":"tiny","objects":[]}'

    def runner(*command, **_kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout=resolved, stderr="")

    live._resolve_or_publish_tiny(runner=runner)
    assert calls == [
        ("uv", "run", "python", "scripts/resolve_dataset.py", "gh_archive", "--scale", "tiny"),
        (
            "uv", "run", "python", "scripts/download_datasets.py",
            "--scale", "tiny", "--only", "gh_archive", "--verify-only",
        ),
        ("uv", "run", "python", "scripts/resolve_dataset.py", "gh_archive", "--scale", "tiny"),
    ]


def test_resolver_rejects_pointer_change_across_read_only_verification():
    calls = []
    responses = iter(
        [
            '{"dataset":"gh_archive","scale":"tiny","publication_id":"before","objects":[]}',
            "",
            '{"dataset":"gh_archive","scale":"tiny","publication_id":"after","objects":[]}',
        ]
    )

    def runner(*command, **_kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout=next(responses), stderr="")

    with pytest.raises(AssertionError, match="changed during verify-only"):
        live._resolve_or_publish_tiny(runner=runner)
    assert len(calls) == 3
    assert not any("--refresh" in command for command in calls)


def _source_fixture(records):
    content = gzip.compress(
        b"".join(
            json.dumps(record, separators=(",", ":")).encode() + b"\n"
            for record in records
        )
    )

    class Client:
        def get_object(self, *, Bucket, Key):
            assert Bucket == "landing"
            assert Key == "gh_archive/_generations/plan/publication/2023-01-01-0.json.gz"
            return {"Body": io.BytesIO(content)}

    resolved = {
        "objects": [
            {
                "object_name": "2023-01-01-0.json.gz",
                "uri": "s3://landing/gh_archive/_generations/plan/publication/2023-01-01-0.json.gz",
                "size_bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
        ]
    }
    return Client(), resolved


def _event(event_id="1", created_at="2023-01-01T00:00:00Z"):
    return {
        "id": event_id,
        "type": "PushEvent",
        "actor": {"login": "octocat"},
        "repo": {"name": "openai/example"},
        "created_at": created_at,
        "payload": {"ignored": True},
    }


def test_source_inventory_preserves_identical_duplicates_and_counts_them():
    duplicate = _event("same")
    client, resolved = _source_fixture(
        [duplicate, duplicate, _event("other", "2023-01-01T00:00:01Z")]
    )
    assert live._source_inventory(client, resolved) == live.SourceEvidence(
        row_count=3, distinct_ids=2, exact_duplicate_rows=1, distinct_actors=1,
    )


def test_live_identity_is_frozen_to_the_reviewed_canonical_replay():
    assert live.EXPECTED_LIVE_IDENTITY == {
        "jar_sha256": "b826e218d8ad4a9a4dadd1b835e3533c9649735725cfb3f71508e7e04e952c04",
        "plan_id": "8ab812c3621cc3dae68989d9f24134351ea9683453133b31feaff579d0fa3e7f",
        "publication_id": "e53a481df5d54c6eabc645838fb2f2ba",
        "manifest_sha256": "998ec39bc61dca1b460e4b851d718a5347b8c7e575b96dd1e3ec62fd0b791678",
        "source_size_bytes": 59_785_519,
        "source_sha256": "2b0c0cc3b067f61c0f39d7623517904d95d22ef9d5c998953050a0b78adb6258",
        "row_count": 101_917,
        "distinct_ids": 101_916,
        "exact_duplicate_rows": 1,
        "distinct_actors": 16_331,
        "session_starts": 16_767,
        "events_checksum": "7ea82e3d0b5bad96",
        "sessions_checksum": "36136a1cab232348",
    }


def test_full_session_oracle_requires_exact_duplicate_aware_multiset_equality():
    captured = []

    def query(sql):
        captured.append(sql)
        return [[0]]

    assert live._session_oracle(query=query) == 0
    sql = captured[0]
    lag_window = sql.split("lag(created_at)", 1)[1].split("AS previous_created_at", 1)[0]
    assert "ROWS BETWEEN" not in lag_window
    for required in (
        "lag(created_at)",
        "date_diff('second', previous_created_at, created_at) > 1800",
        "sum(new_session)",
        "GROUP BY id, type, actor_login, repo_name, created_at",
        "IS NOT DISTINCT FROM",
        "expected_multiplicity",
        "actual_multiplicity",
        'lakehouse.silver.gh_events',
        'lakehouse.silver.gh_sessions',
    ):
        assert required in sql


@pytest.mark.parametrize(
    ("records", "message"),
    [
        ([{key: value for key, value in _event().items() if key != "id"}], "missing id"),
        ([{**_event(), "id": 7}], "invalid id"),
        ([{**_event(), "actor": {"login": None}}], "invalid actor.login"),
        ([{**_event(), "repo": {}}], "missing repo.name"),
        ([_event(created_at="2023-01-01T00:00:00+00:00")], "whole-second UTC"),
        ([_event(created_at="2023-01-01T00:00:00.000Z")], "whole-second UTC"),
        ([_event("same"), {**_event("same"), "type": "CreateEvent"}], "conflicting event ID"),
    ],
)
def test_source_row_count_rejects_invalid_consumed_values(records, message):
    client, resolved = _source_fixture(records)
    with pytest.raises(AssertionError, match=message):
        live._source_inventory(client, resolved)


def test_source_row_count_rejects_size_digest_invalid_gzip_and_duplicate_json_keys():
    client, resolved = _source_fixture([_event()])
    wrong_size = {**resolved, "objects": [{**resolved["objects"][0], "size_bytes": 1}]}
    with pytest.raises(AssertionError, match="size"):
        live._source_inventory(client, wrong_size)
    wrong_digest = {**resolved, "objects": [{**resolved["objects"][0], "sha256": "0" * 64}]}
    with pytest.raises(AssertionError, match="digest"):
        live._source_inventory(client, wrong_digest)

    invalid = b"not gzip"
    bad_client, bad_resolved = _source_fixture([_event()])
    bad_client.get_object = lambda **_kwargs: {"Body": io.BytesIO(invalid)}
    bad_resolved["objects"][0].update(
        size_bytes=len(invalid), sha256=hashlib.sha256(invalid).hexdigest()
    )
    with pytest.raises(AssertionError, match="gzip"):
        live._source_inventory(bad_client, bad_resolved)

    duplicate = gzip.compress(
        b'{"id":"1","id":"2","type":"PushEvent","actor":{"login":"a"},'
        b'"repo":{"name":"r"},"created_at":"2023-01-01T00:00:00Z"}\n'
    )
    duplicate_client, duplicate_resolved = _source_fixture([_event()])
    duplicate_client.get_object = lambda **_kwargs: {"Body": io.BytesIO(duplicate)}
    duplicate_resolved["objects"][0].update(
        size_bytes=len(duplicate), sha256=hashlib.sha256(duplicate).hexdigest()
    )
    with pytest.raises(AssertionError, match="duplicate JSON key"):
        live._source_inventory(duplicate_client, duplicate_resolved)


def test_pointer_snapshot_binds_nonempty_body_and_etag():
    class Client:
        def __init__(self, etag='"pointer-etag"'):
            self.etag = etag

        def get_object(self, *, Bucket, Key):
            assert Bucket == "landing"
            assert Key == "_data-eng-locks/current/gh_archive.json"
            return {"Body": io.BytesIO(b'{"dataset":"gh_archive"}'), "ETag": self.etag}

    assert live._pointer_snapshot(Client()) == (
        b'{"dataset":"gh_archive"}',
        '"pointer-etag"',
    )
    with pytest.raises(AssertionError, match="ETag"):
        live._pointer_snapshot(Client(etag=None))


def test_live_harness_is_gh_archive_specific_and_never_refreshes_the_pointer():
    text = (ROOT / "tests/scenarios/test_gh_archive_pipeline_live.py").read_text(encoding="utf-8")
    assert "tpch" not in text.lower()
    assert "--refresh" not in text
    for value in (
        'DAG_ID = "gh_archive_flatten_sessionization"',
        'key = "gh-archive-pipeline/0.1.0/app.jar"',
        '"gh_events"',
        '"gh_sessions"',
        '"2023-01-01-0.json.gz"',
        '"gh_archive_consumed_fields"',
        '"lakehouse.silver.gh_events"',
        '"lakehouse.silver.gh_sessions"',
        '"created_at:timestamptz"',
        '"previous_created_at:timestamptz"',
        "first_snapshot_ids",
        "second_snapshot_ids",
        "date_diff('second', previous_created_at, created_at) > 1800",
        'EXPECTED_LIVE_IDENTITY["distinct_actors"]',
        'EXPECTED_LIVE_IDENTITY["session_starts"]',
        'EXPECTED_LIVE_IDENTITY["events_checksum"]',
        'EXPECTED_LIVE_IDENTITY["sessions_checksum"]',
        "_session_oracle() == 0",
        "len(set(first_drivers + second_drivers)) == 4",
    ):
        assert value in text


def test_paused_dags_test_discovers_exactly_one_terminal_api_run_and_two_drivers():
    calls = []
    driver_sets = iter([{"old-driver"}, {"old-driver", "flatten-driver", "session-driver"}])
    api_calls = 0

    def api(_method, _path, _body=None):
        nonlocal api_calls
        api_calls += 1
        runs = [] if api_calls == 1 else [
            {
                "dag_run_id": "manual__owned",
                "state": "success",
                "start_date": "2026-08-12T20:30:00Z",
                "end_date": "2026-08-12T20:30:30Z",
            }
        ]
        return {"dag_runs": runs, "total_entries": len(runs)}

    def runner(*command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, stdout="bounded output", stderr="")

    terminal_calls = []

    def terminal(found):
        terminal_calls.append(found)
        return {"driverState": "FINISHED", "success": True}

    run, drivers = live._execute_paused_test_run(
        api=api,
        runner=runner,
        drivers=lambda: next(driver_sets),
        terminal=terminal,
        window_start="2026-08-12T20:29:59Z",
        owned=set(),
        logical_date=datetime(2026, 8, 12, 20, 30, tzinfo=timezone.utc),
    )
    assert run["dag_run_id"] == "manual__owned"
    assert drivers == ("flatten-driver", "session-driver")
    assert terminal_calls == ["flatten-driver", "session-driver"]
    command, kwargs = calls[0]
    assert command[:5] == ("docker", "exec", "data-eng-lab-airflow-scheduler", "bash", "-o")
    assert any("airflow dags test" in argument for argument in command)
    assert "--use-executor" in command and "2026-08-12T20:30:00+00:00" in command
    assert kwargs["timeout"] == 900


def test_paused_dags_test_rejects_zero_or_multiple_new_api_runs():
    for runs in (
        [],
        [
            {"dag_run_id": "one", "state": "success", "start_date": "2026-08-12T20:30:00Z"},
            {"dag_run_id": "two", "state": "success", "start_date": "2026-08-12T20:30:01Z"},
        ],
    ):
        responses = iter([
            {"dag_runs": [], "total_entries": 0},
            {"dag_runs": runs, "total_entries": len(runs)},
        ])
        with pytest.raises(AssertionError, match="exactly one new API-visible run"):
            live._execute_paused_test_run(
                api=lambda *_args, **_kwargs: next(responses),
                runner=lambda *command, **_kwargs: subprocess.CompletedProcess(command, 0, stdout="", stderr=""),
                drivers=lambda: set(),
                terminal=lambda _driver: {},
                window_start="2026-08-12T20:29:59Z",
                owned=set(),
                logical_date=datetime(2026, 8, 12, 20, 30, tzinfo=timezone.utc),
            )


def test_paused_dags_test_redacts_bounded_failure_diagnostics():
    error = subprocess.CalledProcessError(1, ("docker",), output="secret-value\n" + "x" * 20000)

    with pytest.raises(AssertionError) as caught:
        live._execute_dag_test(
            datetime(2026, 8, 12, 20, 30, tzinfo=timezone.utc),
            runner=lambda *_args, **_kwargs: (_ for _ in ()).throw(error),
            secrets=("secret-value",),
        )
    message = str(caught.value)
    assert "secret-value" not in message and "<redacted>" in message
    assert len(message) <= 5000
