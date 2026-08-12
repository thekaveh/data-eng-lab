from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "tpch_star_schema_live_harness",
    ROOT / "tests/scenarios/test_tpch_star_schema_live.py",
)
live = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(live)


def test_owned_stack_rejects_preexisting_project_without_mutation():
    commands = []

    def runner(*command, **_kwargs):
        commands.append(command)

    with pytest.raises(RuntimeError, match="already running"):
        with live._owned_stack(runner=runner, probe=lambda: ("data-eng-lab-minio",)):
            raise AssertionError("must not enter")
    assert commands == []


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
            lambda *_args, **_kwargs: {"dag_runs": runs},
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
        return {"dag_runs": runs}

    found = live._assert_owned_runs(
        api,
        "2026-08-12T20:00:00Z",
        {"owned-first", "owned-second"},
        require_terminal=True,
    )
    assert set(found) == {"owned-first", "owned-second"}


def test_resolver_refreshes_only_missing_tiny_publication_and_verifies_afterward():
    calls = []
    resolved = '{"dataset":"tpch","scale":"tiny","objects":[]}'

    def runner(*command, **_kwargs):
        calls.append(command)
        if command[:4] == ("uv", "run", "python", "scripts/resolve_dataset.py") and len(calls) == 1:
            raise subprocess.CalledProcessError(1, command)
        return subprocess.CompletedProcess(command, 0, stdout=resolved, stderr="")

    assert live._resolve_or_publish_tiny(runner=runner)["dataset"] == "tpch"
    assert (
        "uv", "run", "python", "scripts/download_datasets.py",
        "--scale", "tiny", "--only", "tpch", "--refresh",
    ) in calls
    assert (
        "uv", "run", "python", "scripts/download_datasets.py",
        "--scale", "tiny", "--only", "tpch", "--verify-only",
    ) in calls


def test_resolver_does_not_refresh_an_existing_verified_publication():
    calls = []
    resolved = '{"dataset":"tpch","scale":"tiny","objects":[]}'

    def runner(*command, **_kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout=resolved, stderr="")

    live._resolve_or_publish_tiny(runner=runner)
    assert not any("--refresh" in command for command in calls)
    assert any("--verify-only" in command for command in calls)
