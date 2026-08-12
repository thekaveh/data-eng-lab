from __future__ import annotations

import importlib.util
import subprocess
from datetime import datetime, timezone
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


def test_owned_run_guard_uses_pre_acceptance_id_baseline_not_skewed_logical_date():
    historical_probe = {
        "dag_run_id": "historical-future-logical-date",
        "state": "success",
        "start_date": None,
        "logical_date": "2026-08-12T20:30:00Z",
    }
    found = live._assert_owned_runs(
        lambda *_args, **_kwargs: {"dag_runs": [historical_probe]},
        "2026-08-12T20:28:00Z",
        set(),
        baseline={"historical-future-logical-date"},
    )
    assert found == {}


def test_resolver_failure_propagates_without_any_dataset_mutation_or_retry():
    calls = []

    def runner(*command, **_kwargs):
        calls.append(command)
        raise subprocess.CalledProcessError(1, command, output="generic resolver failure")

    with pytest.raises(subprocess.CalledProcessError, match="resolve_dataset.py"):
        live._resolve_or_publish_tiny(runner=runner)
    assert calls == [
        ("uv", "run", "python", "scripts/resolve_dataset.py", "tpch", "--scale", "tiny")
    ]


def test_resolver_does_not_refresh_an_existing_verified_publication():
    calls = []
    resolved = '{"dataset":"tpch","scale":"tiny","objects":[]}'

    def runner(*command, **_kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout=resolved, stderr="")

    live._resolve_or_publish_tiny(runner=runner)
    assert calls == [
        ("uv", "run", "python", "scripts/resolve_dataset.py", "tpch", "--scale", "tiny"),
        (
            "uv", "run", "python", "scripts/download_datasets.py",
            "--scale", "tiny", "--only", "tpch", "--verify-only",
        ),
        ("uv", "run", "python", "scripts/resolve_dataset.py", "tpch", "--scale", "tiny"),
    ]


def test_paused_dags_test_discovers_exactly_one_terminal_api_run_and_driver():
    calls = []
    driver_sets = iter([{"old-driver"}, {"old-driver", "new-driver"}])
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
        return {"dag_runs": runs}

    def runner(*command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, stdout="bounded output", stderr="")

    run, driver = live._execute_paused_test_run(
        api=api,
        runner=runner,
        drivers=lambda: next(driver_sets),
        terminal=lambda found: {"driverState": "FINISHED", "success": found == "new-driver"},
        window_start="2026-08-12T20:29:59Z",
        owned=set(),
        logical_date=datetime(2026, 8, 12, 20, 30, tzinfo=timezone.utc),
    )
    assert run["dag_run_id"] == "manual__owned" and driver == "new-driver"
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
        responses = iter([{"dag_runs": []}, {"dag_runs": runs}])
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
