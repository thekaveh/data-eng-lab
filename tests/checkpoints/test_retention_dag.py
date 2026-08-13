from __future__ import annotations

import importlib.util
import io
import json
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
DAG_PATH = ROOT / "airflow-dags/checkpoint_retention/dag.py"
TASKS_PATH = ROOT / "airflow-dags/checkpoint_retention/tasks.py"
EXPECTED_IDS = (
    "streaming-events-v1",
    "streaming-event-windows-v1",
    "streaming-online-retail-cdc-v1",
    "streaming-gh-archive-file-v1",
    "go-live-streaming-test-v1",
)


class _Response(io.BytesIO):
    def __init__(self, body: bytes, close_error=None):
        super().__init__(body)
        self.close_error = close_error
        self.close_count = 0

    def close(self):
        self.close_count += 1
        super().close()
        if self.close_error is not None:
            raise self.close_error


def _load_tasks(name="checkpoint_retention_test_tasks"):
    spec = importlib.util.spec_from_file_location(name, TASKS_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def dag_module(monkeypatch):
    class FakeDag:
        calls = []

        def __init__(self, *args, **kwargs):
            self.calls.append((args, kwargs))

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class FakeOperator:
        calls = []

        def __init__(self, **kwargs):
            self.calls.append(kwargs)

    tasks = _load_tasks("data_eng_lab_airflow_dags.checkpoint_retention.tasks")
    packages = {
        "airflow": types.SimpleNamespace(DAG=FakeDag),
        "airflow.operators": types.ModuleType("airflow.operators"),
        "airflow.operators.python": types.SimpleNamespace(PythonOperator=FakeOperator),
        "data_eng_lab_airflow_dags": types.ModuleType("data_eng_lab_airflow_dags"),
        "data_eng_lab_airflow_dags.checkpoint_retention": types.ModuleType(
            "data_eng_lab_airflow_dags.checkpoint_retention"
        ),
        "data_eng_lab_airflow_dags.checkpoint_retention.tasks": tasks,
        "pendulum": types.SimpleNamespace(datetime=lambda *args, **kwargs: (args, kwargs)),
    }
    for name, module in packages.items():
        monkeypatch.setitem(sys.modules, name, module)
    spec = importlib.util.spec_from_file_location("checkpoint_retention_test_dag", DAG_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module, FakeDag, FakeOperator


def test_dag_is_manual_paused_serialized_and_has_exactly_one_plan_task(dag_module):
    module, fake_dag, fake_operator = dag_module
    assert module.checkpoint_retention is not None
    assert len(fake_dag.calls) == 1
    args, kwargs = fake_dag.calls[0]
    assert args == ()
    assert kwargs["dag_id"] == "checkpoint_retention"
    assert kwargs["schedule"] is None
    assert kwargs["catchup"] is False
    assert kwargs["max_active_runs"] == 1
    assert kwargs["is_paused_upon_creation"] is True
    assert kwargs["default_args"]["retries"] == 1
    assert len(fake_operator.calls) == 1
    assert fake_operator.calls[0]["task_id"] == "plan_checkpoint_retention"
    assert fake_operator.calls[0]["python_callable"].__name__ == "run_retention_plans"
    assert fake_operator.calls[0]["execution_timeout"].total_seconds() == 300


def test_dag_and_task_source_have_no_apply_delete_conf_or_network_import_side_effect():
    dag = DAG_PATH.read_text(encoding="utf-8")
    tasks = TASKS_PATH.read_text(encoding="utf-8")
    lowered = (dag + tasks).lower()
    assert "apply" not in lowered
    assert "delete" not in lowered
    assert "dag_run" not in lowered
    assert ".conf" not in lowered
    assert "urllib.request.urlopen(" not in tasks
    assert "_open(request" in tasks


def test_task_posts_one_fixed_complete_registry_inventory_and_returns_safe_ordered_summaries(monkeypatch):
    tasks = _load_tasks()
    response = _Response(
        json.dumps(
            {
                "plans": [
                    {
                        "checkpoint_id": checkpoint_id,
                        "decision": "refused",
                        "inventory": {"object_count": 0, "total_bytes": 0},
                        "policy_sha256": "a" * 64,
                        "refusal_codes": ["lease_missing"],
                    }
                    for checkpoint_id in EXPECTED_IDS
                ],
                "state": "accepted",
            },
            separators=(",", ":"),
        ).encode()
    )
    captured = {}

    def opener(request, timeout):
        captured.update(url=request.full_url, headers=dict(request.header_items()), body=request.data, timeout=timeout)
        return response

    monkeypatch.setenv("CHECKPOINT_RETENTION_API_TOKEN", "task-secret-token")
    monkeypatch.setenv("CHECKPOINT_RETENTION_URI", "http://checkpoint-retention:8080")
    monkeypatch.setattr(tasks, "_open", opener)

    result = tasks.run_retention_plans()

    assert captured["url"] == "http://checkpoint-retention:8080/v1/plans"
    assert captured["timeout"] == 30
    assert captured["headers"]["Authorization"] == "Bearer task-secret-token"
    assert json.loads(captured["body"]) == {"actor": "airflow-dry-run", "checkpoint_ids": list(EXPECTED_IDS)}
    assert [item["checkpoint_id"] for item in result["plans"]] == list(EXPECTED_IDS)
    assert result["state"] == "accepted"
    assert "task-secret-token" not in json.dumps(result)
    assert response.close_count == 1


@pytest.mark.parametrize(
    "body",
    [
        b'{"plans":[],"state":"accepted"}',
        b'{"plans":[{"checkpoint_id":"unknown"}],"state":"accepted"}',
        b'{"plans":[],"state":"accepted","state":"refused"}',
        b"x" * 65_537,
    ],
)
def test_task_rejects_incomplete_malformed_duplicate_and_oversized_responses(monkeypatch, body):
    tasks = _load_tasks()
    monkeypatch.setenv("CHECKPOINT_RETENTION_API_TOKEN", "task-token")
    monkeypatch.setenv("CHECKPOINT_RETENTION_URI", "http://checkpoint-retention:8080")
    monkeypatch.setattr(tasks, "_open", lambda *_args, **_kwargs: _Response(body))

    with pytest.raises(tasks.RetentionTaskFailure):
        tasks.run_retention_plans()


def test_task_failure_and_cleanup_are_sanitized_and_control_flow_is_preserved(monkeypatch):
    tasks = _load_tasks()
    monkeypatch.setenv("CHECKPOINT_RETENTION_API_TOKEN", "task-token")
    monkeypatch.setenv("CHECKPOINT_RETENTION_URI", "http://checkpoint-retention:8080")
    monkeypatch.setattr(tasks, "_open", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("secret")))
    with pytest.raises(tasks.RetentionTaskFailure, match="service_failure") as failure:
        tasks.run_retention_plans()
    assert failure.value.__cause__ is None

    interrupt = KeyboardInterrupt()
    monkeypatch.setattr(tasks, "_open", lambda *_args, **_kwargs: (_ for _ in ()).throw(interrupt))
    with pytest.raises(KeyboardInterrupt) as caught:
        tasks.run_retention_plans()
    assert caught.value is interrupt
