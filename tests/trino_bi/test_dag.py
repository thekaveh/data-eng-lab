from __future__ import annotations

import importlib.util
import socket
import sys
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[2]
AIRFLOW_DAGS = ROOT / "airflow-dags"
DAG_PATH = AIRFLOW_DAGS / "trino_bi" / "dag.py"


class FakeDAG:
    current = None

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.tasks = []

    def __enter__(self):
        FakeDAG.current = self
        return self

    def __exit__(self, *_args):
        FakeDAG.current = None


class FakePythonOperator:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        assert FakeDAG.current is not None
        FakeDAG.current.tasks.append(self)


def _fake_modules():
    airflow = ModuleType("airflow")
    airflow.DAG = FakeDAG
    operators = ModuleType("airflow.operators")
    python = ModuleType("airflow.operators.python")
    python.PythonOperator = FakePythonOperator
    pendulum = ModuleType("pendulum")
    pendulum.datetime = lambda *args, **kwargs: (args, kwargs)
    return {
        "airflow": airflow,
        "airflow.operators": operators,
        "airflow.operators.python": python,
        "pendulum": pendulum,
    }


def _load(monkeypatch):
    assert DAG_PATH.is_file(), "shared Trino BI DAG entrypoint has not been implemented"
    for name, module in _fake_modules().items():
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.syspath_prepend(str(AIRFLOW_DAGS))
    tasks = __import__("trino_bi.tasks", fromlist=["run_tpch_bi"])
    namespace = ModuleType("data_eng_lab_airflow_dags")
    package = ModuleType("data_eng_lab_airflow_dags.trino_bi")
    package.__path__ = [str(AIRFLOW_DAGS / "trino_bi")]
    monkeypatch.setitem(sys.modules, "data_eng_lab_airflow_dags", namespace)
    monkeypatch.setitem(sys.modules, "data_eng_lab_airflow_dags.trino_bi", package)
    monkeypatch.setitem(sys.modules, "data_eng_lab_airflow_dags.trino_bi.tasks", tasks)
    monkeypatch.setattr(
        socket,
        "create_connection",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("import-time network")),
    )
    spec = importlib.util.spec_from_file_location("trino_bi_test_dag", DAG_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_two_serialized_scheduled_dags_have_exact_operator_contract(monkeypatch) -> None:
    module = _load(monkeypatch)
    expected = {
        "tpch_bi_query": ("0 1 * * *", "run_tpch_bi"),
        "nyc_taxi_trino_daily": ("0 2 * * *", "run_nyc_bi"),
    }
    assert set(module.DAGS) == set(expected)
    for dag_id, (schedule, callable_name) in expected.items():
        dag = module.DAGS[dag_id]
        assert dag.kwargs == {
            "dag_id": dag_id,
            "description": dag.kwargs["description"],
            "default_args": {
                "owner": "data-eng-lab",
                "depends_on_past": False,
                "retries": 1,
                "retry_delay": module.timedelta(minutes=2),
            },
            "schedule": schedule,
            "start_date": ((2023, 1, 1), {"tz": "UTC"}),
            "catchup": False,
            "max_active_runs": 1,
            "tags": ["data-eng-lab", "scenario", "trino", "read-only"],
        }
        assert len(dag.tasks) == 1
        task = dag.tasks[0].kwargs
        assert task["task_id"] == "run_bounded_bi_query"
        assert task["python_callable"].__name__ == callable_name
        assert set(task) == {"task_id", "python_callable"}


def test_dag_exposes_both_objects_without_params_or_dynamic_sql(monkeypatch) -> None:
    module = _load(monkeypatch)
    assert module.tpch_bi_query is module.DAGS["tpch_bi_query"]
    assert module.nyc_taxi_trino_daily is module.DAGS["nyc_taxi_trino_daily"]
    source = DAG_PATH.read_text(encoding="utf-8")
    assert "params=" not in source
    assert "dag_run" not in source
    assert "conf" not in source
    assert "HttpHook" not in source
    assert "requests" not in source
    assert "trino.dbapi" not in source
    assert "airflow.providers.trino" not in source


def test_import_has_no_connection_or_transport_side_effect(monkeypatch) -> None:
    module = _load(monkeypatch)
    assert module.run_tpch_bi.__module__ == "trino_bi.tasks"
    assert module.run_nyc_bi.__module__ == "trino_bi.tasks"
