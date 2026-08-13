from __future__ import annotations

import ast
import importlib.util
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
DAG = ROOT / "spark-apps/nyc-taxi-data-quality/dag.py"


@pytest.fixture
def dag_module(monkeypatch):
    edges = []

    class FakeTask:
        instances = []

        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
            self.task_id = kwargs.get("task_id")
            self.instances.append(self)

        def __rshift__(self, other):
            edges.append((self.task_id, other.task_id))
            return other

    class FakeOperator(FakeTask):
        instances = []

        def _get_hook(self):
            return object()

    class FakeSensor(FakeTask):
        instances = []

        pass

    class FakeDag:
        calls = []

        def __init__(self, *args, **kwargs):
            self.calls.append((args, kwargs))

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    modules = {
        "airflow": types.SimpleNamespace(DAG=FakeDag),
        "airflow.providers": types.ModuleType("airflow.providers"),
        "airflow.providers.apache": types.ModuleType("airflow.providers.apache"),
        "airflow.providers.apache.spark": types.ModuleType("airflow.providers.apache.spark"),
        "airflow.providers.apache.spark.operators": types.ModuleType(
            "airflow.providers.apache.spark.operators"
        ),
        "airflow.providers.apache.spark.operators.spark_submit": types.SimpleNamespace(
            SparkSubmitOperator=FakeOperator
        ),
        "airflow.providers.standard": types.ModuleType("airflow.providers.standard"),
        "airflow.providers.standard.sensors": types.ModuleType("airflow.providers.standard.sensors"),
        "airflow.providers.standard.sensors.external_task": types.SimpleNamespace(
            ExternalTaskSensor=FakeSensor
        ),
        "atlas_spark_utils": types.SimpleNamespace(
            RestConfirmingSparkHook=lambda hook, rest_host: (hook, rest_host)
        ),
        "pendulum": types.SimpleNamespace(datetime=lambda *args, **kwargs: (args, kwargs)),
    }
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    spec = importlib.util.spec_from_file_location("nyc_taxi_data_quality_test_dag", DAG)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, FakeDag, FakeSensor, FakeOperator, edges


def test_dag_is_daily_serialized_operator_owned_and_has_no_resolver_or_import_network():
    text = DAG.read_text(encoding="utf-8")
    tree = ast.parse(text)
    assert "class AtlasSparkSubmitOperator(SparkSubmitOperator)" in text
    assert "RestConfirmingSparkHook" in text and "super()._get_hook()" in text
    assert "s3a://jars/nyc-taxi-data-quality/0.1.0/app.jar" in text
    assert "com.thekaveh.dataeng.quality.NycTaxiDataQuality" in text
    assert 'conn_id="spark_default"' in text and 'deploy_mode="cluster"' in text
    assert 'schedule="@daily"' in text and "catchup=False" in text and "max_active_runs=1" in text
    assert "spark.standalone.submit.waitAppCompletion" in text
    assert "dataset-resolver" not in text and "urllib" not in text
    assert not any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in {"urlopen", "requests"}
        for node in tree.body
    )


def test_sensor_requires_exact_same_logical_date_successful_upstream_task(dag_module):
    _, _, sensor_class, _, edges = dag_module
    assert len(sensor_class.instances) == 1
    sensor = sensor_class.instances[0]
    assert sensor.kwargs == {
        "task_id": "wait_for_nyc_taxi_etl",
        "external_dag_id": "nyc_taxi_etl",
        "external_task_id": "submit_nyc_taxi_etl",
        "allowed_states": ["success"],
        "failed_states": ["failed", "upstream_failed", "skipped"],
        "check_existence": True,
        "mode": "reschedule",
        "poke_interval": 60,
        "timeout": 3600,
    }
    assert "execution_delta" not in sensor.kwargs and "execution_date_fn" not in sensor.kwargs
    assert edges == [("wait_for_nyc_taxi_etl", "submit_nyc_taxi_data_quality")]


def test_spark_task_uses_exact_fixed_arguments_and_runtime_contract(dag_module):
    module, _, _, operator_class, _ = dag_module
    assert len(operator_class.instances) == 1
    task = operator_class.instances[0]
    assert task.kwargs["task_id"] == "submit_nyc_taxi_data_quality"
    assert task.kwargs["conn_id"] == "spark_default"
    assert task.kwargs["application"] == "s3a://jars/nyc-taxi-data-quality/0.1.0/app.jar"
    assert task.kwargs["java_class"] == "com.thekaveh.dataeng.quality.NycTaxiDataQuality"
    assert task.kwargs["deploy_mode"] == "cluster"
    assert task.rest_host == "spark-master"
    assert task.kwargs["conf"] is module.spark_conf
    assert task.kwargs["application_args"] == [
        "--logical-date",
        "{{ logical_date.strftime('%Y-%m-%dT%H:%M:%SZ') }}",
        "--data-interval-end",
        "{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%SZ') }}",
        "--upstream-dag-id",
        "nyc_taxi_etl",
    ]
    assert module.spark_conf["spark.sql.session.timeZone"] == "UTC"
    assert module.spark_conf["spark.eventLog.enabled"] == "true"
    assert module.spark_conf["spark.eventLog.dir"] == "s3a://spark-history/"


def test_dag_schedule_retry_and_serialization_contract(dag_module):
    _, dag_class, _, _, _ = dag_module
    assert len(dag_class.calls) == 1
    kwargs = dag_class.calls[0][1]
    assert kwargs["schedule"] == "@daily"
    assert kwargs["catchup"] is False
    assert kwargs["max_active_runs"] == 1
    assert kwargs["default_args"]["retries"] == 1
    assert kwargs["default_args"]["retry_delay"].total_seconds() == 120
