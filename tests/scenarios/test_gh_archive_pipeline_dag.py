from __future__ import annotations

import importlib.util
import io
import json
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
DAG = ROOT / "spark-apps/gh-archive-pipeline/dag.py"
PLAN = "1" * 64
MANIFEST = "2" * 64
PUBLICATION = "0123456789ab4def8123456789abcdef"
NAMES = tuple(f"2023-01-01-{hour}.json.gz" for hour in range(6))
COUNTS = {"tiny": 1, "small": 3, "medium": 6}


class _Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


def _document(scale="tiny"):
    prefix = f"s3://landing/gh_archive/_generations/{PLAN}/{PUBLICATION}/"
    return {
        "dataset": "gh_archive", "scale": scale, "plan_id": PLAN,
        "manifest_sha256": MANIFEST, "publication_id": PUBLICATION,
        "objects": [
            {"object_name": name, "uri": prefix + name, "size_bytes": index + 1,
             "sha256": f"{index + 3:064x}", "schema_id": "gh_archive_consumed_fields"}
            for index, name in enumerate(NAMES[: COUNTS[scale]])
        ],
    }


@pytest.fixture
def dag_module(monkeypatch):
    operators = []

    class FakeOperator:
        def __init__(self, *args, **kwargs):
            self.task_id = kwargs.get("task_id")
            self.application_args = kwargs.pop("application_args", [])
            self.kwargs = kwargs
            self.downstream = []
            operators.append(self)

        def __rshift__(self, other):
            self.downstream.append(other.task_id)
            return other

        def _get_hook(self):
            return object()

        def execute(self, context):
            self.context = context
            return "submitted"

    class FakePythonOperator(FakeOperator):
        pass

    class FakeDag:
        calls = []
        def __init__(self, *args, **kwargs): self.calls.append((args, kwargs))
        def __enter__(self): return self
        def __exit__(self, *_args): return False

    modules = {
        "airflow": types.SimpleNamespace(DAG=FakeDag),
        "airflow.providers": types.ModuleType("airflow.providers"),
        "airflow.providers.apache": types.ModuleType("airflow.providers.apache"),
        "airflow.providers.apache.spark": types.ModuleType("airflow.providers.apache.spark"),
        "airflow.providers.apache.spark.operators": types.ModuleType("airflow.providers.apache.spark.operators"),
        "airflow.providers.apache.spark.operators.spark_submit": types.SimpleNamespace(
            SparkSubmitOperator=FakeOperator
        ),
        "airflow.operators": types.ModuleType("airflow.operators"),
        "airflow.operators.python": types.SimpleNamespace(PythonOperator=FakePythonOperator),
        "atlas_spark_utils": types.SimpleNamespace(RestConfirmingSparkHook=lambda hook, rest_host: (hook, rest_host)),
        "pendulum": types.SimpleNamespace(datetime=lambda *args, **kwargs: (args, kwargs)),
    }
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    spec = importlib.util.spec_from_file_location("gh_archive_pipeline_test_dag", DAG)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, FakeDag, operators


def test_dag_has_one_resolver_two_ordered_rest_confirming_stages():
    text = DAG.read_text(encoding="utf-8")
    assert 'dag_id="gh_archive_flatten_sessionization"' in text
    assert 'schedule="@daily"' in text and "max_active_runs=1" in text
    assert 'application="s3a://jars/gh-archive-pipeline/0.1.0/app.jar"' in text
    assert "com.thekaveh.dataeng.gharchive.GhArchiveFlatten" in text
    assert "com.thekaveh.dataeng.gharchive.GhArchiveSessionization" in text
    assert "resolve >> flatten >> sessionize" in text
    assert "RestConfirmingSparkHook" in text and "spark.standalone.submit.waitAppCompletion" in text
    assert "readStream" not in text and "redpanda" not in text.lower()


def test_scale_precedence(dag_module, monkeypatch):
    module, _, _ = dag_module
    monkeypatch.setenv("DATASET_SCALE", "medium")
    assert module._effective_scale({"dag_run": types.SimpleNamespace(conf={"dataset_scale": "tiny"})}) == "tiny"
    assert module._effective_scale({}) == "medium"
    for invalid in ("", "large", 1, True):
        with pytest.raises(ValueError, match="tiny, small, medium"):
            module._effective_scale({"dag_run": types.SimpleNamespace(conf={"dataset_scale": invalid})})


@pytest.mark.parametrize("scale", ["tiny", "small", "medium"])
def test_resolver_returns_bounded_canonical_payload_in_registry_order(dag_module, monkeypatch, scale):
    module, _, _ = dag_module
    calls = []
    monkeypatch.setattr(
        module.urllib.request,
        "urlopen",
        lambda request, timeout: calls.append((request, timeout))
        or _Response(json.dumps(_document(scale)).encode()),
    )
    payload = module._resolve_dataset(scale)
    assert payload == json.dumps(_document(scale), sort_keys=True, separators=(",", ":"))
    assert module._parse_resolution(payload, scale).uris == tuple(item["uri"] for item in _document(scale)["objects"])
    request, timeout = calls[0]
    assert timeout == 120 and json.loads(request.data) == {"dataset": "gh_archive", "expected_scale": scale}


@pytest.mark.parametrize("mutate", [
    lambda d: {**d, "extra": True},
    lambda d: {**d, "objects": d["objects"][:-1]},
    lambda d: {**d, "objects": list(reversed(d["objects"]))},
    lambda d: {**d, "objects": [{**d["objects"][0], "size_bytes": 0}, *d["objects"][1:]]},
    lambda d: {**d, "objects": [{**d["objects"][0], "sha256": "bad"}, *d["objects"][1:]]},
    lambda d: {**d, "objects": [{**d["objects"][0], "schema_id": "wrong"}, *d["objects"][1:]]},
])
def test_parser_fails_closed_on_inventory_and_type_drift(dag_module, mutate):
    module, _, _ = dag_module
    payload = json.dumps(mutate(_document("small")), sort_keys=True, separators=(",", ":"))
    with pytest.raises(ValueError, match="dataset resolution failed"):
        module._parse_resolution(payload, "small")


def test_both_operators_pull_the_same_xcom_without_network(dag_module, monkeypatch):
    module, _, _ = dag_module
    payload = json.dumps(_document(), sort_keys=True, separators=(",", ":"))
    pulls = []
    ti = types.SimpleNamespace(xcom_pull=lambda task_ids: pulls.append(task_ids) or payload)
    monkeypatch.setattr(module.urllib.request, "urlopen", lambda *_a, **_k: pytest.fail("operator must not resolve"))
    for java_class in (module.FLATTEN_CLASS, module.SESSION_CLASS):
        operator = module.AtlasResolvedSparkSubmitOperator(task_id="submit", java_class=java_class)
        context = {"ti": ti, "dag_run": types.SimpleNamespace(conf={"dataset_scale": "tiny"})}
        assert operator.execute(context) == "submitted"
        assert operator.application_args[-8:] == ["--dataset-scale", "tiny", "--plan-id", PLAN,
                                                 "--publication-id", PUBLICATION, "--manifest-sha256", MANIFEST]
    assert pulls == ["resolve_gh_archive", "resolve_gh_archive"]


def test_dag_serializes_exact_task_graph(dag_module):
    _, fake_dag, operators = dag_module
    assert fake_dag.calls[0][1]["max_active_runs"] == 1
    by_id = {item.task_id: item for item in operators}
    assert by_id["resolve_gh_archive"].downstream == ["submit_gh_archive_flatten"]
    assert by_id["submit_gh_archive_flatten"].downstream == ["submit_gh_archive_sessionization"]


def test_cluster_submission_has_complete_s3a_and_iceberg_credentials(dag_module):
    module, _, _ = dag_module
    required = {
        "spark.app.name",
        "spark.executor.memory",
        "spark.driver.memory",
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "spark.driverEnv.AWS_ACCESS_KEY_ID",
        "spark.driverEnv.AWS_SECRET_ACCESS_KEY",
        "spark.driverEnv.AWS_REGION",
        "spark.driverEnv.AWS_ENDPOINT_URL_S3",
        "spark.executorEnv.AWS_ACCESS_KEY_ID",
        "spark.executorEnv.AWS_SECRET_ACCESS_KEY",
        "spark.executorEnv.AWS_REGION",
        "spark.executorEnv.AWS_ENDPOINT_URL_S3",
        "spark.sql.catalog.lakehouse.s3.access-key-id",
        "spark.sql.catalog.lakehouse.s3.secret-access-key",
        "spark.sql.catalog.lakehouse.client.region",
    }
    assert required <= set(module.spark_conf)
