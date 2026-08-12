from __future__ import annotations

import ast
import importlib.util
import io
import json
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
DAG = ROOT / "spark-apps/movielens-feature-pipeline/dag.py"
PLAN = "1" * 64
MANIFEST = "2" * 64
PUBLICATION = "0123456789ab4def8123456789abcdef"
OBJECTS = {
    "tiny": (
        ("links.csv", "movielens_latest_small_links"),
        ("tags.csv", "movielens_latest_small_tags"),
        ("ratings.csv", "movielens_latest_small_ratings"),
        ("README.txt", "movielens_latest_small_readme"),
        ("movies.csv", "movielens_latest_small_movies"),
    ),
    "small": (
        ("links.csv", "movielens_latest_small_links"),
        ("tags.csv", "movielens_latest_small_tags"),
        ("ratings.csv", "movielens_latest_small_ratings"),
        ("README.txt", "movielens_latest_small_readme"),
        ("movies.csv", "movielens_latest_small_movies"),
    ),
    "medium": (
        ("tags.csv", "movielens_25m_tags"),
        ("links.csv", "movielens_25m_links"),
        ("README.txt", "movielens_25m_readme"),
        ("ratings.csv", "movielens_25m_ratings"),
        ("genome-tags.csv", "movielens_25m_genome_tags"),
        ("genome-scores.csv", "movielens_25m_genome_scores"),
        ("movies.csv", "movielens_25m_movies"),
    ),
}


class _Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


def _document(scale="tiny"):
    prefix = f"s3://landing/movielens/_generations/{PLAN}/{PUBLICATION}/"
    return {
        "dataset": "movielens",
        "scale": scale,
        "plan_id": PLAN,
        "manifest_sha256": MANIFEST,
        "publication_id": PUBLICATION,
        "objects": [
            {
                "object_name": name,
                "uri": prefix + name,
                "size_bytes": index + 1,
                "sha256": f"{index + 3:064x}",
                "schema_id": schema,
            }
            for index, (name, schema) in enumerate(OBJECTS[scale])
        ],
    }


@pytest.fixture
def dag_module(monkeypatch):
    class FakeOperator:
        def __init__(self, *args, **kwargs):
            self.application_args = kwargs.pop("application_args", [])
            self.kwargs = kwargs

        def _get_hook(self):
            return object()

        def execute(self, context):
            self.executed_context = context
            return "submitted"

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
        "airflow.providers.apache.spark.operators": types.ModuleType("airflow.providers.apache.spark.operators"),
        "airflow.providers.apache.spark.operators.spark_submit": types.SimpleNamespace(
            SparkSubmitOperator=FakeOperator
        ),
        "atlas_spark_utils": types.SimpleNamespace(
            RestConfirmingSparkHook=lambda hook, rest_host: (hook, rest_host)
        ),
        "pendulum": types.SimpleNamespace(datetime=lambda *args, **kwargs: (args, kwargs)),
    }
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    spec = importlib.util.spec_from_file_location("movielens_feature_pipeline_test_dag", DAG)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, FakeDag


def test_dag_is_operator_owned_serialized_and_daily_without_import_resolution():
    text = DAG.read_text(encoding="utf-8")
    module = ast.parse(text)
    assert "class AtlasSparkSubmitOperator(SparkSubmitOperator)" in text
    assert "RestConfirmingSparkHook" in text and "super()._get_hook()" in text
    assert "_resolve_dataset(self.dataset, scale)" in text
    assert "self.application_args" in text and "super().execute(context)" in text
    assert "s3a://jars/movielens-feature-pipeline/0.1.0/app.jar" in text
    assert "com.thekaveh.dataeng.movielens.MovieLensFeaturePipeline" in text
    assert 'conn_id="spark_default"' in text and 'deploy_mode="cluster"' in text
    assert 'schedule="@daily"' in text and "catchup=False" in text and "max_active_runs=1" in text
    assert "spark.standalone.submit.waitAppCompletion" in text
    module_calls = [node for node in module.body if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call)]
    assert not any(
        isinstance(call.value.func, ast.Name) and call.value.func.id == "_resolve_dataset" for call in module_calls
    )


def test_effective_scale_precedence_and_validation(dag_module, monkeypatch):
    module, _ = dag_module
    monkeypatch.setenv("DATASET_SCALE", "medium")
    assert module._effective_scale({"dag_run": types.SimpleNamespace(conf={"dataset_scale": "tiny"})}) == "tiny"
    assert module._effective_scale({}) == "medium"
    monkeypatch.delenv("DATASET_SCALE")
    assert module._effective_scale({}) == "small"
    for invalid in ("", "large", 1, True):
        with pytest.raises(ValueError, match="tiny, small, medium"):
            module._effective_scale({"dag_run": types.SimpleNamespace(conf={"dataset_scale": invalid})})


@pytest.mark.parametrize("scale", ["tiny", "small", "medium"])
def test_resolver_preserves_exact_scale_specific_registry_order_and_schema_ids(dag_module, monkeypatch, scale):
    module, _ = dag_module
    calls = []

    def opener(request, timeout):
        calls.append((request, timeout))
        return _Response(json.dumps(_document(scale)).encode())

    monkeypatch.setattr(module.urllib.request, "urlopen", opener)
    result = module._resolve_dataset("movielens", scale)
    assert result.uris == tuple(item["uri"] for item in _document(scale)["objects"])
    request, timeout = calls.pop()
    assert request.full_url.endswith("/v1/resolve") and request.method == "POST" and timeout == 120
    assert json.loads(request.data) == {"dataset": "movielens", "expected_scale": scale}


@pytest.mark.parametrize(
    "mutate",
    [
        lambda d: {**d, "extra": True},
        lambda d: {**d, "dataset": "other"},
        lambda d: {**d, "scale": "small"},
        lambda d: {**d, "objects": d["objects"][:-1]},
        lambda d: {**d, "objects": d["objects"] + [d["objects"][0]]},
        lambda d: {**d, "objects": list(reversed(d["objects"]))},
        lambda d: {**d, "objects": [{**d["objects"][0], "size_bytes": 0}, *d["objects"][1:]]},
        lambda d: {**d, "objects": [{**d["objects"][0], "size_bytes": "1"}, *d["objects"][1:]]},
        lambda d: {**d, "objects": [{**d["objects"][0], "sha256": "bad"}, *d["objects"][1:]]},
        lambda d: {**d, "objects": [{**d["objects"][0], "schema_id": "wrong"}, *d["objects"][1:]]},
        lambda d: {
            **d,
            "objects": [
                {**d["objects"][0], "uri": d["objects"][0]["uri"].replace("links.csv", "deep/links.csv")},
                *d["objects"][1:],
            ],
        },
    ],
)
def test_resolver_rejects_extra_crossscale_missing_duplicate_order_zero_size_type_digest_schema_and_path(
    dag_module, monkeypatch, mutate
):
    module, _ = dag_module
    monkeypatch.setattr(
        module.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _Response(json.dumps(mutate(_document())).encode()),
    )
    with pytest.raises(ValueError, match="dataset resolution failed"):
        module._resolve_dataset("movielens", "tiny")


@pytest.mark.parametrize(
    "body",
    [
        b'{"dataset":"movielens","dataset":"movielens"}',
        json.dumps({"x": [[[[[[[[[[[[[[[[[[]]]]]]]]]]]]]]]]]]}).encode(),
        b"x" * ((1 << 20) + 1),
        b"[]",
        b'{"dataset": NaN}',
    ],
)
def test_resolver_rejects_duplicate_deep_oversize_nonobject_and_nonfinite_json(dag_module, monkeypatch, body):
    module, _ = dag_module
    monkeypatch.setattr(module.urllib.request, "urlopen", lambda *_args, **_kwargs: _Response(body))
    with pytest.raises(ValueError, match="dataset resolution failed"):
        module._resolve_dataset("movielens", "tiny")


def test_operator_resolves_only_during_execute_and_forwards_exact_arguments(dag_module, monkeypatch):
    module, _ = dag_module
    document = _document()
    resolution = module.Resolution(
        tuple(item["uri"] for item in document["objects"]), "tiny", PLAN, PUBLICATION, MANIFEST
    )
    calls = []
    monkeypatch.setattr(module, "_resolve_dataset", lambda dataset, scale: calls.append((dataset, scale)) or resolution)
    operator = module.AtlasSparkSubmitOperator(dataset="movielens", task_id="submit")
    assert calls == []
    assert operator.execute({"dag_run": types.SimpleNamespace(conf={"dataset_scale": "tiny"})}) == "submitted"
    assert calls == [("movielens", "tiny")]
    assert operator.application_args == [
        *resolution.uris,
        "--dataset-scale",
        "tiny",
        "--plan-id",
        PLAN,
        "--publication-id",
        PUBLICATION,
        "--manifest-sha256",
        MANIFEST,
    ]


def test_dag_serializes_the_non_atomic_two_table_replacement(dag_module):
    _, fake_dag = dag_module
    assert len(fake_dag.calls) == 1
    assert fake_dag.calls[0][1]["max_active_runs"] == 1
