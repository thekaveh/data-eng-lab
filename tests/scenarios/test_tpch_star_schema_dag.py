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
DAG = ROOT / "spark-apps/tpch-star-schema/dag.py"
PLAN = "1" * 64
MANIFEST = "2" * 64
PUBLICATION = "0123456789ab4def8123456789abcdef"


class _Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


def _document():
    names = (
        "customer.parquet",
        "lineitem.parquet",
        "nation.parquet",
        "orders.parquet",
        "part.parquet",
        "partsupp.parquet",
        "region.parquet",
        "supplier.parquet",
    )
    prefix = f"s3://landing/tpch/_generations/{PLAN}/{PUBLICATION}/"
    return {
        "dataset": "tpch",
        "scale": "tiny",
        "plan_id": PLAN,
        "manifest_sha256": MANIFEST,
        "publication_id": PUBLICATION,
        "objects": [
            {
                "object_name": name,
                "uri": prefix + name,
                "size_bytes": index + 1,
                "sha256": f"{index + 3:064x}",
                "schema_id": name.removesuffix(".parquet"),
            }
            for index, name in enumerate(names)
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
        "atlas_spark_utils": types.SimpleNamespace(RestConfirmingSparkHook=lambda hook, rest_host: (hook, rest_host)),
        "pendulum": types.SimpleNamespace(datetime=lambda *args, **kwargs: (args, kwargs)),
    }
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    spec = importlib.util.spec_from_file_location("tpch_star_schema_test_dag", DAG)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, FakeDag


def test_tpch_dag_is_runtime_resolved_operator_owned_and_daily():
    text = DAG.read_text(encoding="utf-8")
    module = ast.parse(text)
    assert "class AtlasSparkSubmitOperator(SparkSubmitOperator)" in text
    assert "RestConfirmingSparkHook" in text and "super()._get_hook()" in text
    assert "_resolve_dataset(self.dataset, scale)" in text
    assert "self.application_args" in text and "super().execute(context)" in text
    assert "s3a://jars/tpch-star-schema/0.1.0/app.jar" in text
    assert "com.thekaveh.dataeng.tpch.TpchStarSchema" in text
    assert 'conn_id="spark_default"' in text and 'deploy_mode="cluster"' in text
    assert 'schedule="@daily"' in text and "catchup=False" in text
    assert "max_active_runs=1" in text
    assert "spark.standalone.submit.waitAppCompletion" in text
    module_calls = [node for node in module.body if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call)]
    assert not any(
        isinstance(call.value.func, ast.Name) and call.value.func.id == "_resolve_dataset" for call in module_calls
    )


def test_tpch_dag_passes_complete_provenance_after_exact_eight_uris():
    text = DAG.read_text(encoding="utf-8")
    for name in (
        "customer.parquet",
        "lineitem.parquet",
        "nation.parquet",
        "orders.parquet",
        "part.parquet",
        "partsupp.parquet",
        "region.parquet",
        "supplier.parquet",
    ):
        assert name in text
    for flag in ("--dataset-scale", "--plan-id", "--publication-id", "--manifest-sha256"):
        assert flag in text
    assert 'f"s3://landing/{dataset}/_generations/{plan}/{publication}/"' in text


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


def test_resolver_posts_bounded_exact_request_and_preserves_registry_order(dag_module, monkeypatch):
    module, _ = dag_module
    calls = []

    def opener(request, timeout):
        calls.append((request, timeout))
        return _Response(json.dumps(_document()).encode())

    monkeypatch.setattr(module.urllib.request, "urlopen", opener)
    result = module._resolve_dataset("tpch", "tiny")
    assert result.uris == tuple(item["uri"] for item in _document()["objects"])
    request, timeout = calls.pop()
    assert request.full_url.endswith("/v1/resolve") and request.method == "POST" and timeout == 120
    assert json.loads(request.data) == {"dataset": "tpch", "expected_scale": "tiny"}


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
        lambda d: {**d, "objects": [{**d["objects"][0], "schema_id": "orders"}, *d["objects"][1:]]},
        lambda d: {
            **d,
            "objects": [
                {**d["objects"][0], "uri": d["objects"][0]["uri"].replace("customer.parquet", "x/customer.parquet")},
                *d["objects"][1:],
            ],
        },
    ],
)
def test_resolver_rejects_malformed_type_zero_size_digest_schema_order_and_path(dag_module, monkeypatch, mutate):
    module, _ = dag_module
    monkeypatch.setattr(
        module.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _Response(json.dumps(mutate(_document())).encode()),
    )
    with pytest.raises(ValueError, match="dataset resolution failed"):
        module._resolve_dataset("tpch", "tiny")


@pytest.mark.parametrize(
    "body",
    [
        b'{"dataset":"tpch","dataset":"tpch"}',
        json.dumps({"x": [[[[[[[[[[[[[[[[[[]]]]]]]]]]]]]]]]]]}).encode(),
        b"x" * ((1 << 20) + 1),
        b"[]",
    ],
)
def test_resolver_rejects_duplicate_deep_oversize_and_non_object_documents(dag_module, monkeypatch, body):
    module, _ = dag_module
    monkeypatch.setattr(module.urllib.request, "urlopen", lambda *_args, **_kwargs: _Response(body))
    with pytest.raises(ValueError, match="dataset resolution failed"):
        module._resolve_dataset("tpch", "tiny")


def test_operator_resolves_only_during_execute_and_forwards_exact_args(dag_module, monkeypatch):
    module, _ = dag_module
    document = _document()
    resolution = module.Resolution(
        tuple(item["uri"] for item in document["objects"]), "tiny", PLAN, PUBLICATION, MANIFEST
    )
    calls = []
    monkeypatch.setattr(module, "_resolve_dataset", lambda dataset, scale: calls.append((dataset, scale)) or resolution)
    operator = module.AtlasSparkSubmitOperator(dataset="tpch", task_id="submit")
    assert calls == []
    assert operator.execute({"dag_run": types.SimpleNamespace(conf={"dataset_scale": "tiny"})}) == "submitted"
    assert calls == [("tpch", "tiny")]
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


def test_dag_serializes_non_atomic_two_table_replacements(dag_module):
    _, fake_dag = dag_module
    assert len(fake_dag.calls) == 1
    assert fake_dag.calls[0][1]["max_active_runs"] == 1
