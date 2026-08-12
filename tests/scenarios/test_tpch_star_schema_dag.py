from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DAG = ROOT / "spark-apps/tpch-star-schema/dag.py"


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
