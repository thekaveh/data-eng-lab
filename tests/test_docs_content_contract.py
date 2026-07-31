"""Source-backed contracts for public Spark-application documentation."""

from __future__ import annotations

import ast
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from scripts.docs.manifest import iter_leaf_sections, load_manifest

ROOT = Path(__file__).resolve().parents[1]
POM_NAMESPACE = {"m": "http://maven.apache.org/POM/4.0.0"}


def _pom_contract(app: str) -> dict[str, str]:
    root = ET.parse(ROOT / "spark-apps" / app / "pom.xml").getroot()
    properties = root.find("m:properties", POM_NAMESPACE)
    assert properties is not None
    return {
        "artifact": root.findtext("m:artifactId", namespaces=POM_NAMESPACE) or "",
        "version": root.findtext("m:version", namespaces=POM_NAMESPACE) or "",
        "scala": properties.findtext("m:scala.version", namespaces=POM_NAMESPACE) or "",
        "spark": properties.findtext("m:spark.version", namespaces=POM_NAMESPACE) or "",
    }


def _called_name(call: ast.Call) -> str | None:
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return None


def _keyword(call: ast.Call, name: str) -> ast.expr | None:
    return next((item.value for item in call.keywords if item.arg == name), None)


def _literal(node: ast.expr | None):
    return ast.literal_eval(node) if node is not None else None


def _dag_contract(app: str) -> dict[str, object]:
    path = ROOT / "spark-apps" / app / "dag.py"
    module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    hook = next(
        node
        for node in ast.walk(module)
        if isinstance(node, ast.Call) and _called_name(node) == "SparkSubmitHook"
    )
    submit = next(
        node
        for node in ast.walk(module)
        if isinstance(node, ast.Call) and _called_name(node) == "submit_and_confirm_via_rest"
    )
    spark_conf = next(
        node.value
        for node in module.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "spark_conf" for target in node.targets)
    )
    assert isinstance(spark_conf, ast.Dict)
    conf = {
        _literal(key): _literal(value)
        for key, value in zip(spark_conf.keys, spark_conf.values, strict=True)
        if isinstance(key, ast.Constant) and isinstance(value, ast.Constant)
    }
    return {
        "application": _literal(_keyword(submit, "application")),
        "java_class": _literal(_keyword(hook, "java_class")),
        "deploy_mode": _literal(_keyword(hook, "deploy_mode")),
        "application_args": _literal(_keyword(hook, "application_args")),
        "extensions": conf["spark.sql.extensions"],
    }


def _jenkins_contract(app: str) -> dict[str, str]:
    text = (ROOT / "spark-apps" / app / "Jenkinsfile").read_text(encoding="utf-8")
    app_name = re.search(r"APP = '([^']+)'", text)
    version = re.search(r"VERSION = '([^']+)'", text)
    destination = re.search(
        r'MINIO_BUCKET_ICEBERG_JARS}/\$\{APP}/\$\{VERSION}/(app\.jar)', text
    )
    assert app_name and version and destination
    return {
        "application": (
            f"s3a://jars/{app_name.group(1)}/{version.group(1)}/{destination.group(1)}"
        )
    }


@pytest.mark.parametrize("app", ["nyc-taxi-etl", "nyc-taxi-medallion"])
def test_spark_app_docs_match_build_publish_and_dag_contracts(app: str):
    pom = _pom_contract(app)
    dag = _dag_contract(app)
    jenkins = _jenkins_contract(app)
    assert pom["artifact"] == app
    assert dag["application"] == jenkins["application"]

    java_class = str(dag["java_class"])
    entrypoint = f"src/main/scala/{java_class.replace('.', '/')}.scala"
    assert (ROOT / "spark-apps" / app / entrypoint).is_file()

    for relative in (
        f"docs/spark-apps/{app}.md",
        f"spark-apps/{app}/README.md",
    ):
        text = (ROOT / relative).read_text(encoding="utf-8")
        assert f"Scala ({pom['scala']})" in text
        assert f"Spark {pom['spark']}" in text
        assert f"`{entrypoint}`" in text
        assert f"`{java_class}`" in text
        assert f"`{dag['application']}`" in text
        assert "`dag.py`" in text
        assert f"`{dag['extensions']}`" in text
        assert "Spark standalone cluster mode" in text
        assert "src/main/scala/dag.py" not in text
        assert "YARN/K8s" not in text


def test_etl_docs_match_transform_and_positional_argument_contract():
    transform = (
        "src/main/scala/com/thekaveh/dataeng/nyctaxi/transforms/TaxiTransforms.scala"
    )
    source = (ROOT / "spark-apps/nyc-taxi-etl" / transform).read_text(encoding="utf-8")
    assert "def clean(df: DataFrame)" in source
    assert 'F.col("tpep_pickup_datetime")' in source

    for relative in (
        "docs/spark-apps/nyc-taxi-etl.md",
        "spark-apps/nyc-taxi-etl/README.md",
    ):
        text = (ROOT / relative).read_text(encoding="utf-8")
        assert f"`{transform}`" in text
        assert "`TaxiTransforms.clean`" in text
        assert "two positional arguments" in text
        assert "`tpep_pickup_datetime`" in text
        assert "`passenger_count`" in text
        assert "`createOrReplace()`" in text
        assert "TaxiTransforms.sanitize" not in text
        assert "--source" not in text
        assert "partitioned by trip_date" not in text


def test_medallion_docs_match_transform_output_and_fixed_table_contract():
    transform = (
        "src/main/scala/com/thekaveh/dataeng/medallion/transforms/MedallionTransforms.scala"
    )
    source = (ROOT / "spark-apps/nyc-taxi-medallion" / transform).read_text(
        encoding="utf-8"
    )
    assert 'F.count("*").as("trips")' in source

    for relative in (
        "docs/spark-apps/nyc-taxi-medallion.md",
        "spark-apps/nyc-taxi-medallion/README.md",
    ):
        text = (ROOT / relative).read_text(encoding="utf-8")
        assert f"`{transform}`" in text
        assert "one optional positional bronze-table argument" in text
        assert "`trips`" in text
        assert "`createOrReplace()`" in text
        assert "upstream DAG dependency" not in text
        assert "MedallionMedallion" not in text
        assert "trip_count" not in text
        assert "parameterised property tests" not in text


def test_spark_app_overview_matches_publish_and_runtime_ownership_contract():
    text = (ROOT / "docs/spark-apps/index.md").read_text(encoding="utf-8")
    assert "s3a://jars/<app>/0.1.0/app.jar" in text
    assert "Atlas Spark image supplies the Spark, S3A, and Iceberg runtime" in text
    assert "<app>.jar" not in text
    assert "Iceberg bindings bundled" not in text


def _scenario_result_rows(markdown: str) -> list[list[str]]:
    section = markdown.split("## Scenario Execution", 1)[1].split("## Trino Validation", 1)[0]
    rows = []
    for line in section.splitlines():
        if not line.startswith("|") or line.startswith("|---") or "| Scenario |" in line:
            continue
        rows.append([cell.strip() for cell in line.strip("|").split("|")])
    return rows


def test_go_live_scenario_matrix_covers_manifest_and_matches_summary_counts():
    manifest = load_manifest(ROOT / "docs/manifest.yaml", ROOT)
    expected = {
        section.source.stem.rsplit("-", 2)[0]
        for section in iter_leaf_sections(manifest.sections)
        if section.source is not None
        and section.source.parent == Path("docs/scenarios")
        and section.source.name != "index.md"
    }
    text = (ROOT / "docs/go-live-results.md").read_text(encoding="utf-8")
    rows = _scenario_result_rows(text)
    by_scenario = {row[0]: row[1:] for row in rows}

    assert len(rows) == len(by_scenario) == len(expected) == 19
    assert set(by_scenario) == expected
    assert sum(columns[0] == "PASS" for columns in by_scenario.values()) == 19
    assert sum(columns[1:] == ["PASS", "MATCH"] for columns in by_scenario.values()) == 17
    assert sum(columns[1:] == ["N/A", "—"] for columns in by_scenario.values()) == 2
    assert by_scenario["incremental_upsert-online_retail"] == ["PASS", "PASS", "MATCH"]
    assert "**Summary: 19/19 scenarios passed. 17/17 dual-language scenarios show parity.**" in text
