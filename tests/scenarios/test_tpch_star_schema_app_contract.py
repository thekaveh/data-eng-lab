from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
APP = ROOT / "spark-apps/tpch-star-schema"


def test_jenkins_builds_tests_and_publishes_exact_reviewed_jar():
    text = (APP / "Jenkinsfile").read_text(encoding="utf-8")
    assert text.index("mvn -q -B test") < text.index("mvn -q -B package -DskipTests") < text.index("mc cp")
    assert "target/${APP}-${VERSION}.jar" in text
    assert "${MINIO_BUCKET_ICEBERG_JARS}/${APP}/${VERSION}/app.jar" in text
    assert "MINIO_ICEBERG_ACCESS_KEY" in text and "MINIO_ICEBERG_SECRET_KEY" in text


def test_runbook_freezes_provenance_recovery_and_downstream_contract():
    text = (APP / "README.md").read_text(encoding="utf-8")
    for value in (
        "customer.parquet", "supplier.parquet", "decimal(25,2)", "data_eng_lab.dataset.plan_id",
        "data_eng_lab.dataset.publication_id", "data_eng_lab.dataset.manifest_sha256",
        '"dim_customer$properties"', '"fct_orders$properties"', "FINISHED", "success=true",
        "same immutable generation", "@daily", "s3a://jars/tpch-star-schema/0.1.0/app.jar",
    ):
        assert value in text


def test_downstream_guard_has_executable_exact_five_property_contract():
    text = (APP / "README.md").read_text(encoding="utf-8")
    for value in (
        '"dim_customer$properties"',
        '"fct_orders$properties"',
        "data_eng_lab.dataset",
        "data_eng_lab.dataset.scale",
        "data_eng_lab.dataset.plan_id",
        "data_eng_lab.dataset.publication_id",
        "data_eng_lab.dataset.manifest_sha256",
        "FULL OUTER JOIN",
        "property_key IS NULL",
        "dim_value <> fact_value",
        "fail closed before BI SQL",
    ):
        assert value in text


def test_notebooks_are_explicitly_outside_the_production_write_trust_boundary():
    for path in (
        ROOT / "scenarios/star_schema-tpch-spark-iceberg/README.md",
        ROOT / "docs/scenarios/star_schema-tpch-spark-iceberg.md",
        ROOT / "docs/notebooks/star_schema-tpch-spark-iceberg.md",
    ):
        text = path.read_text(encoding="utf-8")
        assert "not a production write path" in text
        assert "can invalidate downstream #83" in text
        assert "tpch_star_schema" in text


def test_live_gate_executes_real_lifecycle_instead_of_asserting_report_prose():
    text = (ROOT / "tests/scenarios/test_tpch_star_schema_live.py").read_text(encoding="utf-8")
    assert "report.read_text" not in text
    for value in (
        "scripts/start-all.sh",
        "scripts/stop-all.sh",
        "mvn",
        "scripts/resolve_dataset.py",
        "/api/v2",
        'f"/dags/{DAG_ID}',
        "dataset_scale",
        "FINISHED",
        "success",
        "SELECT key, value FROM lakehouse.gold.",
        "$properties",
        '_properties("dim_customer")',
        '_properties("fct_orders")',
        "snapshot_table",
        "finally:",
        "is_paused",
    ):
        assert value in text
