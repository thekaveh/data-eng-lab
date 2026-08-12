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
