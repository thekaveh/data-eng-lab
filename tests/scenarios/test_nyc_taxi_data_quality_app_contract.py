from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
APP = ROOT / "spark-apps/nyc-taxi-data-quality"


def test_pom_freezes_reviewed_runtime_and_artifact():
    root = ET.parse(APP / "pom.xml").getroot()
    ns = {"m": "http://maven.apache.org/POM/4.0.0"}
    assert root.findtext("m:groupId", namespaces=ns) == "com.thekaveh.dataeng"
    assert root.findtext("m:artifactId", namespaces=ns) == "nyc-taxi-data-quality"
    assert root.findtext("m:version", namespaces=ns) == "0.1.0"
    assert root.findtext("m:properties/m:spark.version", namespaces=ns) == "4.1.2"
    assert root.findtext("m:properties/m:scala.version", namespaces=ns) == "2.13.14"


def test_jenkins_tests_packages_and_publishes_exact_reviewed_jar():
    text = (APP / "Jenkinsfile").read_text(encoding="utf-8")
    for fragment in (
        "APP = 'nyc-taxi-data-quality'",
        "VERSION = '0.1.0'",
        "mvn -q -B test",
        "mvn -q -B package -DskipTests",
        'target/${APP}-${VERSION}.jar',
        'atlas/${MINIO_BUCKET_ICEBERG_JARS}/${APP}/${VERSION}/app.jar',
        "MINIO_ICEBERG_ACCESS_KEY",
        "MINIO_ICEBERG_SECRET_KEY",
    ):
        assert fragment in text
    assert "set -x" not in text and "echo $MINIO" not in text


def test_runbook_freezes_snapshot_only_policy_rules_and_recovery_contract():
    text = (APP / "README.md").read_text(encoding="utf-8")
    for fragment in (
        "lakehouse.bronze.nyc_taxi_trips",
        "lakehouse.silver.nyc_taxi_clean",
        "lakehouse.silver.nyc_taxi_quarantine",
        "lakehouse.gold.nyc_taxi_quality_facts",
        "nyc_taxi_quality_v1",
        "bronze.invalid_ratio.v1",
        "silver.quarantine_ratio.v1",
        "1%",
        "5%",
        "same logical date",
        "max_active_runs=1",
        "Concurrent direct JAR execution is unsupported",
        "not five-key generation provenance",
        "deferred",
        "non-atomic",
        "Rerun the same logical date",
        "queries/latest.sql",
        "queries/trend.sql",
        "queries/operator_attention.sql",
        "ExternalTaskSensor",
        "RestConfirmingSparkHook",
    ):
        assert fragment in text


def test_runbook_documents_exact_schemas_and_operational_commands():
    text = (APP / "README.md").read_text(encoding="utf-8")
    for column in (
        "VendorID long",
        "tpep_pickup_datetime timestamp_ntz",
        "trip_distance double",
        "fare_amount double",
        "trip_date date",
        "quality_run_id string",
        "source_snapshot_id long",
        "metric_value decimal(38,9)",
        "diagnostic_code string",
    ):
        assert column in text
    assert "mvn -q -B -f spark-apps/nyc-taxi-data-quality/pom.xml test" in text
    assert "trino --file spark-apps/nyc-taxi-data-quality/queries/latest.sql" in text
