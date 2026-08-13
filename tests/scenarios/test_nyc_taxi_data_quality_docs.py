from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
SCENARIO_ID = "data_quality-nyc_taxi-spark-iceberg"
RULE_IDS = (
    "bronze.source_available.v1",
    "bronze.schema.v1",
    "bronze.snapshot_freshness.v1",
    "bronze.invalid_ratio.v1",
    "silver.partition_conservation.v1",
    "silver.clean_nonempty.v1",
    "silver.quarantine_ratio.v1",
    "silver.output_readback.v1",
)


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_execution_matrix_promotes_the_reviewed_quality_dag_exactly():
    document = yaml.safe_load(_read("scenarios/execution-modes.yaml"))
    row = next(item for item in document["scenarios"] if item["scenario_id"] == SCENARIO_ID)
    assert row["classification"] == "existing production DAG"
    assert row["execution_entrypoint"] == "spark-apps/nyc-taxi-data-quality/dag.py"
    assert row["child_issue"] is None
    assert row["runtime"] == "Production Atlas Airflow and Spark standalone application"
    assert row["schedule_policy"] == "Serialized @daily after the matching successful nyc_taxi_etl logical date"


def test_scenario_surfaces_document_current_production_and_notebook_trust_boundaries():
    for path in (
        f"scenarios/{SCENARIO_ID}/README.md",
        f"docs/scenarios/{SCENARIO_ID}.md",
    ):
        text = _read(path)
        for token in (
            "existing production DAG",
            "spark-apps/nyc-taxi-data-quality/dag.py",
            "nyc_taxi_data_quality",
            "@daily",
            "max_active_runs=1",
            "wait_for_nyc_taxi_etl",
            "submit_nyc_taxi_data_quality",
            "lakehouse.bronze.nyc_taxi_trips",
            "lakehouse.silver.nyc_taxi_clean",
            "lakehouse.silver.nyc_taxi_quarantine",
            "lakehouse.gold.nyc_taxi_quality_facts",
            "snapshot-bound",
            "queries/latest.sql",
            "queries/trend.sql",
            "queries/operator_attention.sql",
            "2026-08-13-nyc-taxi-data-quality-live-acceptance.md",
            "five-key",
        ):
            assert token in text, (path, token)
        assert all(rule in text for rule in RULE_IDS)
        lowered = text.casefold()
        assert "without production provenance" in lowered
        assert "bypass airflow serialization" in lowered
        assert "production writes must use" in lowered
        assert "approved new production dag" not in lowered
        assert "no production dag exists" not in lowered
        assert "future production scope" not in lowered
        assert "not (rule) or fare_amount is null" not in lowered


def test_catalog_runbook_changelog_and_manifest_publish_six_apps_and_eight_dags():
    apps = _read("docs/spark-apps/index.md")
    scenarios = _read("docs/scenarios/index.md")
    go_live = _read("docs/go-live.md")
    changelog = _read("docs/CHANGELOG.md")
    manifest = _read("docs/manifest.yaml")
    assert "All six apps" in apps
    assert "nyc-taxi-data-quality" in apps
    assert "`nyc_taxi_data_quality`" in apps
    assert "eight production DAGs" in scenarios
    assert "nine scenarios" in scenarios
    assert "`nyc_taxi_data_quality`" in go_live
    assert "spark-apps/nyc-taxi-data-quality/README.md" in go_live
    assert "NYC Taxi data-quality" in changelog
    assert "docs/spark-apps/nyc-taxi-data-quality.md" in manifest


def test_architecture_master_matches_the_production_contract():
    text = _read(f"docs/diagrams/{SCENARIO_ID}.html")
    for token in (
        "nyc_taxi_etl",
        "ExternalTaskSensor",
        "nyc_taxi_data_quality",
        "snapshot-bound",
        "null-safe",
        "nyc_taxi_clean",
        "nyc_taxi_quarantine",
        "nyc_taxi_quality_facts",
        "Fixed Trino dashboards",
        "same-date rerun",
        "existing production DAG",
        "spark-apps/nyc-taxi-data-quality/dag.py",
        "execution-modes.yaml",
    ):
        assert token in text, token
