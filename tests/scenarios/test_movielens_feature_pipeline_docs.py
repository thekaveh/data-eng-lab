from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
SCENARIO = ROOT / "scenarios/feature_engineering-movielens-spark-iceberg/README.md"
SCENARIO_DOC = ROOT / "docs/scenarios/feature_engineering-movielens-spark-iceberg.md"
NOTEBOOK_DOC = ROOT / "docs/notebooks/feature_engineering-movielens-spark-iceberg.md"
APP_DOC = ROOT / "docs/spark-apps/movielens-feature-pipeline.md"
DIAGRAM = ROOT / "docs/diagrams/feature_engineering-movielens-spark-iceberg.html"
MATRIX = ROOT / "scenarios/execution-modes.yaml"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_canonical_docs_freeze_the_two_gold_table_contract():
    required = {
        "lakehouse.gold.ml_user_features",
        "userId long",
        "avg_rating double",
        "num_ratings long",
        "lakehouse.gold.ml_movie_features",
        "movieId long",
        "movie_avg double",
        "popularity long",
        "Duplicate rating rows intentionally count separately",
    }
    for path in (SCENARIO, SCENARIO_DOC, APP_DOC):
        text = _read(path)
        assert required <= {phrase for phrase in required if phrase in text}, path


def test_docs_identify_the_only_supported_production_write_path_and_notebook_risk():
    for path in (SCENARIO, SCENARIO_DOC, NOTEBOOK_DOC, APP_DOC):
        text = _read(path)
        assert "movielens_feature_pipeline" in text, path
        assert "spark-apps/movielens-feature-pipeline" in text, path
        assert "not a supported production write path" in text, path
        assert "provenance" in text, path


def test_stale_preproduction_and_three_table_claims_are_gone():
    forbidden = {
        "approved new production DAG",
        "No production DAG exists yet",
        "rating deviation",
        "genre distributions",
        "user_item_interactions",
        "lakehouse.silver.user_features",
        "lakehouse.silver.item_features",
    }
    for path in (SCENARIO, SCENARIO_DOC, APP_DOC, DIAGRAM):
        text = _read(path)
        assert not {phrase for phrase in forbidden if phrase in text}, path


def test_diagram_shows_the_verified_serialized_production_boundary():
    text = _read(DIAGRAM)
    for phrase in (
        "Resolver-verified MovieLens publication",
        "max_active_runs=1",
        "RestConfirmingSparkHook",
        "Explicit ratings schema",
        "User features first",
        "Movie features second",
        "Five equal provenance properties",
        "No cross-table atomic commit",
    ):
        assert phrase in text


def test_execution_mode_is_promoted_to_the_live_proven_entrypoint():
    document = yaml.safe_load(_read(MATRIX))
    row = next(
        item
        for item in document["scenarios"]
        if item["scenario_id"] == "feature_engineering-movielens-spark-iceberg"
    )
    assert row["classification"] == "existing production DAG"
    assert row["execution_entrypoint"] == "spark-apps/movielens-feature-pipeline/dag.py"
    assert row["child_issue"] is None
    assert row["schedule_policy"].startswith("@daily")


def test_public_indexes_and_projection_count_the_four_production_apps():
    assert "Four CI-built Maven applications" in _read(ROOT / "README.md")
    assert "Four CI-built Maven applications" in _read(ROOT / "docs/index.md")
    projection = _read(ROOT / "docs/scenarios/execution-modes.md")
    assert "`movielens_feature_pipeline`" in projection
    assert "four production DAGs today" in projection
    catalog = _read(ROOT / "docs/scenarios/index.md")
    assert "four production DAGs" in catalog
    assert "two production DAGs" not in catalog
