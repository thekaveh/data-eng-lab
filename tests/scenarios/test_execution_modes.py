from __future__ import annotations

import dataclasses
import re
from pathlib import Path

import pytest
import yaml

from scripts.scenario_execution import (
    CLASSIFICATIONS,
    ExecutionModeError,
    check_projection,
    load_execution_modes,
    parse_execution_modes,
    render_markdown,
    validate_execution_modes,
)

ROOT = Path(__file__).resolve().parents[2]
MATRIX = ROOT / "scenarios/execution-modes.yaml"
PROJECTION = ROOT / "docs/scenarios/execution-modes.md"

EXISTING = "existing production DAG"
APPROVED = "approved new production DAG"
NOTEBOOK_ONLY = "intentionally notebook-only"
UNSCHEDULED = "intentionally unscheduled long-running streaming"
DEPRECATED = "deprecated or superseded"

EXPECTED = {
    "batch_ingest-nyc_taxi-spark-iceberg": (EXISTING, None, "spark-apps/nyc-taxi-etl/dag.py"),
    "bi_query-tpch-trino-iceberg": (APPROVED, 83, None),
    "cdc_streaming-online_retail-spark-iceberg": (UNSCHEDULED, None, None),
    "data_quality-nyc_taxi-spark-iceberg": (APPROVED, 91, None),
    "feature_engineering-movielens-spark-iceberg": (APPROVED, 108, None),
    "federated_query-nyc_taxi-trino-iceberg": (APPROVED, 83, None),
    "incremental_upsert-online_retail-spark-iceberg": (NOTEBOOK_ONLY, None, None),
    "join_optimization-tpch-spark-iceberg": (NOTEBOOK_ONLY, None, None),
    "json_flatten-gh_archive-spark-iceberg": (APPROVED, 109, None),
    "medallion-nyc_taxi-spark-iceberg": (EXISTING, None, "spark-apps/nyc-taxi-medallion/dag.py"),
    "scd2-online_retail-spark-iceberg": (NOTEBOOK_ONLY, None, None),
    "schema_evolution-gh_archive-spark-iceberg": (NOTEBOOK_ONLY, None, None),
    "sessionization-gh_archive-spark-iceberg": (APPROVED, 109, None),
    "star_schema-tpch-spark-iceberg": (APPROVED, 107, None),
    "streaming_ingest-events-spark-iceberg": (UNSCHEDULED, None, None),
    "streaming_ingest-gh_archive-spark-iceberg": (NOTEBOOK_ONLY, None, None),
    "streaming_windows-events-spark-iceberg": (UNSCHEDULED, None, None),
    "table_maintenance-nyc_taxi-spark-iceberg": (NOTEBOOK_ONLY, None, None),
    "time_travel-nyc_taxi-spark-iceberg": (NOTEBOOK_ONLY, None, None),
}


def _document() -> dict:
    return yaml.safe_load(MATRIX.read_text(encoding="utf-8"))


def test_classification_vocabulary_matches_issue_82_exactly():
    assert CLASSIFICATIONS == (
        EXISTING,
        APPROVED,
        NOTEBOOK_ONLY,
        UNSCHEDULED,
        DEPRECATED,
    )


def test_matrix_covers_every_paired_scenario_exactly_once():
    modes = load_execution_modes(MATRIX, ROOT)
    discovered = {
        path.name
        for path in (ROOT / "scenarios").iterdir()
        if path.is_dir()
        and (path / "jupyter/notebook.ipynb").is_file()
        and (path / "zeppelin/notebook.zpln").is_file()
    }
    assert len(modes) == 19
    assert len({mode.scenario_id for mode in modes}) == 19
    assert {mode.scenario_id for mode in modes} == discovered == set(EXPECTED)


def test_reviewed_classifications_children_and_entrypoints_are_frozen():
    modes = load_execution_modes(MATRIX, ROOT)
    actual = {
        mode.scenario_id: (mode.classification, mode.child_issue, mode.execution_entrypoint)
        for mode in modes
    }
    assert actual == EXPECTED
    counts = {classification: 0 for classification in CLASSIFICATIONS}
    for mode in modes:
        counts[mode.classification] += 1
    assert counts == {
        EXISTING: 2,
        APPROVED: 7,
        NOTEBOOK_ONLY: 7,
        UNSCHEDULED: 3,
        DEPRECATED: 0,
    }
    assert {mode.child_issue for mode in modes if mode.child_issue is not None} == {
        83,
        91,
        107,
        108,
        109,
    }


def test_every_row_has_complete_reviewable_contract_fields():
    modes = load_execution_modes(MATRIX, ROOT)
    expected_fields = {
        "scenario_id",
        "classification",
        "justification",
        "owner",
        "runtime",
        "schedule_policy",
        "execution_entrypoint",
        "dependencies",
        "acceptance_contract",
        "child_issue",
    }
    assert {field.name for field in dataclasses.fields(modes[0])} == expected_fields
    for mode in modes:
        assert all(
            value.strip()
            for value in (
                mode.scenario_id,
                mode.classification,
                mode.justification,
                mode.owner,
                mode.runtime,
                mode.schedule_policy,
            )
        )
        assert mode.dependencies and all(value.strip() for value in mode.dependencies)
        assert mode.acceptance_contract and all(value.strip() for value in mode.acceptance_contract)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda document: document.update(extra=True), "unknown top-level fields"),
        (lambda document: document["scenarios"][0].update(extra=True), "unknown row fields"),
        (lambda document: document["scenarios"][0].pop("owner"), "missing row fields"),
        (lambda document: document["scenarios"][0].update(owner=""), "owner must be a non-empty string"),
        (lambda document: document["scenarios"][0].update(dependencies=[]), "dependencies must be a non-empty list"),
        (
            lambda document: document["scenarios"][0].update(classification="scheduled placeholder"),
            "unknown classification",
        ),
        (
            lambda document: document["scenarios"].append(dict(document["scenarios"][0])),
            "duplicate scenario_id",
        ),
    ],
)
def test_parser_rejects_schema_drift(mutation, message):
    document = _document()
    mutation(document)
    with pytest.raises(ExecutionModeError, match=message):
        parse_execution_modes(yaml.safe_dump(document, sort_keys=False))


def test_parser_wraps_invalid_yaml():
    with pytest.raises(ExecutionModeError, match="invalid YAML"):
        parse_execution_modes("scenarios: [")


def test_classification_semantics_and_paths_validate():
    modes = load_execution_modes(MATRIX, ROOT)
    validate_execution_modes(modes, ROOT)
    for mode in modes:
        if mode.classification == EXISTING:
            assert mode.execution_entrypoint is not None
            assert (ROOT / mode.execution_entrypoint).is_file()
            assert mode.child_issue is None
        elif mode.classification == APPROVED:
            assert mode.execution_entrypoint is None
            assert mode.child_issue is not None
        else:
            assert mode.execution_entrypoint is None
            assert mode.child_issue is None
        if mode.classification == UNSCHEDULED:
            assert "unscheduled" in mode.schedule_policy.casefold()


def test_no_scenario_local_dag_or_runtime_empty_operator_remains():
    assert list((ROOT / "scenarios").rglob("dag.py")) == []
    production_dags = sorted((ROOT / "spark-apps").rglob("dag.py"))
    assert [path.relative_to(ROOT).as_posix() for path in production_dags] == [
        "spark-apps/nyc-taxi-etl/dag.py",
        "spark-apps/nyc-taxi-medallion/dag.py",
    ]
    assert all("EmptyOperator" not in path.read_text(encoding="utf-8") for path in production_dags)


def test_production_dag_mount_and_matrix_entrypoints_agree():
    overlay = (ROOT / "compose/data-eng-lab.yml").read_text(encoding="utf-8")
    assert "../spark-apps:/opt/airflow/dags/data_eng_lab_spark_apps:ro" in overlay
    modes = load_execution_modes(MATRIX, ROOT)
    assert {
        mode.execution_entrypoint
        for mode in modes
        if mode.execution_entrypoint is not None
    } == {
        path.relative_to(ROOT).as_posix()
        for path in (ROOT / "spark-apps").rglob("dag.py")
    }


def test_markdown_projection_is_deterministic_and_current():
    modes = load_execution_modes(MATRIX, ROOT)
    rendered = render_markdown(modes)
    assert rendered == render_markdown(modes)
    assert rendered == PROJECTION.read_text(encoding="utf-8")
    assert check_projection(ROOT) == ()
    assert sum(line.startswith("| `") for line in rendered.splitlines()) == 19
    for scenario_id, (classification, child, entrypoint) in EXPECTED.items():
        assert f"`{scenario_id}`" in rendered
        assert classification in rendered
        if child is not None:
            assert f"#{child}" in rendered
        if entrypoint is not None:
            assert f"`{entrypoint}`" in rendered


def test_repository_and_docs_aggregate_gates_include_matrix_validation():
    verify_text = (ROOT / "scripts/verify_repo.py").read_text(encoding="utf-8")
    docs_text = (ROOT / "scripts/docs/check_docs.py").read_text(encoding="utf-8")
    assert "_check_scenario_execution_modes" in verify_text
    assert "check_execution_modes" in docs_text


def test_current_public_docs_never_publish_no_op_dags_or_false_triggers():
    public_paths = [
        ROOT / "README.md",
        *sorted((ROOT / "docs").rglob("*.md")),
        *sorted((ROOT / "scenarios").glob("*/README.md")),
    ]
    public_paths = [path for path in public_paths if "docs/superpowers/" not in path.as_posix()]
    text = "\n".join(path.read_text(encoding="utf-8") for path in public_paths)
    assert "EmptyOperator" not in text
    assert "Atlas #268" not in text
    assert "airflow dags trigger batch_ingest_nyc_taxi" not in text
    assert "airflow dags trigger medallion_nyc_taxi" not in text
    obsolete = {
        match.group(1)
        for match in re.finditer(r"airflow dags trigger ([a-z0-9_]+)", text)
    }
    assert obsolete <= {"nyc_taxi_etl", "nyc_taxi_medallion"}


def test_indexes_and_every_scenario_surface_link_the_matrix():
    assert "execution-modes.md" in (ROOT / "docs/scenarios/index.md").read_text(encoding="utf-8")
    assert "execution-modes.md" in (ROOT / "docs/notebooks/index.md").read_text(encoding="utf-8")
    for scenario_id, (classification, child, entrypoint) in EXPECTED.items():
        for path in (
            ROOT / "scenarios" / scenario_id / "README.md",
            ROOT / "docs/scenarios" / f"{scenario_id}.md",
        ):
            text = path.read_text(encoding="utf-8")
            assert classification in text, path
            if child is not None:
                assert f"#{child}" in text, path
            if entrypoint is not None:
                assert entrypoint in text, path


def test_diagram_masters_do_not_claim_obsolete_scenario_dags():
    for scenario_id, (classification, child, entrypoint) in EXPECTED.items():
        path = ROOT / "docs/diagrams" / f"{scenario_id}.html"
        text = path.read_text(encoding="utf-8")
        assert "EmptyOperator" not in text, path
        assert "placeholder" not in text.casefold(), path
        assert "Atlas #268" not in text, path
        assert classification in text, path
        if child is not None:
            assert f"#{child}" in text, path
        if entrypoint is not None:
            assert entrypoint in text, path
