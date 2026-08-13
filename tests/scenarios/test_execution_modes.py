from __future__ import annotations

import dataclasses
import html
import json
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
    "bi_query-tpch-trino-iceberg": (
        EXISTING,
        None,
        "airflow-dags/trino_bi/dag.py",
    ),
    "cdc_streaming-online_retail-spark-iceberg": (UNSCHEDULED, None, None),
    "data_quality-nyc_taxi-spark-iceberg": (
        EXISTING,
        None,
        "spark-apps/nyc-taxi-data-quality/dag.py",
    ),
    "feature_engineering-movielens-spark-iceberg": (
        EXISTING,
        None,
        "spark-apps/movielens-feature-pipeline/dag.py",
    ),
    "federated_query-nyc_taxi-trino-iceberg": (
        EXISTING,
        None,
        "airflow-dags/trino_bi/dag.py",
    ),
    "incremental_upsert-online_retail-spark-iceberg": (NOTEBOOK_ONLY, None, None),
    "join_optimization-tpch-spark-iceberg": (NOTEBOOK_ONLY, None, None),
    "json_flatten-gh_archive-spark-iceberg": (
        EXISTING,
        None,
        "spark-apps/gh-archive-pipeline/dag.py",
    ),
    "medallion-nyc_taxi-spark-iceberg": (EXISTING, None, "spark-apps/nyc-taxi-medallion/dag.py"),
    "scd2-online_retail-spark-iceberg": (NOTEBOOK_ONLY, None, None),
    "schema_evolution-gh_archive-spark-iceberg": (NOTEBOOK_ONLY, None, None),
    "sessionization-gh_archive-spark-iceberg": (
        EXISTING,
        None,
        "spark-apps/gh-archive-pipeline/dag.py",
    ),
    "star_schema-tpch-spark-iceberg": (EXISTING, None, "spark-apps/tpch-star-schema/dag.py"),
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
        EXISTING: 9,
        APPROVED: 0,
        NOTEBOOK_ONLY: 7,
        UNSCHEDULED: 3,
        DEPRECATED: 0,
    }
    assert {mode.child_issue for mode in modes if mode.child_issue is not None} == set()


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


@pytest.mark.parametrize(
    "text",
    [
        "version: 1\nversion: 1\nscenarios: []\n",
        "version: 1\nscenarios:\n  - scenario_id: x\n    scenario_id: y\n",
    ],
)
def test_parser_rejects_duplicate_mapping_keys_at_every_level(text):
    with pytest.raises(ExecutionModeError, match="duplicate mapping key"):
        parse_execution_modes(text)


@pytest.mark.parametrize("version", [True, 1.0, "1"])
def test_parser_requires_exact_non_boolean_integer_version_one(version):
    document = _document()
    document["version"] = version
    with pytest.raises(ExecutionModeError, match="version must be integer 1"):
        parse_execution_modes(yaml.safe_dump(document, sort_keys=False))


def test_issue_references_survive_yaml_parse_and_markdown_projection_exactly():
    text = MATRIX.read_text(encoding="utf-8")
    issue_phrases = {
        "#78 operator-owned Spark submission",
        "#81 verified dataset resolver",
        "checkpoint ownership policy #85",
        "events producer and checkpoint ownership policy #85",
        "#91 durable quality monitoring",
    }
    for line in text.splitlines():
        if "#" in line and not line.lstrip().startswith("#"):
            assert re.search(r"(['\"]).*#.*\1\s*$", line), line
    modes = load_execution_modes(MATRIX, ROOT)
    rendered = render_markdown(modes)
    parsed_phrases = {
        value
        for mode in modes
        for value in (*mode.dependencies, mode.runtime, mode.schedule_policy, *mode.acceptance_contract)
    }
    assert issue_phrases <= parsed_phrases
    assert all(phrase in rendered for phrase in issue_phrases)


def test_nyc_quality_notebooks_use_null_safe_conservation_and_warn_against_production_writes():
    scenario = ROOT / "scenarios/data_quality-nyc_taxi-spark-iceberg"
    jupyter = json.loads((scenario / "jupyter/notebook.ipynb").read_text(encoding="utf-8"))
    zeppelin = json.loads((scenario / "zeppelin/notebook.zpln").read_text(encoding="utf-8"))
    python = "\n".join("".join(cell.get("source", [])) for cell in jupyter["cells"])
    scala = "\n".join(paragraph.get("text", "") for paragraph in zeppelin["paragraphs"])
    exact_columns = {
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
        "trip_date",
    }
    for text in (python, scala):
        assert "coalesce" in text and "source_count" in text
        assert "valid_count + quarantine_count == source_count" in text
        assert "NOT (" not in text and "fare_amount IS NULL" not in text
        assert all(column in text for column in exact_columns)
        assert "lakehouse.bronze.nyc_taxi_trips" in text
        assert "lakehouse.silver.nyc_taxi_clean" in text
        assert "lakehouse.silver.nyc_taxi_quarantine" in text
        warning = text.casefold()
        assert "production writes must use" in warning
        assert "without production provenance" in warning
        assert "bypass airflow serialization" in warning
        assert "invalidate" in warning and "quality facts" in warning


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
        "spark-apps/gh-archive-pipeline/dag.py",
        "spark-apps/movielens-feature-pipeline/dag.py",
        "spark-apps/nyc-taxi-data-quality/dag.py",
        "spark-apps/nyc-taxi-etl/dag.py",
        "spark-apps/nyc-taxi-medallion/dag.py",
        "spark-apps/tpch-star-schema/dag.py",
    ]
    assert all("EmptyOperator" not in path.read_text(encoding="utf-8") for path in production_dags)
    consumer_dags = sorted((ROOT / "airflow-dags").rglob("dag.py"))
    assert [path.relative_to(ROOT).as_posix() for path in consumer_dags] == [
        "airflow-dags/trino_bi/dag.py",
    ]


def test_zero_scenario_dags_is_independent_of_matrix_loading():
    assert not list((ROOT / "scenarios").rglob("dag.py"))
    assert "dag.py" not in (ROOT / "tests/scenarios/build_notebooks.py").read_text(encoding="utf-8")


def test_production_dag_mount_and_matrix_entrypoints_agree():
    overlay = (ROOT / "compose/data-eng-lab.yml").read_text(encoding="utf-8")
    assert "../spark-apps:/opt/airflow/dags/data_eng_lab_spark_apps:ro" in overlay
    assert "../airflow-dags:/opt/airflow/dags/data_eng_lab_airflow_dags:ro" in overlay
    assert "../scenarios:/opt/airflow/dags" not in overlay
    assert overlay.count("../spark-apps:/opt/airflow/dags/data_eng_lab_spark_apps:ro") == 2
    modes = load_execution_modes(MATRIX, ROOT)
    assert {
        mode.execution_entrypoint
        for mode in modes
        if mode.execution_entrypoint is not None
    } == {
        path.relative_to(ROOT).as_posix()
        for parent in (ROOT / "spark-apps", ROOT / "airflow-dags")
        for path in parent.rglob("dag.py")
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


def test_repository_verifier_fails_closed_when_matrix_is_absent(tmp_path):
    from scripts.verify_repo import _check_scenario_execution_modes

    findings = _check_scenario_execution_modes(tmp_path, {})
    assert [finding.message for finding in findings] == ["scenarios/execution-modes.yaml is required"]


def test_current_public_docs_never_publish_no_op_dags_or_false_triggers():
    public_paths = [
        ROOT / "README.md",
        *sorted((ROOT / "docs").rglob("*.md")),
        *sorted((ROOT / "scenarios").glob("*/README.md")),
    ]
    public_paths = [path for path in public_paths if "docs/superpowers/" not in path.as_posix()]
    text = "\n".join(path.read_text(encoding="utf-8") for path in public_paths)
    assert "EmptyOperator" not in text
    assert not re.search(r"atlas\s*(?:issue\s*)?#?\s*(?:268|269)", text, re.IGNORECASE)
    assert "airflow dags trigger batch_ingest_nyc_taxi" not in text
    assert "airflow dags trigger medallion_nyc_taxi" not in text
    obsolete = {
        match.group(1)
        for match in re.finditer(r"airflow dags trigger ([a-z0-9_]+)", text)
    }
    assert obsolete <= {
        "gh_archive_flatten_sessionization",
        "movielens_feature_pipeline",
        "nyc_taxi_trino_daily",
        "nyc_taxi_etl",
        "nyc_taxi_medallion",
        "tpch_bi_query",
        "tpch_star_schema",
    }


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
        assert " and dag.py" not in text, path
        assert "execution-modes.yaml" in text, path


def test_notebook_only_maintenance_claims_match_executable_cells():
    maintenance = load_execution_modes(MATRIX, ROOT)
    by_id = {mode.scenario_id: mode for mode in maintenance}
    table = by_id["table_maintenance-nyc_taxi-spark-iceberg"]
    travel = by_id["time_travel-nyc_taxi-spark-iceberg"]
    assert "rewrite_data_files" in table.justification
    assert "expire_snapshots" in table.acceptance_contract[0]
    assert "remove_orphan_files" in table.acceptance_contract[0]
    assert "history metadata" in travel.justification
    assert "CREATE BRANCH" in travel.acceptance_contract[0]
    assert "does not execute a time-travel query or rollback" in travel.acceptance_contract[1]
    for scenario_id in ("table_maintenance-nyc_taxi-spark-iceberg", "time_travel-nyc_taxi-spark-iceberg"):
        readme = (ROOT / "scenarios" / scenario_id / "README.md").read_text(encoding="utf-8")
        canonical = (ROOT / "docs/scenarios" / f"{scenario_id}.md").read_text(encoding="utf-8")
        assert "VACUUM" not in readme and "VACUUM" not in canonical
        assert "partition overwrite" not in readme and "partition overwrite" not in canonical
        assert "fast-forward" not in readme and "fast-forward" not in canonical
    for path in (
        ROOT / "scenarios/table_maintenance-nyc_taxi-spark-iceberg/README.md",
        ROOT / "docs/scenarios/table_maintenance-nyc_taxi-spark-iceberg.md",
    ):
        assert "`silver` and `gold`" not in path.read_text(encoding="utf-8")
    for path in (
        ROOT / "scenarios/time_travel-nyc_taxi-spark-iceberg/README.md",
        ROOT / "docs/scenarios/time_travel-nyc_taxi-spark-iceberg.md",
    ):
        assert "Also demonstrates time travel" not in path.read_text(encoding="utf-8")


def test_notebook_only_pages_never_claim_an_unrelated_spark_app_productionizes_them():
    modes = load_execution_modes(MATRIX, ROOT)
    for mode in modes:
        if mode.classification != NOTEBOOK_ONLY:
            continue
        for path in (
            ROOT / "scenarios" / mode.scenario_id / "README.md",
            ROOT / "docs/scenarios" / f"{mode.scenario_id}.md",
        ):
            text = path.read_text(encoding="utf-8").casefold()
            assert "productionizes this scenario for airflow" not in text, path
            assert "production spark app: nyc-taxi-medallion" not in text, path


def _notebook_executable_text(scenario_id: str) -> tuple[str, str]:
    root = ROOT / "scenarios" / scenario_id
    jupyter = json.loads((root / "jupyter/notebook.ipynb").read_text(encoding="utf-8"))
    jupyter_code = "\n".join(
        "".join(cell.get("source", []))
        if isinstance(cell.get("source", []), list)
        else cell.get("source", "")
        for cell in jupyter["cells"]
        if cell.get("cell_type") == "code"
    )
    zeppelin = json.loads((root / "zeppelin/notebook.zpln").read_text(encoding="utf-8"))
    zeppelin_code = "\n".join(
        paragraph.get("text", "")
        for paragraph in zeppelin["paragraphs"]
        if paragraph.get("text", "").startswith("%spark")
    )
    return jupyter_code, zeppelin_code


def test_data_quality_docs_match_both_executable_notebooks_exactly():
    jupyter, zeppelin = _notebook_executable_text("data_quality-nyc_taxi-spark-iceberg")
    exact_contract = {
        'spark.table("lakehouse.bronze.nyc_taxi_trips")',
        'fare_amount > 0 AND passenger_count BETWEEN 1 AND 6',
        'lakehouse.silver.nyc_taxi_clean',
        'lakehouse.silver.nyc_taxi_quarantine',
        'createOrReplace()',
    }
    assert all(value in jupyter and value in zeppelin for value in exact_contract)
    forbidden = (
        "custom spark sql function",
        "no downstream sink",
        "csv seed",
        "30-minute",
        "1800 seconds",
    )
    for path in (
        ROOT / "scenarios/data_quality-nyc_taxi-spark-iceberg/README.md",
        ROOT / "docs/scenarios/data_quality-nyc_taxi-spark-iceberg.md",
    ):
        text = path.read_text(encoding="utf-8")
        assert all(value in text for value in exact_contract - {'createOrReplace()'})
        assert "createOrReplace" in text
        assert all(value not in text.casefold() for value in forbidden), path
        assert "without production provenance" in text
        assert "production writes must use" in text


def test_all_scenario_diagram_visible_text_is_ascii_for_portable_png_rendering():
    for scenario_id in EXPECTED:
        path = ROOT / "docs/diagrams" / f"{scenario_id}.html"
        visible = " ".join(
            html.unescape(re.sub(r"<[^>]+>", "", value))
            for value in re.findall(r"<text\b[^>]*>(.*?)</text>", path.read_text(encoding="utf-8"), re.S)
        )
        assert visible.isascii(), (path, sorted({character for character in visible if not character.isascii()}))


def test_acceptance_renderer_uses_clean_list_punctuation():
    rendered = render_markdown(load_execution_modes(MATRIX, ROOT))
    assert ".;" not in rendered
