from __future__ import annotations

import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
SCENARIOS = (
    "json_flatten-gh_archive-spark-iceberg",
    "sessionization-gh_archive-spark-iceberg",
)


def _matrix_rows():
    document = yaml.safe_load((ROOT / "scenarios/execution-modes.yaml").read_text())
    return {row["scenario_id"]: row for row in document["scenarios"]}


def test_both_gh_archive_stages_are_existing_serialized_production_entries():
    rows = _matrix_rows()
    for scenario in SCENARIOS:
        row = rows[scenario]
        assert row["classification"] == "existing production DAG"
        assert row["child_issue"] is None
        assert row["execution_entrypoint"] == "spark-apps/gh-archive-pipeline/dag.py"
        assert "@daily" in row["schedule_policy"]
        assert "max_active_runs=1" in row["schedule_policy"]


def test_session_notebooks_consume_flat_table_with_exact_deterministic_contract():
    jupyter = json.loads(
        (ROOT / f"scenarios/{SCENARIOS[1]}/jupyter/notebook.ipynb").read_text()
    )
    jupyter_text = "\n".join("".join(cell.get("source", [])) for cell in jupyter["cells"])
    zeppelin = json.loads(
        (ROOT / f"scenarios/{SCENARIOS[1]}/zeppelin/notebook.zpln").read_text()
    )
    zeppelin_text = "\n".join(paragraph["text"] for paragraph in zeppelin["paragraphs"])
    for text in (jupyter_text, zeppelin_text):
        assert 'lakehouse.silver.gh_events' in text
        assert "actor_login" in text and "created_at" in text and "id" in text
        assert "previous_created_at" in text and "new_session" in text and "session_id" in text
        assert "1800" in text
        assert "DATASET_RESOLVER_URI" not in text
        assert "spark.read.json" not in text
        assert "does not write production provenance" in text
        assert "gh_archive_flatten_sessionization" in text


def test_all_four_educational_notebooks_warn_against_production_writes():
    for scenario in SCENARIOS:
        jupyter = json.loads(
            (ROOT / f"scenarios/{scenario}/jupyter/notebook.ipynb").read_text()
        )
        zeppelin = json.loads(
            (ROOT / f"scenarios/{scenario}/zeppelin/notebook.zpln").read_text()
        )
        texts = (
            "\n".join("".join(cell.get("source", [])) for cell in jupyter["cells"]),
            "\n".join(paragraph["text"] for paragraph in zeppelin["paragraphs"]),
        )
        for text in texts:
            assert "Production-risk warning" in text
            assert "does not write production provenance" in text
            assert "not serialized" in text
            assert "gh_archive_flatten_sessionization" in text


def test_notebook_index_names_both_gh_archive_production_prototypes():
    index = (ROOT / "docs/notebooks/index.md").read_text(encoding="utf-8")
    assert "json_flatten-gh_archive-spark-iceberg" in index
    assert "sessionization-gh_archive-spark-iceberg" in index
    assert "gh_archive_flatten_sessionization" in index
    assert "educational" in index


def test_public_counts_and_diagrams_describe_five_production_dags():
    for path in (ROOT / "README.md", ROOT / "docs/index.md", ROOT / "docs/scenarios/index.md"):
        text = path.read_text(encoding="utf-8")
        assert "five production dags" in text.lower()
        assert "four production dags" not in text.lower()
    projection = (ROOT / "docs/scenarios/execution-modes.md").read_text(encoding="utf-8")
    assert "`gh_archive_flatten_sessionization`" in projection
    assert "five production DAGs today" in projection
    for scenario in SCENARIOS:
        diagram = (ROOT / f"docs/diagrams/{scenario}.html").read_text(encoding="utf-8")
        assert "existing production DAG" in diagram
        assert "gh_archive_flatten_sessionization" in diagram
        assert "spark-apps/gh-archive-pipeline/dag.py" in diagram
