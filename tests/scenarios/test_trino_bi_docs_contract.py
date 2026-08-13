from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCENARIOS = (
    "bi_query-tpch-trino-iceberg",
    "federated_query-nyc_taxi-trino-iceberg",
)


def _notebook_text(path: Path) -> str:
    document = json.loads(path.read_text(encoding="utf-8"))
    if path.suffix == ".ipynb":
        return "\n".join(
            "".join(cell.get("source", []))
            if isinstance(cell.get("source", []), list)
            else str(cell.get("source", ""))
            for cell in document["cells"]
        )
    return "\n".join(str(paragraph.get("text", "")) for paragraph in document["paragraphs"])


def test_public_surfaces_publish_two_read_only_production_dags_and_xcom_retention():
    paths = [
        ROOT / "README.md",
        ROOT / "docs/index.md",
        ROOT / "docs/scenarios/index.md",
        ROOT / "docs/notebooks/index.md",
        *(ROOT / "scenarios" / scenario / "README.md" for scenario in SCENARIOS),
    ]
    text = "\n".join(path.read_text(encoding="utf-8") for path in paths)
    assert "tpch_bi_query" in text
    assert "nyc_taxi_trino_daily" in text
    assert "airflow-dags/trino_bi/dag.py" in text
    assert "metadata-DB XCom" in text
    assert "not an Iceberg table" in text
    assert "approved new production DAG" not in text
    assert "#268" not in text


def test_tpch_docs_publish_exact_five_key_fail_closed_contract():
    text = (ROOT / "scenarios/bi_query-tpch-trino-iceberg/README.md").read_text(encoding="utf-8")
    for key in (
        "data_eng_lab.dataset",
        "data_eng_lab.dataset.scale",
        "data_eng_lab.dataset.plan_id",
        "data_eng_lab.dataset.publication_id",
        "data_eng_lab.dataset.manifest_sha256",
    ):
        assert key in text
    assert '"dim_customer$properties"' in text
    assert '"fct_orders$properties"' in text
    assert "fail closed before BI SQL" in text


def test_nyc_docs_are_explicitly_snapshot_bound_not_provenance_bound():
    text = (ROOT / "scenarios/federated_query-nyc_taxi-trino-iceberg/README.md").read_text(
        encoding="utf-8"
    )
    assert "snapshot-bound" in text
    assert "not resolver-generation-bound" in text
    assert "does not claim five-key provenance" in text


def test_both_notebook_languages_warn_that_direct_ctas_is_not_production():
    for scenario in SCENARIOS:
        root = ROOT / "scenarios" / scenario
        for relative in ("jupyter/notebook.ipynb", "zeppelin/notebook.zpln"):
            text = _notebook_text(root / relative)
            assert "educational direct-write path" in text
            assert "does not enforce production provenance, snapshot checks, or serialization" in text
            assert "Use the Airflow DAG for production BI queries and durable BI artifacts" in text
