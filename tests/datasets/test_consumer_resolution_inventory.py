"""Exact executable-surface inventory for immutable dataset resolution."""

from __future__ import annotations

import json
import re
from pathlib import Path

import nbformat
import pytest

ROOT = Path(__file__).resolve().parents[2]
LEGACY_LANDING_PATTERN = re.compile(r"s3a?://landing/(?:\{dataset\}|[a-z0-9_.-]+)(?:/(?!_generations/)|(?=[\"' )]))")
SCALES = ("tiny", "small", "medium")
RESOLVED_NOTEBOOKS = {
    "batch_ingest-nyc_taxi-spark-iceberg": "nyc_taxi",
    "feature_engineering-movielens-spark-iceberg": "movielens",
    "join_optimization-tpch-spark-iceberg": "tpch",
    "json_flatten-gh_archive-spark-iceberg": "gh_archive",
    "sessionization-gh_archive-spark-iceberg": "gh_archive",
    "star_schema-tpch-spark-iceberg": "tpch",
    "streaming_ingest-gh_archive-spark-iceberg": "gh_archive",
}


def _runtime_sources() -> tuple[Path, ...]:
    paths = [
        ROOT / "scripts" / "bronze_smoke.py",
        ROOT / "scripts" / "new_scenario.py",
        *sorted((ROOT / "spark-apps").rglob("dag.py")),
        *sorted((ROOT / "spark-apps").rglob("src/main/scala/**/*.scala")),
        *sorted((ROOT / "scenarios").rglob("jupyter/notebook.ipynb")),
        *sorted((ROOT / "scenarios").rglob("zeppelin/notebook.zpln")),
    ]
    return tuple(path for path in paths if path.is_file())


def scan_runtime_sources(pattern: re.Pattern[str]) -> list[dict[str, object]]:
    """Return a stable, machine-readable inventory grouped by runtime path."""
    offenders: list[dict[str, object]] = []
    for path in _runtime_sources():
        matches = tuple(dict.fromkeys(pattern.findall(path.read_text(encoding="utf-8"))))
        if matches:
            offenders.append(
                {
                    "path": path.relative_to(ROOT).as_posix(),
                    "runtime": path.suffix.removeprefix("."),
                    "legacy_uris": matches,
                }
            )
    return offenders


def test_runtime_consumers_do_not_construct_flat_landing_paths():
    assert scan_runtime_sources(LEGACY_LANDING_PATTERN) == []


@pytest.mark.parametrize("scenario,dataset", sorted(RESOLVED_NOTEBOOKS.items()))
def test_each_migrated_notebook_bootstraps_one_frozen_expected_scale_result(scenario, dataset):
    root = ROOT / "scenarios" / scenario
    jupyter = nbformat.read(root / "jupyter" / "notebook.ipynb", as_version=4)
    jupyter_code = "\n".join(cell.source for cell in jupyter.cells if cell.cell_type == "code")
    zeppelin = json.loads((root / "zeppelin" / "notebook.zpln").read_text(encoding="utf-8"))
    zeppelin_code = "\n".join(paragraph["text"] for paragraph in zeppelin["paragraphs"])
    for code in (jupyter_code, zeppelin_code):
        assert code.count("/v1/resolve") == 1
        assert "DATASET_RESOLVER_URI" in code
        assert "DATASET_SCALE" in code
        assert dataset in code
        assert "expected_scale" in code
        assert "_generations/" in code
        assert "size_bytes" in code and "sha256" in code and "schema_id" in code


def test_migrated_notebooks_preserve_resolver_object_order_without_globs():
    for scenario in RESOLVED_NOTEBOOKS:
        root = ROOT / "scenarios" / scenario
        text = (root / "jupyter" / "notebook.ipynb").read_text(encoding="utf-8")
        text += (root / "zeppelin" / "notebook.zpln").read_text(encoding="utf-8")
        assert "objects" in text and "object_name" in text and "uri" in text
        assert "sorted(" not in text and ".sorted" not in text
        assert "/*" not in text and "*.parquet" not in text and "*.json" not in text


def test_dags_and_smoke_defer_exact_resolution_to_run_time():
    for path in (
        ROOT / "spark-apps" / "nyc-taxi-etl" / "dag.py",
        ROOT / "spark-apps" / "nyc-taxi-medallion" / "dag.py",
        ROOT / "scripts" / "bronze_smoke.py",
    ):
        text = path.read_text(encoding="utf-8")
        assert text.count("/v1/resolve") == 1
        assert "DATASET_RESOLVER_URI" in text
        assert "DATASET_SCALE" in text
        assert "expected_scale" in text
        assert "_generations/" in text
        assert "size_bytes" in text and "sha256" in text and "schema_id" in text


def test_scala_entrypoints_require_explicit_verified_immutable_uri_arguments():
    for path in (
        ROOT / "spark-apps/nyc-taxi-etl/src/main/scala/com/thekaveh/dataeng/nyctaxi/NycTaxiEtl.scala",
        ROOT / "spark-apps/nyc-taxi-medallion/src/main/scala/com/thekaveh/dataeng/medallion/NycTaxiMedallion.scala",
    ):
        text = path.read_text(encoding="utf-8")
        assert "_generations/" in text
        assert "verified immutable NYC Taxi URI arguments are required" in text
        assert "args.nonEmpty" in text
