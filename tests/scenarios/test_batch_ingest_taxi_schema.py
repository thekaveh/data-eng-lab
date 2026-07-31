"""Regression contract for the NYC Taxi notebooks' mixed-Parquet input schema."""
from __future__ import annotations

import json
from pathlib import Path

import nbformat
import yaml

ROOT = Path(__file__).resolve().parents[2]
SCENARIO = ROOT / "scenarios" / "batch_ingest-nyc_taxi-spark-iceberg"


def _taxi_paths_for_scale(scale_name: str) -> list[str]:
    registry = yaml.safe_load((ROOT / "datasets" / "registry.yaml").read_text())
    urls = registry["datasets"]["nyc_taxi"]["scales"][scale_name]["urls"]
    return [url.replace("https://d37ci6vzurychx.cloudfront.net/trip-data/", "s3a://landing/nyc_taxi/")
            for url in urls]


def _zeppelin_read_code() -> str:
    note = json.loads((SCENARIO / "zeppelin" / "notebook.zpln").read_text())
    return next(paragraph["text"] for paragraph in note["paragraphs"]
                if paragraph["title"] == "3. Read (code)")


def _jupyter_read_code() -> str:
    note = nbformat.read(SCENARIO / "jupyter" / "notebook.ipynb", as_version=4)
    return next(cell.source for cell in note.cells
                if cell.cell_type == "code" and "taxi_paths" in cell.source)


def test_batch_ingest_notebooks_use_default_scale_paths_and_normalize_before_union():
    """Default notebooks match ``make datasets`` and normalize each selected object."""
    declared = _taxi_paths_for_scale("small")
    zeppelin, jupyter = _zeppelin_read_code(), _jupyter_read_code()
    for code in (zeppelin, jupyter):
        assert 'spark.read.parquet("s3a://landing/nyc_taxi/")' not in code
        assert "passenger_count" in code
        assert "cast(\"double\")" in code or "cast('double')" in code
        assert "unionByName" in code
        assert all(path in code for path in declared)
    assert 'val taxiDatasetScale = "small"' in zeppelin
    assert "taxi_dataset_scale = 'small'" in jupyter
    assert "taxiPathsByScale" in zeppelin
    assert "taxi_paths_by_scale" in jupyter
    assert "val taxiPaths = taxiPathsByScale.getOrElse(" in zeppelin
    assert "taxi_paths = taxi_paths_by_scale[taxi_dataset_scale]" in jupyter


def test_batch_ingest_notebooks_preserve_bronze_output_contract():
    """The normalized input still feeds the same filtering and Iceberg write."""
    zeppelin = (SCENARIO / "zeppelin" / "notebook.zpln").read_text()
    jupyter = (SCENARIO / "jupyter" / "notebook.ipynb").read_text()
    for notebook in (zeppelin, jupyter):
        assert "lakehouse.bronze.nyc_taxi_trips" in notebook
        assert "tpep_pickup_datetime" in notebook
        assert "passenger_count" in notebook
        assert "trip_date" in notebook
