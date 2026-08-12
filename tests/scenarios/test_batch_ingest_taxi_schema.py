"""Regression contract for the NYC Taxi notebooks' mixed-Parquet input schema."""

from __future__ import annotations

import json
from pathlib import Path

import nbformat

ROOT = Path(__file__).resolve().parents[2]
SCENARIO = ROOT / "scenarios" / "batch_ingest-nyc_taxi-spark-iceberg"


def _zeppelin_read_code() -> str:
    note = json.loads((SCENARIO / "zeppelin" / "notebook.zpln").read_text())
    return next(paragraph["text"] for paragraph in note["paragraphs"] if paragraph["title"] == "3. Read (code)")


def _jupyter_read_code() -> str:
    note = nbformat.read(SCENARIO / "jupyter" / "notebook.ipynb", as_version=4)
    return next(cell.source for cell in note.cells if cell.cell_type == "code" and "taxi_paths" in cell.source)


def test_batch_ingest_notebooks_resolve_expected_scale_and_normalize_before_union():
    """Each notebook resolves one frozen run scale and normalizes every returned object."""
    zeppelin, jupyter = _zeppelin_read_code(), _jupyter_read_code()
    for code in (zeppelin, jupyter):
        assert "s3a://landing/nyc_taxi" not in code
        assert "passenger_count" in code
        assert 'cast("double")' in code or "cast('double')" in code
        assert "unionByName" in code
        assert "taxi_paths" in code or "taxiPaths" in code
    full_notebooks = (
        (SCENARIO / "zeppelin/notebook.zpln").read_text(encoding="utf-8"),
        (SCENARIO / "jupyter/notebook.ipynb").read_text(encoding="utf-8"),
    )
    for notebook in full_notebooks:
        assert "DATASET_SCALE" in notebook
        assert "expected_scale" in notebook
        assert "nyc_taxi" in notebook


def test_batch_ingest_notebooks_preserve_bronze_output_contract():
    """The normalized input still feeds the same filtering and Iceberg write."""
    zeppelin = (SCENARIO / "zeppelin" / "notebook.zpln").read_text()
    jupyter = (SCENARIO / "jupyter" / "notebook.ipynb").read_text()
    for notebook in (zeppelin, jupyter):
        assert "lakehouse.bronze.nyc_taxi_trips" in notebook
        assert "tpep_pickup_datetime" in notebook
        assert "passenger_count" in notebook
        assert "trip_date" in notebook
