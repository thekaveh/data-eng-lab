# Scenarios

Each scenario lives in a flat folder `scenarios/<pattern>-<dataset>-<engine>-<format>/` and is
**self-contained**: a 6-section `README.md`, a Scala Spark `zeppelin/notebook.zpln`, a PySpark
`jupyter/notebook.ipynb`, and an optional Airflow `dag.py`. Structure is enforced by the repo verifier.

## Add a scenario
```bash
make new-scenario NAME=medallion-nyc_taxi-spark-iceberg
uv run python scripts/verify_repo.py --root .   # validates structure
```
Then fill the notebook sections (`1. Overview` … `6. Verify`) with the scenario's Scala/PySpark logic.
The **Scala and PySpark notebooks must produce equivalent output** — Phase 2b captures a snapshot of
each and compares them with `tables_equivalent` in `tests/scenarios/parity.py`.

## Authored scenarios

### `batch_ingest-nyc_taxi-spark-iceberg`
A batch ingestion pipeline for NYC taxi trip data into Iceberg. Contains:
- Scala Spark notebook (`zeppelin/notebook.zpln`)
- PySpark notebook (`jupyter/notebook.ipynb`)
- Airflow DAG (`dag.py`)

Both notebooks load raw NYC-TLC Parquet data and write Bronze-layer facts to `lakehouse.bronze.nyc_taxi_trips`. Execution and parity validation are live-gated on the enhanced-Atlas stack (A1–A4).

### `medallion-nyc_taxi-spark-iceberg`
A medallion lakehouse (Bronze → Silver → Gold) transformation of NYC taxi data. Contains:
- Scala Spark notebook (`zeppelin/notebook.zpln`)
- PySpark notebook (`jupyter/notebook.ipynb`)
- Airflow DAG (`dag.py`)

Both notebooks implement multi-layer transformations with matching logic. Execution and parity validation are live-gated on the enhanced-Atlas stack (A1–A4).

## Status
The scenario framework (scaffolder + verifier + parity harness) is in place; the curated scenario
notebooks are authored in Phase 2b (once the Atlas lakehouse — A1–A4 — is live).
