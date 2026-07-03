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

### `federated_query-nyc_taxi-trino-iceberg`
A federated query scenario using Trino to query Iceberg tables in the lakehouse. Contains:
- Trino %jdbc notebook section
- Trino client query (Python)
- Airflow DAG (`dag.py`)

Both interfaces execute equivalent federated queries against `lakehouse.bronze.nyc_taxi_trips` and `lakehouse.gold.nyc_taxi_daily_trino`. Execution and parity validation are live-gated on the enhanced-Atlas stack (A7, Atlas issue #268).

### `streaming_ingest-events-spark-iceberg`
A streaming ingestion pipeline for events data via Redpanda into Iceberg. Contains:
- Scala Spark Structured Streaming notebook (`zeppelin/notebook.zpln`)
- PySpark Structured Streaming notebook (`jupyter/notebook.ipynb`)
- Redpanda producer
- Airflow DAG (`dag.py`)

Both notebooks implement Structured Streaming with matching logic, consuming from topic `events` and writing to `lakehouse.bronze.events`. Execution and cross-language parity validation are live-gated on the enhanced-Atlas stack (A9, Atlas issue #269).

## Status
The scenario framework (scaffolder + verifier + parity harness) is in place. Curated scenario
notebooks are authored now; their execution and cross-language parity validation are live-gated
on the enhanced-Atlas lakehouse (A1–A4, A7 #268, A9 #269).
