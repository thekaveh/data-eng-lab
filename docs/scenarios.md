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

### Iceberg-lever & transform scenarios (Spark; delivered-Atlas only)
All scenarios in this group require only the delivered Atlas (Spark 4.1.2 + Iceberg 1.11.0), `scripts/register_iceberg.py` for namespace registration, and `make datasets` for landing data.

#### `time_travel-nyc_taxi-spark-iceberg`
Snapshots, `VERSION AS OF`, rollback, branch/tag (Write-Audit-Publish) on nyc_taxi.

#### `table_maintenance-nyc_taxi-spark-iceberg`
Compaction, `expire_snapshots`, `remove_orphan_files` on nyc_taxi.

#### `star_schema-tpch-spark-iceberg`
Fact/dimension gold marts and dimensional modeling on tpch.

#### `json_flatten-gh_archive-spark-iceberg`
Nested JSON transformation into typed columns on gh_archive.

#### `schema_evolution-gh_archive-spark-iceberg`
Add/rename columns and read old+new data together on gh_archive.

#### `streaming_ingest-gh_archive-spark-iceberg`
File-source Structured Streaming to Iceberg with checkpoints on gh_archive.

### Transactional (upsert / SCD2) scenarios (Spark; delivered-Atlas only)
Advanced transactional patterns using `MERGE INTO` and Slowly Changing Dimension Type 2, requiring only the delivered Atlas (Spark 4.1.2 + Iceberg 1.11.0), `scripts/register_iceberg.py` for namespace registration, and small inline seed data (online_retail dataset can be substituted at scale).

#### `incremental_upsert-online_retail-spark-iceberg`
CDC/upsert pattern with `MERGE INTO` for idempotent inserts and updates on online_retail.

#### `scd2-online_retail-spark-iceberg`
Slowly Changing Dimension Type 2 (effective_from/to, is_current) on online_retail.

**Note:** This completes the design's **core-10** (scenarios #1–#10).

## Status
The scenario framework (scaffolder + verifier + parity harness) is in place. Curated scenario
notebooks are authored now; their execution and cross-language parity validation are live-gated
on the enhanced-Atlas lakehouse (A1–A4, A7 #268, A9 #269).
