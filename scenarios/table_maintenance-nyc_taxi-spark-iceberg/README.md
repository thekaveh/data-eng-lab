# table_maintenance-nyc_taxi-spark-iceberg

Demonstrate Iceberg table maintenance operations on NYC-taxi data: compact data files via
`rewrite_data_files`, expire old snapshots with `expire_snapshots`, and clean orphaned files via
`remove_orphan_files`.
Scala (Zeppelin) and PySpark (Jupyter) notebooks implement the same logic; a Phase-3a JAR
productionizes it for Airflow.

## 1. Scenario summary
Table-maintenance demo: builds a scenario-owned copy `lakehouse.silver.nyc_taxi_tm` (seeded from
the shared bronze table, then augmented with a second snapshot), measures the initial data-file
count, compacts files to a target size, expires all snapshots older than `current_timestamp()`
(retaining the last 1), removes orphaned files, and verifies the final snapshot and file counts.
Does NOT mutate the shared `lakehouse.bronze.nyc_taxi_trips`.

## 2. Why this exists
Demonstrates Iceberg's maintenance and optimization capabilities (compaction, snapshot expiry,
orphan cleanup) that are critical for production lakehouse hygiene and cost control. Essential
for understanding table lifecycle management.

## 3. What's in the notebooks
`zeppelin/notebook.zpln` (Scala) and `jupyter/notebook.ipynb` (PySpark), sections Overview->Verify;
a `dag.py`.

## 4. How to run
Open either notebook on the Atlas stack (after running `batch_ingest_nyc_taxi`), or trigger the
`table_maintenance_nyc_taxi` Airflow DAG.

## 5. Data & dependencies
Requires `lakehouse.bronze.nyc_taxi_trips` (populated by `batch_ingest-nyc_taxi-spark-iceberg`);
Spark + Iceberg `lakehouse` catalog (Atlas A1-A4) with system procedures enabled.

## 6. Known issues & caveats
Notebook execution + Scala/PySpark parity are live-gated on Atlas A1-A4. The Iceberg catalog must
support `system.rewrite_data_files`, `system.expire_snapshots`, and `system.remove_orphan_files`.
Run `scripts/register_iceberg.py` (creates `bronze`, `silver`, and `gold`) before executing this
scenario standalone. Run `make datasets` to populate `lakehouse.bronze.nyc_taxi_trips` first.
Note: the scenario creates and operates on `lakehouse.silver.nyc_taxi_tm`; the shared bronze
table is never modified.
