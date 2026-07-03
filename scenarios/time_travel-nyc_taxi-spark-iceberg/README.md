# time_travel-nyc_taxi-spark-iceberg

Demonstrate Iceberg time-travel capabilities on NYC-taxi data: create snapshots via inserts, query
historical versions with `VERSION AS OF`, explore branching (write-audit-publish), and rollback to
earlier snapshots.
Scala (Zeppelin) and PySpark (Jupyter) notebooks implement the same logic; a Phase-3a JAR
productionizes it for Airflow.

## 1. Scenario summary
Time-travel demo: create an Iceberg table snapshot, insert additional data (creating a second
snapshot), explore historical versions, create a WAP branch for safe mutation, and demonstrate
rollback mechanisms. Uses `lakehouse.silver.nyc_taxi_tt` as a time-travel sandbox.

## 2. Why this exists
Demonstrates Iceberg's core time-travel and versioning features (snapshots, VERSION AS OF, branches,
rollback) on a familiar dataset. Essential for understanding data governance and audit trails.

## 3. What's in the notebooks
`zeppelin/notebook.zpln` (Scala) and `jupyter/notebook.ipynb` (PySpark), sections Overview->Verify;
a `dag.py`.

## 4. How to run
Open either notebook on the Atlas stack (after running `batch_ingest_nyc_taxi`), or trigger the
`time_travel_nyc_taxi` Airflow DAG.

## 5. Data & dependencies
Requires `lakehouse.bronze.nyc_taxi_trips` (populated by `batch_ingest-nyc_taxi-spark-iceberg`);
Spark + Iceberg `lakehouse` catalog (Atlas A1-A4).

## 6. Known issues & caveats
Notebook execution + Scala/PySpark parity are live-gated on Atlas A1-A4. The `silver` namespace must
exist in the Iceberg REST catalog.
Run `scripts/register_iceberg.py` (creates `bronze`, `silver`, and `gold`) before executing this
scenario standalone. Note: `lakehouse.silver.nyc_taxi_tt` is dropped at the end of the Verify
section (or manually, e.g., `DROP TABLE IF EXISTS lakehouse.silver.nyc_taxi_tt`).
The `VERSION AS OF` and `rollback_to_snapshot` lines in the Verify cell are commented examples;
replace `<snapshot_id>` with a concrete id from the history query above before un-commenting.
