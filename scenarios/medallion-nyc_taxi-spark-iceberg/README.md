# medallion-nyc_taxi-spark-iceberg

Promote NYC-taxi data through the medallion architecture: `lakehouse.bronze.nyc_taxi_trips` →
`lakehouse.silver.nyc_taxi_trips` (deduplicated) → `lakehouse.gold.nyc_taxi_daily` (daily aggregate).
Scala (Zeppelin) and PySpark (Jupyter) notebooks implement the same logic; a Phase-3a JAR
productionizes it for Airflow.

## 1. Scenario summary
Medallion transform: bronze → silver (dedupe on pickup/dropoff datetime) → gold (daily trip count +
avg fare). Reads from Iceberg `bronze`, writes `silver` and `gold` Iceberg tables.
## 2. Why this exists
Demonstrates the full lakehouse medallion pattern on top of the batch-ingested NYC-taxi dataset.
## 3. What's in the notebooks
`zeppelin/notebook.zpln` (Scala) and `jupyter/notebook.ipynb` (PySpark), sections Overview->Verify; a `dag.py`.
## 4. How to run
Open either notebook on the Atlas stack (after running `batch_ingest_nyc_taxi`), or trigger the
`medallion_nyc_taxi` Airflow DAG.
## 5. Data & dependencies
Requires `lakehouse.bronze.nyc_taxi_trips` (populated by `batch_ingest-nyc_taxi-spark-iceberg`);
Spark + Iceberg `lakehouse` catalog (Atlas A1-A4).
## 6. Known issues & caveats
Notebook execution + Scala/PySpark parity are live-gated on Atlas A1-A4. The `trip_date` column
must exist on the bronze table (added during ingest).
