# data_quality-nyc_taxi-spark-iceberg

Validate NYC-taxi data by splitting records into clean and quarantine tables based on business rules.
Scala (Zeppelin) and PySpark (Jupyter) notebooks implement the same validation logic.

## 1. Scenario summary
Data quality validation: apply rules (fare_amount > 0 AND passenger_count BETWEEN 1 AND 6) to split
records into clean and quarantine tables. Records with NULL values are quarantined.

## 2. Why this exists
Demonstrates data quality checks and quarantine table patterns for data validation on the medallion
lakehouse. Ensures downstream tables only contain valid records.

## 3. What's in the notebooks
`zeppelin/notebook.zpln` (Scala) and `jupyter/notebook.ipynb` (PySpark), sections Overview->Verify;
a `dag.py` for Airflow integration.

## 4. How to run
Open either notebook on the Atlas stack (after running `batch_ingest_nyc_taxi`), or trigger the
`data_quality_nyc_taxi` Airflow DAG.

## 5. Data & dependencies
Requires `lakehouse.bronze.nyc_taxi_trips` (populated by `batch_ingest-nyc_taxi-spark-iceberg`);
Spark + Iceberg `lakehouse` catalog (Atlas A1-A4).

## 6. Known issues & caveats
Notebook execution + Scala/PySpark parity are live-gated on Atlas A1-A4. This scenario writes to
`lakehouse.silver.nyc_taxi_clean` and `lakehouse.silver.nyc_taxi_quarantine`, which require the
`silver` namespace to exist in the Iceberg REST catalog. Run `scripts/register_iceberg.py` (creates
`bronze`, `silver`, and `gold`) before executing this scenario standalone.
