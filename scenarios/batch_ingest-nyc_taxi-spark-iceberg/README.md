# batch_ingest-nyc_taxi-spark-iceberg

Land raw NYC-taxi Parquet from `s3a://landing/nyc_taxi/` into the Iceberg `lakehouse.bronze.nyc_taxi_trips`
table, cleaned (drop null pickups + non-positive passenger counts, add `trip_date`). Scala (Zeppelin) and
PySpark (Jupyter) notebooks implement the same logic; a Phase-3a JAR productionizes it for Airflow.

## 1. Scenario summary
Batch ingestion: `landing` Parquet -> Iceberg `bronze`, with basic cleaning.
## 2. Why this exists
The simplest end-to-end lakehouse write; the entry point of the medallion.
## 3. What's in the notebooks
`zeppelin/notebook.zpln` (Scala) and `jupyter/notebook.ipynb` (PySpark), sections Overview->Verify; a `dag.py`.
## 4. How to run
Open either notebook on the Atlas stack, or trigger the `batch_ingest_nyc_taxi` Airflow DAG.
## 5. Data & dependencies
`nyc_taxi` in `landing` (`make datasets`); Spark + Iceberg `lakehouse` catalog (Atlas A1-A4).
## 6. Known issues & caveats
Notebook execution + Scala/PySpark parity are live-gated on Atlas A1-A4.
