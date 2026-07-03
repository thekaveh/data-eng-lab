# streaming_ingest-gh_archive-spark-iceberg

Demonstrate Iceberg ingestion via Structured Streaming with a **file source**: read from landing
JSON files, parse with schema, cast timestamp column, and write to an Iceberg table with
checkpoints for exactly-once semantics. No Kafka or external queues required.
Scala (Zeppelin) and PySpark (Jupyter) notebooks implement the same logic.

## 1. Scenario summary
File-source Structured Streaming: define schema, read JSON stream from `s3a://landing/gh_archive`,
cast `created_at` to timestamp, write to Iceberg table `lakehouse.bronze.gh_events_stream` with
checkpoint location `s3a://checkpoints/gh_events_file`.

## 2. Why this exists
Demonstrates Structured Streaming to Iceberg using a simple file source (no Kafka/external
dependency), enabling exactly-once ingestion semantics and checkpointing for fault tolerance.

## 3. What's in the notebooks
`zeppelin/notebook.zpln` (Scala) and `jupyter/notebook.ipynb` (PySpark), sections Overview->Verify; a `dag.py`.

## 4. How to run
Open either notebook on the Atlas stack (after running `make datasets` to populate landing files),
or trigger the `streaming_ingest_gh_archive` Airflow DAG.

## 5. Data & dependencies
File source: `s3a://landing/gh_archive/` (populated by `make datasets`); Spark + Iceberg
`lakehouse` catalog (Atlas A1-A4); checkpoints stored in `s3a://checkpoints/gh_events_file`.
Requires `lakehouse.bronze` namespace to exist.

## 6. Known issues & caveats
Notebook execution + Scala/PySpark parity are live-gated on Atlas A1-A4. Run `scripts/register_iceberg.py`
(creates `bronze`, `silver`, and `gold` namespaces) and `make datasets` before executing this scenario
standalone. This scenario uses a file source, not Kafka, so it does not require Atlas A9.
