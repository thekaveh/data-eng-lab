# federated_query-nyc_taxi-trino-iceberg

Query the NYC-taxi Iceberg lakehouse via Trino SQL — `lakehouse.bronze.nyc_taxi_trips` →
`lakehouse.gold.nyc_taxi_daily_trino` — from both a Zeppelin `%trino` notebook and a
Jupyter notebook using the `trino` Python client. Both surfaces run identical SQL.
Live-gated on Atlas #268.

## 1. Scenario summary
Federated query: Trino reads Iceberg tables directly via the `lakehouse` catalog, aggregates
NYC-taxi trips into a daily summary (`nyc_taxi_daily_trino`), and writes the result back to
the gold layer — all via standard ANSI SQL (no Spark required).

## 2. Why this exists
Demonstrates the Trino-as-query-engine path over the same Iceberg lakehouse used by the
medallion scenario, providing a lightweight SQL-only alternative to PySpark/Scala workloads.

## 3. What's in the notebooks
`zeppelin/notebook.zpln` (Trino SQL via `%trino`) and `jupyter/notebook.ipynb`
(same SQL via the `trino` Python client), both with sections Overview→Verify; a `dag.py`
EmptyOperator placeholder.

## 4. How to run
Open either notebook on the Atlas stack (after running `batch_ingest-nyc_taxi-spark-iceberg`).
The Trino coordinator must be reachable at `trino:8080`. Trigger the
`federated_query_nyc_taxi` Airflow DAG once a TrinoOperator integration is added (Atlas #268).

## 5. Data & dependencies
Requires `lakehouse.bronze.nyc_taxi_trips` (populated by `batch_ingest-nyc_taxi-spark-iceberg`);
Trino coordinator + Iceberg REST catalog (Atlas A5-A7, issue #268).
The `lakehouse.gold` namespace must be created by running `scripts/register_iceberg.py` before the Write cell executes.

## 6. Known issues & caveats
Live execution is gated on Atlas #268 (Trino coordinator integration). The `%trino`
interpreter is seeded by Atlas pointing to the Atlas Trino coordinator.
`lakehouse.gold` namespace must exist in the Iceberg REST catalog before the Write cell runs.
