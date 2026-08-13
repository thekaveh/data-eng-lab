# 5.17. federated_query-nyc_taxi-trino-iceberg

Query the NYC-taxi Iceberg lakehouse through a snapshot-bound, read-only production Trino task, with paired Zeppelin and Jupyter notebooks retained as educational direct-query/CTAS surfaces.

## 1. Purpose

This scenario demonstrates Trino over the same Iceberg lakehouse used by Spark. Production binds to one unchanged NYC Bronze snapshot, reconciles daily counts to an independent source count, and returns a bounded canonical artifact without Spark or an Iceberg write. The paired notebooks retain direct CTAS only as an educational comparison.

## 2. Data Model

### 2.1 Input Source

Source: `lakehouse.bronze.nyc_taxi_trips` (populated by `batch_ingest-nyc_taxi-spark-iceberg`).

### 2.2 Output Artifact

| Table | Layer | Key Columns |
|---|---|---|
| Airflow metadata-DB XCom | Run artifact | `trip_date`, `trip_count`, `avg_fare` (aggregated daily result) |

## 3. Architecture

![Architecture](../diagrams/img/federated_query-nyc_taxi-trino-iceberg.png)

Data flows from the Bronze Iceberg table through fixed read-only Trino SQL into a bounded canonical XCom. The task validates source schema and count, records the current Iceberg snapshot, runs the daily aggregate, and accepts the result only after the snapshot remains unchanged. No Spark cluster or Iceberg write is involved.

## 4. Notebooks

- **Zeppelin (Scala, `%trino`):** Sections: Overview → Read Bronze Table, Aggregate by Day, Write Gold Summary → Verify; identical SQL to PySpark
- **Jupyter (Py, `trino`):** Same sections; identical SQL via the Trino Python client

Their CTAS cells are an **educational direct-write path** that **does not enforce production provenance, snapshot checks, or serialization**. **Use the Airflow DAG for production BI queries and durable BI artifacts**; the production path itself is read-only and stores the artifact in Airflow metadata.

## 5. Orchestration

Classification: **existing production DAG** at `airflow-dags/trino_bi/dag.py`. `nyc_taxi_trino_daily` runs daily at 02:00 UTC with `max_active_runs=1`, one retry, and a two-minute delay. Its bounded canonical metadata-DB XCom is retained with the Airflow metadata database and is **not an Iceberg table**; retrieve it from the task instance XCom view or API before configured metadata retention removes the DagRun.

The NYC source is **snapshot-bound** and **not resolver-generation-bound**. The task requires one unchanged positive Iceberg snapshot around its read and aggregate, but the upstream Bronze table does not expose the producer contract's five properties. Production therefore **does not claim five-key provenance** for NYC.

## 6. Usage

1. Populate bronze table: `batch_ingest-nyc_taxi-spark-iceberg` (or ensure it exists)
2. Ensure the `gold` Iceberg namespace exists: `scripts/register_iceberg.py`
3. For production, run `airflow dags test nyc_taxi_trino_daily <logical-date> --use-executor` from the scheduler or let the daily schedule run.
4. Retrieve `run_bounded_bi_query`'s `return_value` XCom through Airflow's task-instance view/API.
5. Use either paired notebook only for the educational direct-query/CTAS walkthrough.

## 7. Dependencies

- **Dataset:** NYC Taxi trips via `lakehouse.bronze.nyc_taxi_trips`
- **Atlas services:** A5-A7 (Trino, Trino coordinator, Iceberg REST catalog)
- **Other:** None

## 8. Known Issues & Caveats

Atlas provides the Trino coordinator and the `%trino` interpreter. Production accepts only fixed registry SQL through internal `http://trino:8080`; no host fallback or arbitrary DagRun SQL exists. The catalog is currently unauthenticated/ALLOW_ALL, so the application-level read-only registry is a workload boundary, not a claim of catalog security. The NYC source table has no production five-key provenance; use the captured snapshot ID when interpreting each result.

## 9. See Also

- [Upstream: batch_ingest-nyc_taxi-spark-iceberg](./batch_ingest-nyc_taxi-spark-iceberg.md) — Populates the bronze table
- [Datasets](../datasets.md)
- [Lakehouse Architecture](../lakehouse.md)
