# 5.8. data_quality-nyc_taxi-spark-iceberg

Runs the snapshot-bound NYC Taxi quality gate as an **existing production DAG**. The reviewed Spark application partitions every Bronze row into clean or quarantine, persists eight governed Gold facts, and exposes three fixed Trino dashboard queries.

## 1. Purpose

The production entrypoint is `spark-apps/nyc-taxi-data-quality/dag.py`, DAG `nyc_taxi_data_quality`. It evaluates one stable `lakehouse.bronze.nyc_taxi_trips` snapshot after the matching successful `nyc_taxi_etl` logical date. This is snapshot-bound lineage, not upstream five-key resolver-generation provenance; adding those five properties to the Bronze producer remains deferred hardening.

## 2. Data Model

| Table | Role |
|---|---|
| `lakehouse.bronze.nyc_taxi_trips` | Stable 20-column source snapshot |
| `lakehouse.silver.nyc_taxi_clean` | Rows whose fare, passenger-count, and distance operands are valid |
| `lakehouse.silver.nyc_taxi_quarantine` | Null-safe complement of clean, including null/NaN/infinite rule operands |
| `lakehouse.gold.nyc_taxi_quality_facts` | Eight versioned facts keyed by deterministic run ID and rule ID |

Duplicates and rows with null rule operands are preserved. The application verifies exact schema, null-safe multiset conservation, readback counts, and source snapshot stability. Silver replacement and Gold MERGE are not cross-table atomic; a same-date rerun is the supported recovery and converges without duplicate facts.

## 3. Governed Rules

The fixed rule version records owners, severity, thresholds, numerator/denominator, canonical decimal metrics, and status:

- `bronze.source_available.v1`
- `bronze.schema.v1`
- `bronze.snapshot_freshness.v1`
- `bronze.invalid_ratio.v1`
- `silver.partition_conservation.v1`
- `silver.clean_nonempty.v1`
- `silver.quarantine_ratio.v1`
- `silver.output_readback.v1`

Invalid and quarantine ratios pass through 1%, warn above 1% through 5%, and fail above 5%. Missing and stale outrank fail, then warn, then pass. A task succeeds only after the exact accepted fact set is read back.

## 4. Orchestration

The `@daily` DAG uses `max_active_runs=1`. `wait_for_matching_nyc_taxi_etl` is a bounded rescheduling `ExternalTaskSensor` for `nyc_taxi_etl.submit_nyc_taxi_etl` at the same logical date; `submit_nyc_taxi_data_quality` submits the Jenkins-published JAR through Atlas's REST-confirming Spark operator. Concurrent direct JAR execution is unsupported.

The final artifact passed matching-ETL execution, a same-date Airflow replacement/retry, distinct terminal Spark-driver confirmation, exact fact idempotence, fixed-dashboard validation, unchanged source pointer, and volume-preserving cleanup. The tracked evidence record is `2026-08-13-nyc-taxi-data-quality-live-acceptance.md` in the repository's internal report directory.

## 5. Dashboard and Operations

The durable dashboard source is the Gold facts table. Operators retrieve bounded, deterministically ordered results with:

- `spark-apps/nyc-taxi-data-quality/queries/latest.sql`
- `spark-apps/nyc-taxi-data-quality/queries/trend.sql`
- `spark-apps/nyc-taxi-data-quality/queries/operator_attention.sql`

These are fixed SELECT-only queries; thresholds and arbitrary SQL are not DagRun inputs. For recovery, correct the primary failure and rerun the same logical date. Inspect the exact accepted eight-fact set before treating the run as complete.

## 6. Educational Notebooks

The paired Zeppelin and Jupyter notebooks read with `spark.table("lakehouse.bronze.nyc_taxi_trips")`, preserve the original rule `fare_amount > 0 AND passenger_count BETWEEN 1 AND 6`, perform the null-safe two-table split with `createOrReplace`, and assert row conservation. They directly replace the production Silver tables without production provenance, bypass Airflow serialization, and do not persist the governed fact set. Run them only in an isolated learning environment; production writes must use the DAG/application.

## 7. Usage

```bash
mvn -q -B -f spark-apps/nyc-taxi-data-quality/pom.xml test
mvn -q -B -f spark-apps/nyc-taxi-data-quality/pom.xml package
```

Airflow schedules the production path daily. A manual run must share a logical date with a successful `nyc_taxi_etl` run so the sensor contract is exercised rather than bypassed.

## 8. See Also

- [Spark application](../spark-apps/nyc-taxi-data-quality.md)
- [Execution-mode matrix](execution-modes.md)
- [Lakehouse Architecture](../lakehouse.md)
- [NYC Taxi dataset](../datasets.md)
