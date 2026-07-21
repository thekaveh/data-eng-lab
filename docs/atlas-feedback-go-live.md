# Atlas Go-Live Feedback

Post-go-live observations from running the full `data-eng-lab` platform in a production-like environment.

## Summary

All A1–A9 capabilities verified during go-live. The platform is fully operational. 19 scenarios executed with parity between Scala and PySpark notebooks where applicable.

## Key Observations

1. **MinIO stability** — MinIO handled the full dataset load (all 5 datasets at medium scale) without issues. Disk usage should be monitored as scenarios are re-run.
2. **Iceberg REST catalog** — The REST catalog responded to concurrent queries from Spark, Trino, and PyIceberg without contention.
3. **Spark Connect** — The shared PySpark session in JupyterHub is stable across notebook executions. Each notebook manages its own session lifecycle.
4. **Trino performance** — Trino performed well on the dataset sizes used. TPC-H at larger scales may need tuning.
5. **Streaming reliability** — Redpanda handled streaming workloads without issues. The `foreachBatch` CDC pattern produced correct results.
6. **Airflow orchestration** — Airflow successfully scheduled Spark jobs via `SparkSubmitOperator`. DAG execution was reliable.
7. **Jenkins CI** — The JAR build and publish pipeline works end-to-end.

## Recommendations

- Add a scheduled cleanup for streaming checkpoint directories.
- Consider implementing dataset versioning for landing data to support reproducible scenario runs.
- Add observability metrics for the Iceberg REST catalog (query counts, latency).
- Consider adding a data quality monitoring dashboard for Bronze/Silver/Gold tables.

## Related

- [Atlas Expectations](atlas-expectations.md) — Full delivery log
- [Go-Live Results](go-live-results.md) — Detailed validation results
- [Atlas Feedback A7/A9](atlas-feedback-a7a9.md) — Streaming and federated query feedback

## Workaround unwind (2026-07-21, atlas pin 2d006cae)

All four issues below were fixed upstream (atlas#313–#316) and the corresponding
lab-side workarounds removed:

| Atlas issue | Upstream fix | Lab workaround removed |
|---|---|---|
| #308 Spark master REST `:6066` | Documented REST status polling; server was already enabled (`services/spark/compose.yml` `SPARK_MASTER_OPTS`) | 6-line SparkSubmitOperator caveat comment in `spark-apps/nyc-taxi-etl/dag.py` (kept `waitAppCompletion`) |
| #309 Spark Connect core monopoly | `SPARK_CONNECT_CORES_MAX=1` default cap | `spark.cores.max: "1"` removed from both spark-apps DAGs |
| #310 spark-connect healthcheck | TCP healthcheck on `:15002` | consumer-side `wait_healthy` gate (Atlas `--detach` now health-gates the whole track) |
| #311 Airflow-3 conn resolution | Documented metadata-DB read (`services/airflow/README.md`) | none required — `probe_airflow.py` docstring now cites the upstream doc |
