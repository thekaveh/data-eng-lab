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
