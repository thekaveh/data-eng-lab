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
| #308 Spark master REST `:6066` | REST endpoint enabled + documented upstream — but not usable end-to-end by SparkSubmitHook (see 2026-07-21 findings below) | 6-line caveat comment replaced by an updated caveat: the false-negative still reproduces; `waitAppCompletion` remains the completion signal |
| #309 Spark Connect core monopoly | `SPARK_CONNECT_CORES_MAX=1` default cap | `spark.cores.max: "1"` removed from both spark-apps DAGs |
| #310 spark-connect healthcheck | TCP healthcheck on `:15002` | consumer-side `wait_healthy` gate (Atlas `--detach` now health-gates the whole track) |
| #311 Airflow-3 conn resolution | Documented metadata-DB read (`services/airflow/README.md`) | none required — `probe_airflow.py` docstring now cites the upstream doc |

## 2026-07-21 live verification findings (atlas pin 2d006cae)

Cold-start verification of the consumer-manifest migration surfaced two Atlas-side gaps:

1. **Airflow 3.3.0 Execution API unreachable across the container split (severe — blocks all DAG tasks).**
   `LocalExecutor` resolves the Execution API to `http://localhost:8080/execution/` when
   `[core] execution_api_server_url` is unset, but Atlas runs `airflow api-server` in the
   separate `airflow-webserver` container. Every task dies at Pre-Execute (supervisor
   `ConnectionError` → SIGKILL). Verified in-container: `curl http://airflow-webserver:8080/execution/`
   → 404 (reachable); `http://localhost:8080/execution/` → 000. Fix belongs in Atlas's
   airflow compose: set `AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-webserver:8080/execution/`
   on `airflow-scheduler` and `airflow-dag-processor`.

2. **atlas#308 is not consumable by SparkSubmitHook (structural).** The REST endpoint works
   (`curl http://spark-master:6066/v1/submissions/status/<driver-id>` → `success: true`), but the
   hook polls via the `spark_default` connection's port: 7077 is required for cluster-mode
   submission (RPC), while status polling needs 6066 (REST) — one connection cannot satisfy
   both, so the legacy `spark-submit --status` path runs against 7077 and always fails.
   The lab keeps its DAG caveat and relies on `spark.standalone.submit.waitAppCompletion=true`
   as the completion signal. Resolution requires either provider support for a separate
   status URL, a documented `deploy_mode=client` recommendation, or an Atlas-seeded second
   connection scheme.
