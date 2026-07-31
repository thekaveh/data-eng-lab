# 8.5. Atlas Go-Live Findings

Post-go-live observations from running the full `data-eng-lab` platform in a production-like environment.

## Summary

As of the original go-live run (2026-07-04, atlas `85ff46b`): all A1–A9 capabilities verified during go-live. The platform is fully operational. 19 scenarios executed with parity between Scala and PySpark notebooks where applicable.

Status update (2026-07-28, atlas `882877a4`): the then-current reviewed pin included
the upstream #791 in-network Execution API URL repair and [Atlas #850](https://github.com/thekaveh/atlas/issues/850)'s
corrected `AIRFLOW__API_AUTH__JWT_SECRET` mapping for Airflow 3.3's `[api_auth]`
section. The earlier `af7713ee` focused retest remains recorded below as failed
evidence for the ineffective `AIRFLOW__API__JWT_SECRET` mapping. At this point,
Airflow DAG live acceptance and promotion had not been claimed. Non-Airflow
focused checks, including Zeppelin, Trino, and streaming, remained valid. #792
was still open.

Status update (2026-07-30, atlas `0644a8f3`): Atlas [PR #878](https://github.com/thekaveh/atlas/pull/878)
merged the #876 correction to the #792 wrapper. The current pin supplies
`atlas_spark_utils.submit_and_confirm_via_rest()`: the parent DAGs construct `SparkSubmitHook`
without an application, submit through the required `spark_default` RPC endpoint
(`spark://spark-master:7077`), and confirm the hook's driver ID through the master's REST
endpoint (`spark-master:6066`). This bypasses the provider's incompatible post-submit `:7077`
poll without treating a failed Spark driver as success. At this point, the integration awaited
the representative live rerun, so this historical update did not claim Airflow success or promotion.

Status update (2026-07-31, atlas `985918ce8c805081947d53b1c48bb80610237a5b`): Atlas [PR #883](https://github.com/thekaveh/atlas/pull/883)
resolved the follow-up [#880](https://github.com/thekaveh/atlas/issues/880) defect. The provider
sets `hook._driver_id` only on the tracking path, so the prior helper could not obtain it after
correctly disabling the incompatible `:7077` poll. The current helper captures the `spark-submit`
log, extracts the standalone submission ID, and verifies the driver through `spark-master:6066`.
The representative Airflow feature-artifact task succeeded on its first and only attempt. The
standalone Spark REST record reached `FINISHED` with `success=true`, resolving the Airflow
acceptance gate for this pin. The Atlas consumer modernization had already completed Gitflow
promotion through PRs #66, #67, and #68.

## Key Observations

1. **MinIO stability** — MinIO handled the full dataset load (all 5 datasets at medium scale) without issues. Disk usage should be monitored as scenarios are re-run.
2. **Iceberg REST catalog** — The REST catalog responded to concurrent queries from Spark, Trino, and PyIceberg without contention.
3. **Spark Connect** — The shared PySpark session in JupyterHub is stable across notebook executions. Each notebook manages its own session lifecycle.
4. **Trino performance** — Trino performed well on the dataset sizes used. TPC-H at larger scales may need tuning.
5. **Streaming reliability** — Redpanda handled streaming workloads without issues. The `foreachBatch` CDC pattern produced correct results.
6. **Airflow orchestration** — Airflow schedules Spark jobs through Atlas #880's provider-compatible `SparkSubmitHook` plus log-derived driver-ID REST confirmation. The 2026-07-31 representative task completed successfully with terminal Spark confirmation.
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

Three of the four issues below (#309–#311) were fixed upstream (atlas#314–#316) and the
corresponding lab-side workarounds removed; #308's fix is only partial — the REST endpoint
is enabled but not consumable by SparkSubmitHook, so the DAG caveat remains (see the row
below and the 2026-07-21 findings section that follows):

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
   on `airflow-scheduler` and `airflow-dag-processor`. Filed as [thekaveh/atlas#791](https://github.com/thekaveh/atlas/issues/791).

2. **atlas#308 is not consumable by SparkSubmitHook (structural).** The REST endpoint works
   (`curl http://spark-master:6066/v1/submissions/status/<driver-id>` → `success: true`), but the
   hook polls via the `spark_default` connection's port: 7077 is required for cluster-mode
   submission (RPC), while status polling needs 6066 (REST) — one connection cannot satisfy
   both, so the legacy `spark-submit --status` path runs against 7077 and always fails.
   The lab keeps its DAG caveat and relies on `spark.standalone.submit.waitAppCompletion=true`
   as the completion signal. Resolution requires either provider support for a separate
   status URL, a documented `deploy_mode=client` recommendation, an Atlas-seeded second
   connection scheme, or tolerating the post-submit poll exception lab-side given
   `waitAppCompletion` already signals completion. Filed as [thekaveh/atlas#792](https://github.com/thekaveh/atlas/issues/792).

## Reviewed-pin update (atlas `881df596`)

Atlas #791 is resolved in the reviewed pin: the scheduler and DAG processor set
`AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-webserver:8080/execution/`.
That gives Airflow's executor the in-network webserver address instead of a
container-local `localhost` address. At this historical pin, the record did not
claim a fresh lab run: the next gate still had to demonstrate a successful
`nyc_taxi_etl` DAG without the former Pre-Execute connection failure. The
distinct SparkSubmitHook driver-status poll behavior in
[atlas#792](https://github.com/thekaveh/atlas/issues/792) was still open at this
pin; `spark.standalone.submit.waitAppCompletion=true` was then the authoritative
completion signal.

## Task 7 live-gate update (2026-07-28, atlas `881df596`)

The Execution API URL repair is active: the scheduler and DAG processor both
resolve `http://airflow-webserver:8080/execution/`. The representative
`nyc_taxi_etl` run nevertheless stopped during Pre-Execute because the
webserver, scheduler, and DAG processor resolved different `api jwt_secret`
values. The webserver returned 403 with `InvalidSignatureError`, and the
scheduler recorded `Invalid auth token`; Spark was never reached.

This is an Atlas-internal shared-secret defect, not a consumer endpoint
override problem. [atlas#850](https://github.com/thekaveh/atlas/issues/850)
originally tracked the attempted hypothesis that a durable shared
`AIRFLOW__API__JWT_SECRET` plus cross-service configuration would repair the
failure. The later retest below proves that hypothesis ineffective: Airflow
3.3 reads `[api_auth] jwt_secret`. The correction required at that point was a
shared `AIRFLOW__API_AUTH__JWT_SECRET`, effective-configuration regression
coverage, and successful representative DAG proof. The issue was still open,
so the consumer had to repin to a corrected reviewed upstream fix and rerun the
focused live suite before promotion.

The same live run exposed a separate consumer-owned notebook/data compatibility
problem: the declared January–June 2023 NYC Taxi Parquet files disagree on
`passenger_count` (`INT64` in March, `double` elsewhere). The paired Zeppelin
and Jupyter batch-ingest notebooks now read each declared object in deterministic
order, cast that column to `double` at the read boundary, and union by name.
Their Bronze filtering and Iceberg output contract remain unchanged.

## Focused #850 retest (2026-07-28, atlas `af7713ee`)

The current reviewed pin includes #850's attempted repair: Atlas generates one
durable `AIRFLOW_JWT_SECRET` and supplies `AIRFLOW__API__JWT_SECRET` with the
same value to the webserver, scheduler, and DAG processor. That container
environment value is shared, but the Airflow 3.3 effective configuration is
`[api_auth] jwt_secret`, not `[api] jwt_secret`. `airflow config get-value api
jwt_secret` therefore resolves a different shared runtime value, and the
representative `nyc_taxi_etl` task again fails at Pre-Execute with `Invalid auth
token` before Spark starts.

The scheduler and DAG processor correctly resolve
`AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-webserver:8080/execution/`,
so #791's in-network DNS repair is validated. The rest of the focused gate also
passed: consumer launch/doctor/endpoint assertion, Layer 1 (13/13), Layer 2
(6/6), datasets, Jenkins JAR build, repaired default-small Zeppelin notebook,
Trino, and streaming producer/consumer. The paired Jupyter process completed
cleanly but did not retain a pytest terminal summary and is not claimed as a
pass.

At this point, [atlas#850](https://github.com/thekaveh/atlas/issues/850) was
reopened pending `AIRFLOW__API_AUTH__JWT_SECRET` plus an effective-configuration
regression test across the webserver, scheduler, and DAG processor. Airflow DAG
live success and Gitflow promotion remained gated on a corrected reviewed pin.
[atlas#792](https://github.com/thekaveh/atlas/issues/792) was a separate
SparkSubmitHook status-poll caveat.

## Corrected #850 pin before final retest (2026-07-28, atlas `882877a4`)

Atlas [#850](https://github.com/thekaveh/atlas/issues/850) is closed by the
reviewed correction merged through Atlas PRs #860 and #861. It keeps the durable
shared `AIRFLOW_JWT_SECRET` but maps it as
`AIRFLOW__API_AUTH__JWT_SECRET` on the Airflow services, which configures
Airflow 3.3's effective `[api_auth] jwt_secret` setting. Atlas also updates its
manifest, Compose, environment reference, baseline, and regression test to lock
that section mapping.

The parent pinned that immutable merged commit before the final acceptance run.
At this historical point, `nyc_taxi_etl` still had to complete and verify
effective secret parity. The `af7713ee` section above remains the failed-gate
evidence; [atlas#792](https://github.com/thekaveh/atlas/issues/792) remained a
separate SparkSubmitHook status-poll caveat at this pin. The later `0644a8f3`
update supplied the provider-compatible REST-confirmation pattern, and the
2026-07-31 result above records the completed rerun.
