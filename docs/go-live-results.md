# Go-Live Validation Results — `data-eng-lab` Atlas Data-Eng Track

**Date:** 2026-07-04
**Environment:** Full Atlas data-eng track — ~30 containers, 156 GiB Docker image footprint.

---

## Environment

Brought up via `./scripts/start-all.sh`. Container set:

| Layer | Services |
|---|---|
| Spark | spark-master, spark-connect, spark-worker ×2, spark-history |
| Notebooks | zeppelin, jupyterhub |
| Catalog / Storage | iceberg-rest, minio |
| Query engine | trino |
| Streaming | redpanda, redpanda-console |
| Orchestration | airflow-webserver, airflow-scheduler, airflow-worker, airflow-triggerer |
| CI/CD | jenkins |
| Supabase set | supabase-db, supabase-studio, supabase-kong, supabase-auth, supavisor |
| Gateway | kong |
| Vector / Graph | weaviate, neo4j |

Host ports (slot-allocated from `infra/.env`): `ICEBERG_REST_PORT=63022`, `TRINO_PORT=63029`, `MINIO_PORT=63020`, `ZEPPELIN_PORT=63099`, `REDPANDA_KAFKA_PORT=63010`.

---

## Results

### Bootstrap

| Step | Result |
|---|---|
| `create_buckets` — created `lakehouse-test` bucket | PASS |
| `register_iceberg` — created `lakehouse.bronze`, `lakehouse.silver`, `lakehouse.gold` in REST catalog | PASS |
| iceberg-rest `/v1/config` HTTP 200 | PASS |
| trino `/v1/info` HTTP 200 | PASS |

### Preflight Layer 1 (`tests/infra/test_preflight_live.py`)

All services healthy. **PASS.**

### Preflight Layer 2 (`tests/infra/test_layer2_live.py`) — all 6 edges

| Edge | Result |
|---|---|
| spark → minio + iceberg (real Iceberg write) | PASS |
| jupyter → pyiceberg | PASS |
| airflow → minio + spark | PASS |
| zeppelin → spark | PASS |
| trino → lakehouse (`SHOW SCHEMAS FROM lakehouse` returns bronze/silver/gold/information_schema/system) | PASS |
| spark → redpanda | PASS |

### Dataset Ingestion

`download_datasets.py --only nyc_taxi --scale tiny` landed Parquet files into `s3://landing/nyc_taxi/`. **PASS.**

### Trino Live Query

```sql
SHOW SCHEMAS FROM lakehouse
```

Returns: `bronze`, `silver`, `gold`, `information_schema`, `system`. **PASS.**

---

## Bugs Found and Fixed During This Run

| Bug | Root Cause | Fix |
|---|---|---|
| `spark→redpanda` preflight probe failed | Non-mounted file path + `python` binary (Spark image only has `python3`) | Rewrote to an inline `python3 -c` TCP + jar availability check (commit `a408f0d`) |
| `airflow→minio` probe hit `NoCredentialsError` | Airflow-3 Task-SDK cannot resolve DB connections outside a task context | Rewrote to read `minio_default` / `spark_default` directly from the metadata DB (commit `a408f0d`) |
| Parity harness Zeppelin poll crashed with `AttributeError: 'str' object has no attribute 'get'` | `GET /api/notebook/job/{note_id}` returns `body` as a dict (not a list); iterating yielded string keys | Fixed poll parsing to unwrap dict body and filter to dict paragraph entries; added ABORT to error set (this commit) |

---

## Known Issues / Needs Iteration

**Zeppelin `batch_ingest` notebook — full execution not completed.**
The Zeppelin interpreter started successfully (Spark app registered, 2 workers / 4 cores allocated). The first Spark action (`spark.read.parquet(s3a://landing/nyc_taxi/)`) was slow to complete under the 4-core allocation; the run was aborted before finishing.

The scenario's *logic* is validated independently by:
- The CI scalatest suite (Spark 4.1.2, unit-level).
- The Layer-2 `spark→iceberg` write edge (functional).

Full in-notebook execution via the parity harness (`test_batch_ingest_scala_pyspark_parity`) requires either more cores, a longer interpreter timeout, or both. The harness itself is now correct (Zeppelin poll bug fixed); this is a resource/tuning gap, not a code defect.

---

## Summary

Phases 1 and 2 of the go-live runbook are fully validated. All six Layer-2 integration edges pass against the live stack. Three bugs were surfaced and fixed. The remaining gap (full notebook execution) is a resource-tuning issue, not a blocking defect in the Atlas contract or the harness code.

---

*See also:* [Go-Live Runbook](go-live.md) | [Atlas Enablement Contract](atlas-enablement.md)

---

## Deeper Validation — Scenario + Jenkins → JAR → Airflow Loop (2026-07-04)

### batch_ingest End-to-End (Cross-Engine)

Executed via Spark Connect (`sc://spark-connect:15002`, run inside jupyterhub):

- Read `s3a://landing/nyc_taxi/` — **3,066,766 rows, 19 columns**.
- Applied transformations (added `trip_date`, `ingested_at` columns).
- Wrote partitioned Iceberg table `lakehouse.bronze.nyc_taxi_trips`.

Trino independently queried the same table:

```sql
SELECT COUNT(*) FROM lakehouse.bronze.nyc_taxi_trips;
-- 3,066,766
SELECT trip_date, COUNT(*) AS cnt
FROM lakehouse.bronze.nyc_taxi_trips
GROUP BY trip_date ORDER BY cnt DESC LIMIT 1;
-- 2023-01-26 | 114,877
```

**Result: PASS.** Write-with-one-engine / read-with-another on the shared Iceberg table is proven.

### Cluster Resource Note

The Spark Connect server holds all 4 standalone cores (`coresused 4/4`), which starves standalone-mode jobs (the Zeppelin interpreter and Airflow `spark-submit`). For the Airflow test, `spark-connect` was temporarily stopped to free cores, then restarted. On a 4-core cluster, Spark-Connect and standalone-submit workloads contend; a real deployment wants more worker cores or capped `spark.cores.max` on Connect.

### Jenkins → JAR → Airflow Loop

| Step | Result |
|---|---|
| `jenkins/seed-job.sh` — job seeded, SCM clones repo, runs `spark-apps/nyc-taxi-etl/Jenkinsfile` | PASS |
| Jenkins Build #1 (`mvn test + package + publish`, ~100 s) | SUCCESS |
| `jars/nyc-taxi-etl/0.1.0/app.jar` (7.4 KiB) published to MinIO | PASS |
| Airflow `nyc_taxi_etl` DAG triggered, `SparkSubmitOperator` (`deploy_mode=cluster`, `spark://spark-master:7077`) | TRIGGERED |
| Driver ran on a worker to `FINISHED` | PASS |
| `NycTaxiEtl` JAR wrote **2,943,859 cleaned rows** (raw 3.07 M filtered to 2.94 M) to `lakehouse.bronze.nyc_taxi_trips` | PASS |
| Iceberg snapshot confirmed: `committed_at 2026-07-04 12:40:08 UTC`, `operation=overwrite`, `total-records=2,943,859` | PASS |

The JAR schema uses raw columns plus `trip_date` (no `ingested_at`), consistent with the Scala implementation.

**Result: FUNCTIONALLY PROVEN end-to-end.** The full pipeline — source → build → publish → submit → Iceberg write — executed successfully.

### Findings from This Pass

| # | Finding | Resolution |
|---|---|---|
| 1 | `jenkins/seed-job.sh` CSRF crumb must share a session cookie jar with all subsequent POSTs; without it Jenkins returns HTTP 403 "No valid crumb". | **Fixed in this branch** — `CJ=$(mktemp)` cookie jar threaded through crumb fetch and all authenticated POSTs. |
| 2 | `SparkSubmitOperator` in standalone **cluster mode** false-negatives: after the driver `FINISHED` successfully, `_start_driver_status_tracking` raises `AirflowException: Failed to poll for the driver status 10 times: returncode = 1`. The operator queries the Spark master REST API (port 6066), which Atlas's standalone master does not expose, so the Airflow task is marked failed **even though the job succeeded and wrote the table**. | Three options (no DAG change made without maintainer decision): **(a)** enable `spark.master.rest.enabled=true` (port 6066) on the Atlas Spark master — an Atlas-side configuration request; **(b)** switch the DAG to `deploy_mode=client` (driver runs in the Airflow worker; no remote status polling); **(c)** rely on `spark.standalone.submit.waitAppCompletion=true` (already set in the DAG) as the completion signal and treat the status-poll step as advisory. The job itself is validated end-to-end. |
