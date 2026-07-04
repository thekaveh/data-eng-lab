# Atlas Go-Live Findings — Infra Issues from `data-eng-lab`

**From:** `data-eng-lab` (private consumer of the Atlas platform)
**Date of run:** 2026-07-04
**Atlas version under test:** `data-eng` track, atlas pinned at `85ff46b` (the commit that resolved the earlier A7/A9 doc feedback — see [`atlas-feedback-a7a9.md`](atlas-feedback-a7a9.md)).
**Purpose:** a prioritized, self-contained list of the **Atlas-side** infrastructure issues we hit while running a full end-to-end go-live validation. Each issue is written so an Atlas maintainer can act on it with **no prior context**: symptom → how we hit it → evidence → root cause → suggested fix → the lab-side workaround it lets us remove → how to verify the fix.

> **Scope note.** This document lists only **Atlas-side** issues. The live run also surfaced four **lab-side** bugs (our preflight probes, our Scala/PySpark parity harness, and our Jenkins seed script) — those are ours, are already fixed on `main`, and are deliberately **not** in this list. The full run write-up (what passed, with row counts) is in [`go-live-results.md`](go-live-results.md).

---

## TL;DR

The Atlas `data-eng` track came up cleanly and we validated the **entire lakehouse surface** end-to-end: service-health + integration preflight, batch ingestion, the full medallion (bronze→silver→gold), Trino CTAS with bidirectional Spark↔Trino interop, Redpanda→Spark-Structured-Streaming→Iceberg, and the Jenkins→JAR→Airflow production loop.

We hit **4 Atlas-side items**. **None blocked validation** — we worked around each — but fixing them lets us delete lab-side workarounds and makes the Airflow→Spark production path work without caveats:

| # | Severity | Issue | Area | Fixing it lets the lab drop… |
|---|----------|-------|------|------------------------------|
| 1 | **HIGH** | Spark standalone master REST server (`:6066`) not enabled → `SparkSubmitOperator` cluster-mode false-negatives after the job succeeds | Spark master / Airflow | the DAG caveat comment + `waitAppCompletion` reliance |
| 2 | **HIGH** | Spark Connect server holds **all** cluster cores → starves standalone `spark-submit` and Zeppelin `%spark` | Spark Connect / cluster sizing | the "stop spark-connect to free cores" runbook step |
| 3 | LOW | `spark-connect` container has no Docker healthcheck → wait-for-healthy tooling hangs on it | Spark Connect | the `spark-connect` special-case in `start-all.sh` |
| 4 | LOW / DOC | Airflow 3 Task-SDK: DB-seeded connections aren't resolvable outside a task context | Airflow | (nothing — we keep the DB-read; a README line just helps the next consumer) |

**Recommended order:** #1 and #2 first (they gate the Airflow→Spark **cluster-mode** path, a core data-engineering use case). #3 and #4 are polish.

---

## Environment (how to reproduce the setup)

- **Host:** macOS, Docker Desktop with **156 GiB** memory / **24 CPUs** allocated (so none of the below is host-resource starvation).
- **Launch:** `./scripts/start-all.sh` from the `data-eng-lab` repo, which runs `cd infra && ./start.sh --track data-eng --no-tui` with the iceberg-rest / jenkins / trino / redpanda sources set to `container`. ~30 containers came up (spark master + connect + 2 workers + history, zeppelin, jupyterhub, iceberg-rest, minio, trino + console, redpanda + console, airflow ×4, jenkins, the supabase stack, kong, weaviate, neo4j).
- **Slot-allocated host ports this run** (yours will differ; read from `infra/.env`): `ICEBERG_REST_PORT=63022`, `TRINO_PORT=63029`, `MINIO_PORT=63020`, `ZEPPELIN_PORT=63099`, `REDPANDA_KAFKA_PORT=63010`, `JENKINS_PORT=63090`.
- **Cluster shape:** standalone Spark master `spark://spark-master:7077` with **2 workers / 4 cores total**; Spark Connect at `sc://spark-connect:15002` (in-net only, no host port).
- **Bootstrap before testing:** `create_buckets.sh` + `scripts/register_iceberg.py` (created `lakehouse.bronze` / `silver` / `gold` — Atlas intentionally seeds no namespaces).

All container names below are prefixed with the compose project name (`data-eng-lab-…`, i.e. `$PROJECT_NAME` from `infra/.env`).

---

## Issue 1 — [HIGH] Spark standalone master REST submission server not enabled

**Symptom.** Airflow's `SparkSubmitOperator` with `deploy_mode=cluster` — the standard production pattern for orchestrating a Spark JAR from Airflow — **runs the job successfully** (the driver launches on a worker and finishes, and the output table is written) but the Airflow **task is then marked FAILED**.

**How we hit it.** The `nyc_taxi_etl` DAG (`spark-apps/nyc-taxi-etl/dag.py`) submits `s3a://jars/nyc-taxi-etl/0.1.0/app.jar` (class `com.thekaveh.dataeng.nyctaxi.NycTaxiEtl`) to `spark://spark-master:7077` in cluster mode. Reproduce directly:

```bash
docker exec <project>-airflow-scheduler \
  airflow tasks test nyc_taxi_etl submit_nyc_taxi_etl 2025-06-01
```

**Evidence (from the task log).** The driver runs to completion, then the operator fails on its *post-submit* status poll:

```
INFO ClientEndpoint: Driver successfully submitted as driver-20260704123956-0000
INFO ClientEndpoint: State of driver-20260704123956-0000 is RUNNING
INFO ClientEndpoint: Driver running on 172.18.0.13 (worker-…-38469)
INFO ClientEndpoint: State of driver driver-20260704123956-0000 is FINISHED, exiting spark-submit JVM.
…
airflow.sdk.exceptions.AirflowException: Failed to poll for the driver status 10 times: returncode = 1
```

The job genuinely succeeded — the Iceberg table was written, verified independently via the snapshot history (Trino):

```
committed_at              operation  total-records
2026-07-04 12:40:08 UTC   overwrite  2943859   ← written by the JAR via this DAG run
```

**Root cause.** After a cluster-mode submit, `apache-airflow-providers-apache-spark`'s `SparkSubmitHook._start_driver_status_tracking()` polls the driver's status by shelling out to `spark-submit --master spark://spark-master:7077 --status <driverId>`, which talks to the **standalone master's REST submission server on port `6066`**. Atlas's `spark-master` does not enable/expose that REST server, so every status poll exits `1`; after 10 attempts the hook raises. The failure is **purely in status tracking** and happens *after* the driver already reported `FINISHED`.

**Impact.** Any Airflow DAG that uses `SparkSubmitOperator` in `deploy_mode=cluster` (the recommended production mode, so the driver runs on the cluster rather than inside the Airflow worker) will be marked failed despite succeeding — breaking retries, alerting, and downstream task gating. This is a first-class data-engineering use case.

**Suggested Atlas fix.** Enable the standalone master REST submission server on `spark-master`:

```
spark.master.rest.enabled  true
# and ensure port 6066 is reachable in-network (it is the master's REST submission port)
```

(Standalone Spark exposes driver status over this REST endpoint; with it on, `spark-submit --status <driverId>` returns `0` and the operator tracks status correctly.)

**Lab workaround we remove after the fix.** We currently (a) carry a ~6-line caveat comment above the `SparkSubmitOperator` in `spark-apps/nyc-taxi-etl/dag.py`, (b) document the caveat in `go-live-results.md`, and (c) rely on `spark.standalone.submit.waitAppCompletion=true` as the completion signal. With the REST server enabled we delete the caveat and cluster-mode DAGs "just work."

**How to verify the fix.** Re-run `airflow tasks test nyc_taxi_etl submit_nyc_taxi_etl <date>`; the task should end **success** with no "Failed to poll for the driver status" error.

---

## Issue 2 — [HIGH] Spark Connect server monopolizes all standalone worker cores

**Symptom.** Standalone-mode Spark workloads — Airflow `spark-submit` (cluster mode) and Zeppelin `%spark` interpreter paragraphs — **queue forever in `PENDING`** and never get an executor, even though the cluster is idle from the user's perspective.

**How we hit it.** Running the `batch_ingest` Scala notebook in Zeppelin, the first Spark action (`spark.read.parquet("s3a://landing/nyc_taxi/")`) sat at `PENDING` for **8+ minutes** with no progress.

**Evidence.** The standalone master's JSON shows the Spark Connect server holding **every** core:

```bash
docker exec <project>-spark-master sh -c 'curl -s http://localhost:8080/json/' | \
  python3 -c "import sys,json;d=json.load(sys.stdin);print('cores',d['cores'],'used',d['coresused'],'workers',d['aliveworkers']);[print(a['name'],a['cores']) for a in d['activeapps']]"
# cores 4 used 4 workers 2
# Spark Connect server 4
```

`coresused = 4/4`, and the single active app is `Spark Connect server` (cores `4`). Confirmation that this is the cause: stopping Spark Connect frees the cores and the previously-stuck standalone submit immediately proceeds —

```bash
docker stop <project>-spark-connect     # coresused -> 0/4
# the Airflow spark-submit driver then went RUNNING -> FINISHED
docker start <project>-spark-connect     # restore
```

**Root cause.** The Spark Connect server is launched as a long-lived standalone application with an **uncapped `spark.cores.max`**, so on a 4-core cluster it grabs all 4 cores and holds them for its lifetime. Standalone Spark schedules apps FIFO by available cores; with 0 free, any other app (Airflow driver, Zeppelin interpreter) stays `PENDING` indefinitely.

**Impact.** Spark Connect (Jupyter/PySpark) and standalone-mode workloads (Airflow `spark-submit`, Zeppelin `%spark`) **cannot run concurrently** on the default `data-eng` cluster. Since the lab uses Spark Connect for Jupyter notebooks *and* standalone submit for Airflow *and* the standalone interpreter for Zeppelin, they contend for the same 4 cores.

**Suggested Atlas fix.** Cap the Spark Connect server's core request so it leaves headroom, and/or size the `data-eng` cluster with more worker cores. For example, on a 4-core cluster:

```
# on the Spark Connect server launch:
spark.cores.max  2        # leave 2 cores for standalone submits / Zeppelin
```

(Alternatively raise the total worker cores so Connect + at least one standalone app fit simultaneously.)

**Lab workaround we remove after the fix.** Our go-live runbook currently instructs stopping `spark-connect` to free cores before running Airflow/Zeppelin standalone jobs. With headroom, that stop/restart step disappears and Spark Connect notebooks + standalone jobs coexist.

**How to verify the fix.** With `spark-connect` **running**, execute a Zeppelin `%spark` paragraph (or an Airflow cluster-mode `spark-submit`); it should get cores and complete instead of sitting at `PENDING`.

---

## Issue 3 — [LOW] `spark-connect` container has no Docker healthcheck

**Symptom.** Tooling that waits for "all core services healthy" hangs on `spark-connect` forever, because it never transitions to Docker `healthy`.

**Evidence.**

```bash
docker ps --filter name=<project>-spark-connect --format '{{.Status}}'
# Up 12 minutes            ← note: "Up", never "(healthy)"
```

Our `start-all.sh` step `wait_healthy … spark-connect …` never returned (it sat at "waiting for core services to be healthy") even though the Connect server was up and serving on `:15002`; every other core service reported `(healthy)`.

**Root cause.** The `spark-connect` service definition has no `healthcheck`, so Docker reports only `Up`, never `healthy`. Any orchestration that gates on health status can't tell when Connect is actually ready.

**Suggested Atlas fix.** Add a healthcheck to the `spark-connect` service — a TCP check on `:15002`, or the Spark Connect gRPC health probe. e.g.:

```yaml
healthcheck:
  test: ["CMD-SHELL", "bash -c '</dev/tcp/localhost/15002' || exit 1"]
  interval: 10s
  timeout: 3s
  retries: 12
```

**Lab workaround we remove after the fix.** Our `start-all.sh` / preflight can include `spark-connect` in normal health gating instead of special-casing it (we currently proceed without a clean health signal for it).

**How to verify the fix.** `docker inspect --format '{{.State.Health.Status}}' <project>-spark-connect` reports `healthy` once ready.

---

## Issue 4 — [LOW / DOC] Airflow 3 Task-SDK: DB-seeded connections aren't resolvable outside a task

**Symptom.** Code that resolves an Airflow connection **outside a task execution context** (e.g. a health probe run via `docker exec … python probe.py`) raises `AirflowNotFoundException`, even though the connection is present in the metadata DB.

**Evidence.** The connection exists and has valid credentials:

```bash
docker exec <project>-airflow-scheduler airflow connections get minio_default
# shows conn with extra_dejson {'aws_access_key_id': 'minioadmin', …}
```

…but resolving it via a hook outside a task fails:

```
airflow.sdk.exceptions.AirflowNotFoundException: The conn_id 'minio_default' isn't defined
  at airflow.sdk.execution_time.context._get_connection
```

**Root cause.** In Airflow 3 the Task-SDK routes `BaseHook.get_connection()` / `S3Hook(aws_conn_id=…)` through the task **execution API**, which is only wired up inside a running task. Outside a task (a standalone script, a `docker exec` probe), it can't see DB-seeded connections. This is **Airflow 3 behavior, not strictly an Atlas bug** — but it surprised us because the connection is clearly present via the CLI.

**Suggested Atlas fix (documentation).** A line in Atlas's Airflow service README would save the next consumer the investigation: *"Connections seeded into the metadata DB are only resolvable via the Task-SDK inside a running task. Standalone scripts/probes should read them from a metadata-DB session (`airflow.settings.Session` + `airflow.models.Connection`), not `BaseHook.get_connection`."*

**Lab handling (no change needed after the fix).** Our Airflow preflight probe now reads `minio_default` / `spark_default` directly from the metadata DB via a SQLAlchemy session, which works reliably. We keep this regardless; the README note just helps others.

---

## What worked (for a fair picture)

Everything else validated cleanly against the delivered stack:

- **Launch & bootstrap:** the `data-eng` track (~30 containers) came up; `iceberg-rest` `/v1/config` and `trino` `/v1/info` returned HTTP 200; buckets + namespaces bootstrapped.
- **Preflight:** Layer 1 (all services healthy) and Layer 2 integration matrix — Spark→Iceberg→MinIO (real Iceberg write), Jupyter→pyiceberg, Airflow→MinIO, Zeppelin→Spark, **Trino→lakehouse**, **Spark→Redpanda** — all pass.
- **Scenarios:** `batch_ingest` end-to-end (Spark Connect wrote `bronze.nyc_taxi_trips`, Trino read it), the full **medallion** (bronze→silver→gold), **Trino CTAS** (Trino wrote an Iceberg gold mart, Spark read it back — bidirectional interop), and **Redpanda→Spark-Structured-Streaming→Iceberg→Trino**.
- **Production loop:** the Jenkins job (SCM-clones the now-public repo → `mvn test`/`package` → publishes the JAR to MinIO) built and published successfully; the JAR then ran on the cluster via Airflow and wrote 2,943,859 cleaned rows (the only wrinkle being Issue #1's status-poll).

Row counts and commands for all of the above are in [`go-live-results.md`](go-live-results.md).

---

## Relationship to prior Atlas feedback

The earlier round of feedback — the A7 (Trino) / A9 (Redpanda) **delivery** review — was resolved by Atlas in PR #281 (the `85ff46b` we tested here). See [`atlas-feedback-a7a9.md`](atlas-feedback-a7a9.md) §F for that closure. This document is the **next** round: infra issues that only surface when the stack is actually run end-to-end, not from reading the contract.

---

## Net

- **Fix #1 + #2** to make the Airflow→Spark **cluster-mode** path work cleanly — these are the ones that matter for real data-engineering workloads.
- **#3 + #4** are polish (a healthcheck and a README line).
- After #1 + #2 land, `data-eng-lab` deletes its `spark-connect`-stop runbook step and the `SparkSubmitOperator` caveat — a small, localized cleanup we've pre-identified per issue above.

*Questions → ping `data-eng-lab`. Evidence artifacts (task logs, snapshot history, cluster JSON) available on request.*
