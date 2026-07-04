# Atlas Go-Live Findings — Infra Issues from `data-eng-lab` (2026-07-04)

**From:** `data-eng-lab` (private consumer of Atlas)
**Re:** issues found while running the full live go-live validation against the delivered Atlas `data-eng` track (atlas `85ff46b`).
**Purpose:** a prioritized list of **Atlas-side** fixes. Each notes the lab-side workaround we can **remove** once Atlas addresses it — the "minimize & optimize our Atlas-related changes" goal.

**TL;DR:** The stack came up and the entire lakehouse surface validated end-to-end (preflight L1+L2, batch ingest, full medallion, Trino CTAS, Redpanda→Spark streaming, and the Jenkins→JAR→Airflow loop). We hit **4 Atlas-side items** — 2 affecting the Airflow→Spark production path, 1 healthcheck gap, 1 doc clarification. **None blocked validation** (we worked around each); fixing them lets us drop the workarounds. The lab-side bugs we found (preflight probes, parity harness, our Jenkins seed script) are ours and are already fixed — they are **not** in this list.

## Issues (priority order)

### 1. [HIGH] Spark standalone master REST submission server not enabled → breaks `SparkSubmitOperator` cluster mode
- **What:** Airflow's `SparkSubmitOperator` with `deploy_mode=cluster` (the standard production Airflow→Spark pattern) submits to `spark://spark-master:7077`. The driver launches on a worker and **runs to completion**, but the operator's post-submit `_start_driver_status_tracking` polls the master **REST API on :6066** for driver status — Atlas's standalone master does not expose it → the poll fails and the task is marked **FAILED even though the job succeeded and wrote the table**.
- **Evidence:** `INFO ClientEndpoint: State of driver driver-20260704123956-0000 is FINISHED` (job OK), immediately followed by `airflow.sdk.exceptions.AirflowException: Failed to poll for the driver status 10 times: returncode = 1`. The Iceberg table was written (snapshot `2026-07-04 12:40:08Z`, `overwrite`, 2,943,859 rows).
- **Fix:** enable the standalone master REST submission server — `spark.master.rest.enabled=true` (and expose port `6066` in-net) on `spark-master`.
- **Lab workaround removed after fix:** we currently carry a caveat comment in `spark-apps/nyc-taxi-etl/dag.py` + a note in `docs/go-live-results.md`, and rely on `spark.standalone.submit.waitAppCompletion=true`. With the REST server on, cluster-mode DAGs "just work" and the caveat can be dropped.

### 2. [HIGH] Spark Connect server monopolizes all standalone worker cores → starves standalone-mode workloads
- **What:** the `spark-connect` server registers as a standalone application holding **all** cluster cores (`coresused 4/4` on the 4-core `data-eng` cluster). Any standalone-mode workload — Airflow `spark-submit` (cluster mode) and Zeppelin `%spark` interpreter jobs — then queues indefinitely (`PENDING`) with no cores available.
- **Evidence:** a Zeppelin `%spark` `spark.read.parquet(...)` paragraph stayed `PENDING` for 8+ min; `spark-master/json` showed `activeapps=[{name: "Spark Connect server", cores: 4}]`, `coresused=4/4`. Stopping `spark-connect` freed the cores and the Airflow spark-submit driver then ran to `FINISHED`.
- **Fix:** cap the Spark Connect server's `spark.cores.max` (leave headroom) and/or provision more worker cores in the `data-eng` track. E.g. Connect `spark.cores.max=2` on a 4-core cluster leaves 2 cores for standalone submits.
- **Lab workaround removed after fix:** our go-live runbook currently stops `spark-connect` to free cores before Airflow/Zeppelin runs. With headroom, Spark Connect notebooks and standalone Airflow/Zeppelin jobs coexist, and that stop/restart step goes away.

### 3. [LOW] `spark-connect` container has no healthcheck → wait-for-healthy tooling hangs
- **What:** `spark-connect` never reports a Docker `healthy` status (no healthcheck defined), so tooling that gates on "core services healthy" waits on it forever.
- **Evidence:** `start-all.sh`'s `wait_healthy ... spark-connect ...` never passed even though the container was `Up` and serving on :15002; it shows `Up`, never `(healthy)`.
- **Fix:** add a healthcheck to the `spark-connect` service (TCP `:15002` or the Spark Connect gRPC health probe).
- **Lab workaround removed after fix:** our `start-all.sh` / preflight can include `spark-connect` in health gating cleanly instead of special-casing it.

### 4. [LOW / DOC] Airflow 3 Task-SDK: connections not resolvable outside a task context
- **What:** in Airflow 3, `BaseHook.get_connection(...)` / `S3Hook(aws_conn_id=...)` raise `AirflowNotFoundException` when invoked **outside a task execution context** (e.g. `docker exec airflow-scheduler python probe.py`), even though the connection exists in the metadata DB (`airflow connections get <id>` shows it, with valid creds).
- **Evidence:** `airflow.sdk.exceptions.AirflowNotFoundException: The conn_id 'minio_default' isn't defined` from `airflow.sdk.execution_time.context._get_connection`, while `minio_default` is present in the DB.
- **Fix:** not strictly an Atlas bug (it is Airflow 3 behavior), but a line in Atlas's Airflow service README — "connections seeded in the DB must be read via a metadata-DB session, not the Task-SDK, outside a task" — would save downstream consumers the investigation.
- **Lab handling:** our airflow preflight probe now reads `minio_default`/`spark_default` from the metadata DB directly; this can stay. Documenting upstream just helps the next consumer.

## What worked (for a fair picture)

Everything else validated cleanly: `data-eng` track launch (~30 containers), iceberg-rest + Trino reachability, bucket + namespace bootstrap, Spark→Iceberg→MinIO, Jupyter/pyiceberg, Zeppelin→Spark (interpreter healthy), **Trino→lakehouse including CTAS writes**, Spark→Redpanda (the `spark-sql-kafka` jar baked in), the Jenkins job (SCM-clones the public repo → `mvn` → publishes the JAR), the full medallion (bronze→silver→gold), and Redpanda→Spark-Structured-Streaming→Iceberg. See `docs/go-live-results.md` for the numbers.

## Net

Issues **#1** and **#2** are the ones worth fixing to make the Airflow→Spark **cluster-mode** path (a core data-eng use case) work cleanly end-to-end; **#3** and **#4** are polish. After #1 + #2, `data-eng-lab` can drop its `spark-connect`-stop step and the `SparkSubmitOperator` caveat, simplifying the runbook and the DAG.
