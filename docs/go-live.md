# Atlas Go-Live Runbook — `data-eng-lab`

This document is the playbook for **deferred live validation** of the Atlas enablement items (A1–A9). The Atlas infrastructure is considered stable and pinned; this runbook validates the end-to-end lakehouse scenarios against the delivered contract documented in `docs/atlas-enablement.md`.

**Scope:** Validates A1–A6 (delivered) and scaffolds assumptions for A7–A9 (deferred to Phase 5).

**Timeline:** Run this after Atlas is pinned to a release tag (currently `62eb6df`). Full run takes ~30–45 minutes (including container startup and test suites).

---

## Phase 1: Launch

Bring up the full data-eng stack with the new lakehouse services.

```bash
# From repo root, enable the iceberg-rest and jenkins services
./scripts/start-all.sh --iceberg-rest-source container --jenkins-source container
```

This launches ~20+ containers including:
- Spark (standalone master + worker)
- Spark Connect server
- Iceberg REST catalog (PostgreSQL-backed)
- MinIO (S3 storage)
- Zeppelin (Scala Spark notebooks)
- JupyterHub (PySpark + PyIceberg kernels)
- Airflow (orchestration)
- Jenkins (CI/CD)
- Supabase Postgres (catalog backend + project metadata)
- Supavisor (process supervisor for app spawning)
- Weaviate + Neo4j (search/graph databases)

**Expected outcome:**
- `docker ps` shows ~20+ running containers (exact count depends on resource provisioning).
- All containers reach "healthy" status within 2–3 minutes.
- `curl http://localhost:63020/v1/config` returns a valid Iceberg REST catalog config (substitute `63020` with the actual `ICEBERG_REST_PORT` from `.env`).

---

## Phase 2: Bootstrap

Prepare the lakehouse namespaces and run preflight checks.

### 2.1 Create MinIO buckets

The MinIO init script creates core buckets at Atlas bootstrap (`lakehouse`, `jars`, `checkpoints`). Verify they exist:

```bash
# MinIO console should be accessible at http://localhost:9001 (or from .env MINIO_CONSOLE_PORT)
# Log in with root credentials (MINIO_ROOT_USER / MINIO_ROOT_PASSWORD from .env)
# Verify buckets: lakehouse, jars, checkpoints exist
```

Alternatively, verify via `mc` inside the MinIO container:

```bash
docker exec -it $(docker ps -q -f "name=minio") mc ls minio/ | grep -E 'lakehouse|jars|checkpoints'
```

### 2.2 Register Iceberg namespaces

Atlas init does **not** pre-seed namespaces. Create them now:

```bash
# From repo root, run the registration script
uv run python scripts/register_iceberg.py
```

**What it does:**
- Connects to the Iceberg REST catalog at `http://localhost:${ICEBERG_REST_PORT:-63020}` (the host-side address; inferred from `.env`).
- Creates the `bronze`, `silver`, and `gold` namespaces (idempotent — safe to re-run).

**Expected output:**
```
Registering namespaces in Iceberg REST catalog...
✓ Namespace 'bronze' created
✓ Namespace 'silver' created
✓ Namespace 'gold' created
```

### 2.3 Preflight Layer 1 (connectivity)

Run Layer 1 (L1) health checks to confirm all services are reachable:

```bash
RUN_INFRA=1 uv run pytest tests/infra/test_preflight_live.py::test_layer1_all_pass_against_live_stack -v
```

**Expected:** All L1 service probes pass (containers healthy and responding to init checks).

### 2.4 Preflight Layer 2 (functional)

Run Layer 2 (L2) tests to confirm service-to-service integration edges are functional:

```bash
RUN_INFRA=1 uv run pytest tests/infra/test_layer2_live.py::test_layer2_matrix_all_pass -v
```

**Expected:** All L2 edges pass: spark→minio+iceberg, jupyter→pyiceberg, airflow→minio+spark,
zeppelin→spark, trino→lakehouse (if TRINO_ENABLED), spark→redpanda (if REDPANDA_ENABLED).

---

## Phase 3: Validate Live

End-to-end validation of the four key user-facing paths: Zeppelin notebooks, Jupyter notebooks, Jenkins CI, and Airflow orchestration.

### 3.1 Run all integration tests

Execute the full infra test suite (includes L1 + L2 + scenario parity):

```bash
RUN_INFRA=1 uv run pytest tests/infra/ tests/scenarios/ -m infra -q
```

**Expected:** All tests pass. Output summary shows 0 failures.

To run individual test modules:

```bash
# Layer 1 — service existence & initialization
RUN_INFRA=1 uv run pytest tests/infra/test_preflight_live.py -v

# Layer 2 — service-to-service integration matrix (Trino + Redpanda edges included)
RUN_INFRA=1 uv run pytest tests/infra/test_layer2_live.py -v

# Scala/PySpark parity — batch_ingest-nyc_taxi-spark-iceberg notebook equivalence
RUN_INFRA=1 uv run pytest tests/scenarios/test_scenario_execution_live.py::test_batch_ingest_scala_pyspark_parity -v
```

### 3.2 Zeppelin: Run a Scala Spark notebook

Launch a notebook scenario in Zeppelin:

1. Navigate to Zeppelin UI: `http://localhost:${ZEPPELIN_PORT}` (the host port from `infra/.env`, slot-allocated — not 8080).
2. Create a new Scala `%spark` notebook named `test-bronze-read`.
3. Add paragraphs:
   ```scala
   %spark
   spark.version
   ```
   Expected output: `4.1.2` (or the current Spark version).

   ```scala
   %spark
   spark.sql("SHOW NAMESPACES IN lakehouse").show()
   ```
   Expected output: `bronze`, `silver`, `gold` rows.

   ```scala
   %spark
   spark.sql("""
     CREATE TABLE IF NOT EXISTS lakehouse.bronze.zeppelin_test (
       id INT,
       name STRING
     )
     USING iceberg
   """)
   spark.sql("INSERT INTO lakehouse.bronze.zeppelin_test VALUES (1, 'test')")
   spark.sql("SELECT * FROM lakehouse.bronze.zeppelin_test").show()
   ```
   Expected output: 1 row with `id=1, name='test'`.

4. Verify the table persists in MinIO:
   ```bash
   docker exec -it $(docker ps -q -f "name=minio") mc ls minio/lakehouse/
   ```
   Should show the metadata and data files for `zeppelin_test`.

### 3.3 Jupyter: Run a PySpark + PyIceberg notebook

Launch a notebook scenario in JupyterHub:

1. Navigate to JupyterHub: `http://localhost:${JUPYTERHUB_PORT}` (the host port from `infra/.env`, slot-allocated — not 8000).
2. Log in with credentials from `.env` (typically `jupyter` user, password auto-generated).
3. Create a new Python notebook named `test-silver-write`.
4. Add cells:
   ```python
   import pyiceberg
   from pyiceberg.catalog import load_catalog
   
   catalog = load_catalog("rest", **{"uri": "http://iceberg-rest:8181"})
   print("Namespaces:", catalog.list_namespaces())
   ```
   Expected output: Lists `['bronze', 'silver', 'gold']`.

   ```python
   import pandas as pd
   from pyspark.sql import SparkSession
   
   # Spark Connect session auto-configured
   spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
   
   df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
   spark.createDataFrame(df).write \
     .format("iceberg") \
     .mode("overwrite") \
     .saveAsTable("lakehouse.silver.jupyter_test")
   
   spark.sql("SELECT * FROM lakehouse.silver.jupyter_test").show()
   ```
   Expected output: 3 rows.

### 3.4 Scala/PySpark parity test

Validate that the Scala Zeppelin notebook and the PySpark Jupyter notebook produce
identical Iceberg table contents for the `batch_ingest-nyc_taxi-spark-iceberg` scenario:

```bash
RUN_INFRA=1 uv run pytest tests/scenarios/test_scenario_execution_live.py::test_batch_ingest_scala_pyspark_parity -v
```

**What it does:**
1. Imports and runs `scenarios/batch_ingest-nyc_taxi-spark-iceberg/zeppelin/notebook.zpln`
   via the Zeppelin REST API; snapshots `lakehouse.bronze.nyc_taxi_trips`.
2. Drops the table (clean-slate for PySpark).
3. Copies `scenarios/batch_ingest-nyc_taxi-spark-iceberg/jupyter/notebook.ipynb` into
   the jupyterhub container via `docker cp`, executes it with papermill; snapshots again.
4. Asserts schema, row_count, and checksum are identical between the two engines.

**Expected:** Test passes with `1 passed` — schema + row_count + checksum match.

**Requirements:** `ICEBERG_REST_PORT`, `MINIO_PORT`, `MINIO_ROOT_USER`,
`MINIO_ROOT_PASSWORD`, `ZEPPELIN_PORT` (env or `infra/.env`); `PROJECT_NAME`
(defaults to `data-eng-lab`); Docker CLI available on host; jupyterhub container
running with `papermill` or `jupyter-nbconvert`.

### 3.5 Jenkins: Publish and trigger the Maven JAR build

Build and publish the `nyc-taxi-etl` Spark application via Jenkins.

#### Step 1: Seed the Jenkins job (one-time)

From the repo root, first export the required environment variables, then run the seed script:

```bash
# Source the slot-allocated env (Jenkins credentials + port live here)
source infra/.env
# Or export individually:
# export JENKINS_ADMIN_USER JENKINS_ADMIN_PASSWORD JENKINS_PORT
bash jenkins/seed-job.sh
```

**What it does:**
- Authenticates to Jenkins (`JENKINS_URL` from `.env`, credentials from `JENKINS_ADMIN_PASSWORD`).
- Creates or updates the seed job (via JCasC / Job-DSL).
- Points the seed job at the `data-eng-lab` repo (GitHub URL from config).

**Expected output:**
```
Seeding Jenkins job definitions...
✓ Seed job created: 'seed-job'
✓ Job dsl applied: 'nyc-taxi-etl-build'
```

#### Step 2: Trigger the build

Via Jenkins UI (at `http://localhost:${JENKINS_PORT:-63080}`, or from `.env` JENKINS_PORT):
1. Navigate to the `nyc-taxi-etl-build` job.
2. Click **Build Now**.
3. Monitor the build log; it should:
   - Check out the `data-eng-lab` repo.
   - Run `mvn -q -B -f spark-apps/nyc-taxi-etl/pom.xml package`.
   - Run unit tests (ScalaTest).
   - Produce the shaded JAR: `target/app.jar`.
   - Upload to MinIO: `s3a://jars/nyc-taxi-etl/0.1.0/app.jar`.

**Expected output:** Build succeeds with **BUILD SUCCESS**, JAR is present in MinIO.

Verify the JAR exists:

```bash
# Via MinIO console: http://localhost:9001 → navigate to 'jars' bucket → 'nyc-taxi-etl/0.1.0/app.jar'
# Or via mc:
docker exec -it $(docker ps -q -f "name=minio") mc cat minio/jars/nyc-taxi-etl/0.1.0/app.jar | wc -c
# Should output a non-zero byte count (JAR size)
```

### 3.6 Airflow: Trigger the NYC Taxi ETL DAG

Run the end-to-end lakehouse pipeline via Airflow.

#### Prerequisites

- The `nyc-taxi-etl` JAR must already be published to `s3a://jars/nyc-taxi-etl/0.1.0/app.jar` via Jenkins (§3.5 above).
- Landing-zone Parquet data must be present in MinIO. Populate it with:
  ```bash
  uv run python scripts/download_datasets.py
  ```
  This seeds `s3a://landing/nyc_taxi/` with the NYC taxi Parquet files consumed by the ETL.

#### Running the DAG

1. Navigate to Airflow UI: `http://localhost:${AIRFLOW_PORT}` (the host port from `infra/.env`, slot-allocated — not 8080).
2. Find the `nyc_taxi_etl` DAG (dag_id `nyc_taxi_etl`, auto-discovered from the `spark-apps/` DAG mount).
3. Manually trigger the DAG (click **Trigger DAG**) or wait for the `@daily` schedule.
4. The DAG has a **single task**: `submit_nyc_taxi_etl` — a `SparkSubmitOperator` that:
   - Submits `s3a://jars/nyc-taxi-etl/0.1.0/app.jar` to the Spark standalone cluster in cluster deploy-mode.
   - Passes the full `spark.sql.catalog.lakehouse.*` configuration so the driver finds the Iceberg REST catalog.
   - Reads Parquet from `s3a://landing/nyc_taxi/`.
   - Writes to `lakehouse.bronze.nyc_taxi_trips`.

**Expected output:**
- DAG run completes with status **Success**.
- Spark driver logs (in the Spark History UI or Airflow task logs) show:
  ```
  [spark-submit] ... Reading from s3a://landing/nyc_taxi/ ...
  [spark-submit] ... Writing to iceberg table lakehouse.bronze.nyc_taxi_trips ...
  ```
- Verify the table in Spark (via Zeppelin or Jupyter):
  ```scala
  // In Zeppelin
  spark.sql("SELECT COUNT(*) FROM lakehouse.bronze.nyc_taxi_trips").show()
  ```
  Expected output: Row count > 0.

### 3.7 Trino + streaming validation (A7/A9)

**Prerequisites:** Enable the Trino + Redpanda sources:
```bash
# If not already enabled, re-launch with both sources
./scripts/start-all.sh --iceberg-rest-source container --jenkins-source container --trino-source container --redpanda-source container
```

#### 3.7.1 Trino: Federated query

Validate Trino's Iceberg connector and CTAS capability:

1. **Via Zeppelin `%trino` interpreter:**
   - Zeppelin UI → Create a new notebook named `test-trino-federated`.
   - Add a paragraph:
     ```
     %trino
     SELECT COUNT(*) FROM lakehouse.bronze.nyc_taxi_trips
     ```
   - Expected output: Row count > 0 (from the Airflow DAG run above).

   - Add another paragraph to test CTAS:
     ```
     %trino
     CREATE TABLE lakehouse.gold.nyc_taxi_sample AS
     SELECT * FROM lakehouse.bronze.nyc_taxi_trips LIMIT 100
     ```
   - Expected: Table created in the `gold` namespace.

2. **Via Jupyter + Python Trino client:**
   - JupyterHub → New Python notebook named `test-trino-jupyter`.
   - Cell:
     ```python
     from trino.dbapi import connect
     
     conn = connect(
         host="localhost",
         port=63029,  # or read from os.getenv("TRINO_PORT")
         user="atlas",
         catalog="lakehouse",
         schema="bronze"
     )
     cursor = conn.cursor()
     cursor.execute("SELECT COUNT(*) FROM nyc_taxi_trips")
     print(cursor.fetchone())
     ```
   - Expected output: Row count tuple.

#### 3.7.2 Redpanda + Structured Streaming

Validate Redpanda broker and Spark Kafka connector:

1. **Seed a Redpanda topic (if not already seeded):**
   ```bash
   # Run the producer (auto-creates the 'events' topic if not present)
   uv run python scenarios/streaming_ingest-events-spark-iceberg/producer.py
   ```
   This will publish sample events to `redpanda:9092` topic `events`.

2. **Via Zeppelin `%spark` — Structured Streaming read:**
   - Zeppelin UI → Create a notebook named `test-streaming-read`.
   - Paragraph (reads from Redpanda, writes to Iceberg + checkpoint):
     ```scala
     %spark
     spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", "redpanda:9092")
       .option("subscribe", "events")
       .option("startingOffsets", "earliest")
       .load()
       .select(col("value").cast("string") as "json_value")
       .writeStream
       .format("iceberg")
       .option("path", "s3a://lakehouse/bronze/streaming_test")
       .option("checkpointLocation", "s3a://checkpoints/streaming_test")
       .mode("append")
       .start()
     ```
   - Expected: Stream starts, events flow in, checkpoint stored in MinIO.

3. **Run automated live tests:**
   ```bash
   RUN_INFRA=1 uv run pytest tests/scenarios/test_trino_query_live.py tests/scenarios/test_streaming_live.py -v
   ```
   - Expected: Both test modules pass (or skip if `RUN_INFRA=0`).
   - `test_trino_query_live.py` validates: Trino connectivity, Iceberg catalog read/write via CTAS.
   - `test_streaming_live.py` validates: Redpanda connectivity, `readStream.format("kafka")`, Iceberg write + checkpoint.

---

## Phase 4: Manual Steps

A few services require one-time UI setup that cannot be automated (yet).

### 4.1 Zeppelin: Configure the JDBC interpreter (one-time)

The Zeppelin init script seeds the `%spark` (Scala) interpreter but not the `%jdbc` SQL interpreter. If you want direct SQL access to the Iceberg catalog, configure JDBC once:

1. Zeppelin UI → Interpreter → Search for `jdbc`.
2. Click the pencil icon to edit the `jdbc` interpreter.
3. Add property:
   ```
   default.driver = org.postgresql.Driver
   default.url = jdbc:postgresql://supabase-db:5432/iceberg
   default.user = <ICEBERG_DB_USER>
   default.password = <ICEBERG_DB_PASSWORD>
   ```
4. Save. New notebooks can now use `%jdbc` paragraphs to query the Iceberg metadata catalog.

### 4.2 Enable Iceberg REST and Jenkins sources (if toggled off)

If you started the stack with sources disabled, re-enable them for the full stack:

```bash
# Kill the current stack (optional, or restart specific containers)
docker-compose down

# Re-launch with sources enabled
./scripts/start-all.sh --iceberg-rest-source container --jenkins-source container
```

Then re-run Phase 2 (bootstrap) to re-register namespaces and pre-flight checks.

### 4.3 Jenkins: Manual authentication (if needed)

If Jenkins prompts for CSRF token or credentials during seed:

1. Jenkins UI → Manage Jenkins → Security → Configure Global Security.
2. Ensure **CSRF Protection** is enabled (should be by default).
3. Retrieve the initial admin password from the container log:
   ```bash
   docker logs $(docker ps -q -f "name=jenkins") | grep "Initial Admin Password"
   ```
4. Use this password + username `admin` to unlock Jenkins the first time.

---

## Summary: Success Criteria

A successful go-live run should satisfy:

1. ✅ **Phase 1:** All ~20+ containers are healthy and reachable.
2. ✅ **Phase 2:** Namespaces registered, L1 + L2 preflight tests pass.
3. ✅ **Phase 3:**
   - Zeppelin notebook runs Scala Spark queries; tables persist in MinIO.
   - Jupyter notebook runs PySpark + PyIceberg; Spark Connect is auto-configured.
   - `test_batch_ingest_scala_pyspark_parity` passes — Scala (Zeppelin) and PySpark (Jupyter) produce identical schema + row_count + checksum for `lakehouse.bronze.nyc_taxi_trips`.
   - Jenkins successfully builds and publishes the Maven JAR to MinIO.
   - Airflow DAG completes; `lakehouse.bronze.nyc_taxi_trips` has rows.
   - **(A7/A9)** Trino `%trino` interpreter reads/writes Iceberg via CTAS; Redpanda broker accepts Kafka reads; Spark Kafka connector streams to Iceberg + checkpoint.
4. ✅ **Phase 4:** No blocking manual setup (JDBC interpreter optional).

If all above pass, the Atlas enablement is **validated for production use** and the lakehouse is ready for full scenario execution (including Trino multi-engine + Redpanda streaming).

---

## Troubleshooting

### Service X is not reachable

- **Iceberg REST (port 63020):** Check that `iceberg-rest-source` is enabled. Verify Supabase Postgres is running (`docker ps | grep supabase`). If not, re-run `start-all.sh` with `--iceberg-rest-source container`.
- **Jenkins (host port ${JENKINS_PORT:-63080}):** Check `JENKINS_SOURCE` is enabled. Verify the Jenkins container has sufficient memory (Jenkins needs 1GB+).
- **JupyterHub (`${JUPYTERHUB_PORT}` from `infra/.env`):** Check the JupyterHub container logs: `docker logs $(docker ps -q -f "name=jupyterhub")`.

### Spark Connect times out

- Ensure the Spark Connect server is running: `docker ps | grep spark-connect`.
- Check that the Connect server's `spark.master` is pointing to the Spark standalone master: `docker logs $(docker ps -q -f "name=spark-connect") | grep "master\|spark.master"`.

### Iceberg namespace registration fails

- Verify the Iceberg REST catalog is reachable: `curl http://iceberg-rest:8181/v1/config`.
- Check the catalog's Postgres backend: `docker exec -it $(docker ps -q -f "name=supabase") psql -U postgres -d iceberg -c "\dn"` (should list schemas).
- Retry the registration: `uv run python scripts/register_iceberg.py --force` (if supported).

### Jenkins build hangs

- Jenkins may be building in a queue. Check the Jenkins UI for the build log.
- If Maven is slow, pre-pull the Maven dependencies (build cache may be empty).
- Ensure MinIO is running and the Jenkins container has network access: `docker exec -it $(docker ps -q -f "name=jenkins") nc -zv minio 9000`.

### Airflow DAG does not trigger

- Verify the DAG file is present: `docker exec -it $(docker ps -q -f "name=airflow-webserver") ls /home/airflow/dags/`.
- Check the DAG for syntax errors: `docker exec -it $(docker ps -q -f "name=airflow-scheduler") airflow dags test nyc_taxi_etl 2024-07-01`.

---

## Next Steps

After a successful go-live run:

1. **Phase 5 prep:** With A1–A6 validated, Phase 5 can begin authoring Trino (A7) and Redpanda (A9) scenarios against the assumed Atlas contract.
2. **Automation:** Integrate this runbook into CI/CD so every Atlas release is validated end-to-end.
3. **Documentation:** Update this runbook as new scenarios are added (e.g., Trino BI queries, Redpanda streaming).

---

*Cross-referenced from:* `docs/atlas-enablement.md` — the A1–A9 enablement contract and delivered shapes.

*See also:* [`docs/go-live-results.md`](go-live-results.md) — actual results from the 2026-07-04 live validation run (30 containers, 156 GiB Docker; all 6 Layer-2 edges passed; bugs found and fixed).

*Maintained by `data-eng-lab`.* Latest update: Phase 4 Task 4 (go-live runbook authoring).
