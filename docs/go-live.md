# 8.2. Atlas Go-Live Runbook

This playbook validates the Atlas enablement items (A1–A9) against the delivered
contract documented in `atlas-expectations.md`. The current 2026-08-10 accepted
baseline and the earlier 2026-07-31 historical baseline are recorded below;
rerun the same gates for every later Atlas pin.

**Scope:** Validates A1–A9 (all delivered). Full run takes ~30–45 minutes (including container startup and test suites).

---

## 1. Phase 1: Launch

Bring up the full data-eng stack with the new lakehouse services.

```bash
make up
```

`make up` runs `./scripts/start-all.sh`, a thin wrapper over Atlas's own headless
commands in eight phases: stale-symlink cleanup → `env backfill` → consumer
`compose validate` → consumer `doctor` (manifest + compose + env lints against
`atlas.consumer.yml`) → `start.sh --consumer … --track data-eng --no-tui --detach`
(health-gates the whole track before returning) → generated `atlas-consumer.env`
plus `ATLAS_MINIO_HOST_ENDPOINT` assertion → Iceberg namespace registration →
preflight (Layer 1 + Layer 2). All nine `data-eng` services are containerized by
default via the manifest's `env.values` — no `--<svc>-source` CLI flags needed.
`make up` starts services only; it does not acquire datasets or change a dataset
active pointer.

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
- The launcher writes the ignored `atlas-consumer.env` and asserts its supported
  `ATLAS_MINIO_HOST_ENDPOINT` export. For unexported host services, use an
  explicit override or the corresponding port in `infra/.env`.

---

## 2. Phase 2: Bootstrap

Prepare the lakehouse namespaces and run preflight checks.

### 2.1 Buckets

Atlas's minio-init provisions the core buckets (`landing`, `lakehouse`, `jars`,
`checkpoints`) at bootstrap. `atlas.consumer.yml`'s `storage:` key additionally
declares the lab's own `lakehouse-test` scratch bucket, provisioned the same way
with scoped credentials — nothing to run manually. Verify via `mc` inside the
MinIO container if you want to confirm:

```bash
docker exec -it $(docker ps -q -f "name=minio") mc ls minio/ | grep -E 'lakehouse|jars|checkpoints|lakehouse-test'
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
```text
Registering namespaces in Iceberg REST catalog...
✓ Namespace 'bronze' created
✓ Namespace 'silver' created
✓ Namespace 'gold' created
```

### 2.3 Preflight Layer 1 (connectivity)

> **Prerequisite for `RUN_INFRA=1` runs:** the live tests below import client
> libraries not in the default (offline) install — `pyspark-client` (Spark
> Connect), `kafka-python`, and `trino`. Install them once via the `live`
> dependency group before running any `RUN_INFRA=1` command:
>
> ```bash
> uv sync --group live
> ```
>
> Offline CI does not install this group, so it is unaffected.

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

**Expected:** All L2 edges pass: spark→minio+iceberg, jupyter→pyiceberg, airflow→minio+spark, zeppelin→spark, trino→lakehouse (unless `TRINO_SOURCE=disabled`), spark→redpanda (unless `REDPANDA_SOURCE=disabled`). Optional-service edges are gated on the Atlas `*_SOURCE` values in `infra/.env` (set to `container` by `atlas.consumer.yml`), so on a default stack every edge runs.

### 2.5 Publish and verify the dataset tier

Dataset acquisition is a separate, explicit operation after MinIO is healthy:

```bash
make datasets SCALE=small
uv run python scripts/download_datasets.py --scale small --verify-only
```

The first command publishes a complete locked generation when none is active,
or verifies the existing active pointer, immutable manifest, object bytes, and
physical schemas. The second command is read-only and fails when active state is
missing, corrupt, or at another scale. The rule is that runtime mismatch never updates the registry;
intentional lock changes use the issue #80 audit and review workflow in
[Datasets](datasets.md).

Set `DATASET_SCALE=tiny|small|medium` for a run-scoped default. Explicit Airflow
run configuration `{"dataset_scale":"tiny"}` or a notebook scale override takes
precedence; `small` applies only when neither an explicit value nor
`DATASET_SCALE` is present. The internal `dataset-resolver` verifies the selected
generation before returning one immutable URI set to the run.

---

## 3. Phase 3: Validate Live

End-to-end validation of the four key user-facing paths: Zeppelin notebooks, Jupyter notebooks, Jenkins CI, and Airflow orchestration.

The production TPC-H acceptance sequence publishes `tpch-star-schema/0.1.0/app.jar`, triggers `tpch_star_schema` with an explicit `dataset_scale`, requires Airflow success and Spark `FINISHED` with `success=true`, then compares both tables' schemas, row counts, deterministic checksums, and `data_eng_lab.dataset*` properties across a same-generation rerun. Query a nonempty segment-revenue join through Trino before teardown. A between-table failure is recovered by rerunning the same immutable generation; never treat mixed provenance as successful output.

`tpch_star_schema` is the only supported production write path and serializes runs with
`max_active_runs=1`. The educational notebooks directly replace the same tables without production
provenance and validation, so running them against shared gold tables can invalidate downstream #83.

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

Validate that the Scala Zeppelin notebook and the PySpark Jupyter notebook produce identical Iceberg table contents for the `batch_ingest-nyc_taxi-spark-iceberg` scenario:

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
```text
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
# Via MinIO console: http://localhost:${MINIO_CONSOLE_PORT} (from infra/.env) → 'jars' bucket → 'nyc-taxi-etl/0.1.0/app.jar'
# Or via mc:
docker exec -it $(docker ps -q -f "name=minio") mc cat minio/jars/nyc-taxi-etl/0.1.0/app.jar | wc -c
# Should output a non-zero byte count (JAR size)
```

### 3.6 Airflow: Trigger the NYC Taxi ETL DAG

Run the end-to-end lakehouse pipeline via Airflow.

#### Prerequisites

- The `nyc-taxi-etl` JAR must already be published to `s3a://jars/nyc-taxi-etl/0.1.0/app.jar` via Jenkins (section 3.5 above).
- A verified NYC Taxi generation must be active in MinIO. Publish and verify it with:
   ```bash
   make datasets SCALE=small
   uv run python scripts/download_datasets.py --scale small --verify-only
   ```
   Trigger the DAG with `{"dataset_scale":"small"}` to make the expected scale explicit.

#### Running the DAG

1. Navigate to Airflow UI: `http://localhost:${AIRFLOW_PORT}` (the host port from `infra/.env`, slot-allocated — not 8080).
2. Find the `nyc_taxi_etl` DAG (dag_id `nyc_taxi_etl`, auto-discovered from the `spark-apps/` DAG mount).
3. Manually trigger the DAG (click **Trigger DAG**) or wait for the `@daily` schedule.
4. The DAG has a **single task**: `submit_nyc_taxi_etl` — an operator-owned `AtlasSparkSubmitOperator` task that:
    - Submits `s3a://jars/nyc-taxi-etl/0.1.0/app.jar` to the Spark standalone cluster in cluster deploy-mode through `spark_default` (`spark://spark-master:7077`).
    - Passes the full `spark.sql.catalog.lakehouse.*` configuration so the driver finds the Iceberg REST catalog.
    - Confirms the returned driver ID through Atlas's in-network Spark REST endpoint (`spark-master:6066`), requiring `FINISHED` and `success: true`.
    - Resolves NYC Taxi during task execution, validates the requested scale, and passes the returned immutable generation URIs to Spark.
    - Writes to `lakehouse.bronze.nyc_taxi_trips`.

**Expected output:**
- DAG run completes with status **Success**.
- Spark driver logs (in the Spark History UI or Airflow task logs) show the verified immutable URI arguments and target table without a flat landing-path fallback:
   ```text
   [spark-submit] ... s3://landing/nyc_taxi/_generations/<plan-id>/<publication-id>/yellow_tripdata_2023-01.parquet ...
   [spark-submit] ... Writing to iceberg table lakehouse.bronze.nyc_taxi_trips ...
   ```
- Verify the table in Spark (via Zeppelin or Jupyter):
   ```scala
   // In Zeppelin
   spark.sql("SELECT COUNT(*) FROM lakehouse.bronze.nyc_taxi_trips").show()
   ```
   Expected output: Row count > 0.

**Current accepted baseline (2026-08-10, atlas
`c6cf73d7168db1a7840fc45c9ed3e385071996d8`):** PRs #95, #96, and #97 promoted
the operator-owned contract. Airflow runs
`issue78_nyc_taxi_etl_20260810T233212Z` and
`issue78_nyc_taxi_medallion_20260810T233242Z` both succeeded. Their Spark REST
records, `driver-20260810233215-0003` and `driver-20260810233245-0004`, reached
`FINISHED` with `success=true`. Jenkins ETL build #5 and medallion build #1
succeeded, and preflight passed Layer 1 at 13/13 and Layer 2 at 6/6. No false
driver-status polling failure or exception was present. Both DAGs retained
`SparkSubmitOperator.execute()` ownership through `AtlasSparkSubmitOperator`,
with Atlas's `RestConfirmingSparkHook` enforcing the terminal REST result.

**Historical accepted baseline (2026-07-31, atlas `985918ce8c805081947d53b1c48bb80610237a5b`):** the reviewed pin included Atlas
[#850](https://github.com/thekaveh/atlas/issues/850)'s corrected shared
`AIRFLOW__API_AUTH__JWT_SECRET` mapping and [Atlas #880](https://github.com/thekaveh/atlas/issues/880)'s
provider-compatible REST-confirmation helper. At that historical pin, the DAG constructed the hook without an application
and called `submit_and_confirm_via_rest()` so it did not run the provider's incompatible post-submit
`:7077` status poll. The helper captures the standalone driver ID from the spark-submit log before
checking `:6066`; it still allows genuine submission failures to raise and rejects a failed or
non-terminal driver. The representative feature-artifact task succeeded on its first and only
attempt, and Spark reported `FINISHED` with `success=true`. For any later Atlas pin, rerun this
step and require the same terminal REST evidence before promotion. The current
`c6cf73d7168db1a7840fc45c9ed3e385071996d8` contract instead keeps
`SparkSubmitOperator.execute()` in charge and wraps `super()._get_hook()` with
`RestConfirmingSparkHook`; the `:7077` submission and `:6066` terminal criteria are unchanged.

### 3.7 Trino + streaming validation (A7/A9)

**Prerequisites:** none — Trino and Redpanda are containerized by default via
`atlas.consumer.yml`'s `env.values` (`TRINO_SOURCE: container`,
`REDPANDA_SOURCE: container`). To disable either, edit the manifest and re-run
`make up`.

#### 3.7.1 Trino: Federated query

Validate Trino's Iceberg connector and CTAS capability:

1. **Via Zeppelin `%trino` interpreter:**
    - Zeppelin UI → Create a new notebook named `test-trino-federated`.
    - Add a paragraph:
       ```sql
       %trino
       SELECT COUNT(*) FROM lakehouse.bronze.nyc_taxi_trips
       ```
     - Expected output: Row count > 0 (from the Airflow DAG run above).

     - Add another paragraph to test CTAS:
       ```sql
       %trino
       CREATE TABLE lakehouse.gold.nyc_taxi_sample AS
       SELECT * FROM lakehouse.bronze.nyc_taxi_trips LIMIT 100
       ```
     - Expected: Table created in the `gold` namespace.

2. **Via Jupyter + Python Trino client:**
    - JupyterHub → New Python notebook named `test-trino-jupyter`.
    - Cell:
       ```python
       import os

       from trino.dbapi import connect

       conn = connect(
           host="localhost",
           port=int(os.environ["TRINO_PORT"]),   # auto-allocated; from infra/.env
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

## 4. Phase 4: Manual Steps

A few services require one-time UI setup that cannot be automated (yet).

### 4.1 Zeppelin: Configure the JDBC interpreter (one-time)

The Zeppelin init script seeds the `%spark` (Scala) interpreter but not the `%trino` SQL interpreter (which Atlas now auto-seeds per our agreement). If you want direct SQL access to the Iceberg metadata catalog, configure JDBC once:

1. Zeppelin UI → Interpreter → Search for `jdbc`.
2. Click the pencil icon to edit the `jdbc` interpreter.
3. Add property:
    ```properties
    default.driver = org.postgresql.Driver
    default.url = jdbc:postgresql://supabase-db:5432/iceberg
    default.user = <ICEBERG_DB_USER>
    default.password = <ICEBERG_DB_PASSWORD>
    ```
4. Save. New notebooks can now use `%jdbc` paragraphs to query the Iceberg metadata catalog.

### 4.2 Re-enable a disabled service

All `data-eng` services are containerized by default via `atlas.consumer.yml`'s
`env.values` (`*_SOURCE: container`). If you disabled one by editing the manifest
to `disabled`, flip it back to `container` and re-launch:

```bash
make down
make up
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

## 5. Exhaustive Notebook Reproducibility

The representative parity test is intentionally small enough for routine acceptance. Before a release or Atlas pin promotion, use the separate exhaustive gate to re-execute both the Zeppelin and Jupyter notebook for every scenario.

Run this gate only against an exclusive, disposable lab stack. To give both notebook formats equivalent starting state, it intentionally drops each scenario-owned output table before the Zeppelin run and again before the Jupyter run. The reset is limited to the tables declared in `OUTPUT_TABLES` in `tests/scenarios/test_notebook_reproducibility_live.py`:

- `lakehouse.bronze.nyc_taxi_trips`, `lakehouse.bronze.events`, `lakehouse.bronze.gh_events_stream`
- `lakehouse.silver.nyc_taxi_trips`, `lakehouse.silver.nyc_taxi_clean`, `lakehouse.silver.nyc_taxi_quarantine`, `lakehouse.silver.gh_events_se`, `lakehouse.silver.nyc_taxi_tt`, `lakehouse.silver.nyc_taxi_tm`, `lakehouse.silver.online_retail_cdc`, `lakehouse.silver.online_retail`, `lakehouse.silver.gh_events`, `lakehouse.silver.gh_sessions`
- `lakehouse.gold.nyc_taxi_daily`, `lakehouse.gold.event_windows`, `lakehouse.gold.nyc_taxi_daily_trino`, `lakehouse.gold.dim_customer`, `lakehouse.gold.fct_orders`, `lakehouse.gold.bi_segment_revenue`, `lakehouse.gold.tpch_segment_revenue`, `lakehouse.gold.ml_user_features`, `lakehouse.gold.ml_movie_features`, `lakehouse.gold.dim_customer_scd2`

It also deletes only the `events/`, `gh_events_file/`, `event_windows/`, and `online_retail_cdc/` prefixes from the MinIO `checkpoints` bucket before their owning streaming formats run. Do not use the gate on a shared environment where those tables or prefixes contain state you need to preserve. It does not edit Atlas source, drop unrelated tables, or delete unrelated MinIO objects.

Prepare the complete live environment in dependency order:

```bash
make up
make datasets
uv run python scripts/register_iceberg.py
make notebooks-reproducibility
```

The Make target sets `RUN_INFRA=1` and runs `tests/scenarios/test_notebook_reproducibility_live.py` with the `live` dependency group. It discovers and cross-checks all 19 paired scenario directories, then executes their Zeppelin and Jupyter notebooks through the same REST/container helpers used by representative acceptance. Seventeen cases are Scala/PySpark pairs; the two Trino cases use SQL/client notebooks. Broker scenarios also require their producers/topics. In the temporary execution copies, each streaming query drains currently available input and stops its own `query` in a `finally` block before the test resets its table and checkpoint; unrelated active queries are not stopped. Budget several hours for a full run; this gate is deliberately separate from the offline PR suite.

**Expected outcome:** 19 parameterized scenario cases pass, meaning both notebook formats completed without an execution error for every current scenario.

---

## 6. Summary: Success Criteria

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

## 7. Troubleshooting

### Service X is not reachable

- **Iceberg REST (host port `${ICEBERG_REST_PORT}`, from infra/.env):** Check that `ICEBERG_REST_SOURCE: container` is set in `atlas.consumer.yml`'s `env.values`. Verify Supabase Postgres is running (`docker ps | grep supabase`). If not, fix the manifest and re-run `make up`.
- **Jenkins (host port ${JENKINS_PORT:-63080}):** Check `JENKINS_SOURCE` is enabled. Verify the Jenkins container has sufficient memory (Jenkins needs 1GB+).
- **JupyterHub (`${JUPYTERHUB_PORT}` from `infra/.env`):** Check the JupyterHub container logs: `docker logs $(docker ps -q -f "name=jupyterhub")`.

### Spark Connect times out

- Ensure the Spark Connect server is running: `docker ps | grep spark-connect`.
- Check that the Connect server's `spark.master` is pointing to the Spark standalone master: `docker logs $(docker ps -q -f "name=spark-connect") | grep "master\|spark.master"`.
- Atlas now ships its own TCP healthcheck on `spark-connect:15002` (atlas#310) — `make up`'s `--detach` health-gates on it, so a hung Connect server blocks bring-up rather than surfacing later. If bring-up itself times out here, check the spark-connect container's health status directly: `docker inspect --format '{{.State.Health.Status}}' $(docker ps -q -f "name=spark-connect")`.

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

### DAG task dies at Pre-Execute (supervisor ConnectionError → SIGKILL)

- Confirm the target Atlas configuration is active on both the scheduler and DAG
  processor: `AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-webserver:8080/execution/`.
  If it is absent, confirm the checked-out submodule pin and rerun `make up` so
  Atlas rebuilds changed images automatically. See [Atlas Go-Live Findings](atlas-feedback-go-live.md)
  for #791's validated DNS configuration, #850's corrected mapping, the resolved
  #792 status-poll path, and the historical failed-gate evidence.

---

## 8. Next Steps

After a successful go-live run:

1. **Scenario execution:** Use the [execution-mode matrix](scenarios/execution-modes.md)
   delivered by [issue #82](https://github.com/thekaveh/data-eng-lab/issues/82)
   as the authority for all 19 validated paired scenarios. `nyc_taxi_etl`,
   `nyc_taxi_medallion`, and `tpch_star_schema` are production DAGs today; approved children must
   pass their own implementation and live-acceptance gates before this runbook
   advertises another Airflow entrypoint.
2. **Automation:** Integrate this runbook into CI/CD so every Atlas release is validated end-to-end.
3. **Documentation:** Update this runbook as new scenarios are added (e.g., additional streaming sources, Trino BI patterns).

---

*Cross-referenced from:* `atlas-expectations.md` — the A1–A9 enablement contract and delivered shapes.

*See also:* [Go-Live Results](go-live-results.md) — the 2026-07-04 platform validation and the scoped 2026-07-31 Atlas acceptance result.

*Maintained by `data-eng-lab`.* Latest update: 2026-08-10 current-pin Atlas acceptance baseline recorded.
