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
- Connects to the Iceberg REST catalog at `http://iceberg-rest:8181` (inferred from `.env`).
- Creates namespaces: `bronze`, `silver`, `gold` (idempotent).
- Optionally creates a test table under `bronze` to confirm write path.

**Expected output:**
```
Registering namespaces in Iceberg REST catalog...
✓ Namespace 'bronze' created
✓ Namespace 'silver' created
✓ Namespace 'gold' created
✓ Test table created: lakehouse.bronze.test_registration
```

### 2.3 Preflight Layer 1 (connectivity)

Run Layer 1 (L1) health checks to confirm all services are reachable:

```bash
RUN_INFRA=1 uv run pytest tests/integration/test_preflight.py::test_l1_iceberg_rest -v
RUN_INFRA=1 uv run pytest tests/integration/test_preflight.py::test_l1_spark_connect -v
RUN_INFRA=1 uv run pytest tests/integration/test_preflight.py::test_l1_airflow -v
RUN_INFRA=1 uv run pytest tests/integration/test_preflight.py::test_l1_jenkins -v
RUN_INFRA=1 uv run pytest tests/integration/test_preflight.py::test_l1_jupyter -v
RUN_INFRA=1 uv run pytest tests/integration/test_preflight.py::test_l1_zeppelin -v
```

**Expected:** All L1 tests pass (services are reachable and respond to basic probes).

### 2.4 Preflight Layer 2 (functional)

Run Layer 2 (L2) tests to confirm core workflows are functional:

```bash
RUN_INFRA=1 uv run pytest tests/integration/test_preflight.py::test_l2_iceberg_write -v
RUN_INFRA=1 uv run pytest tests/integration/test_preflight.py::test_l2_spark_catalog -v
RUN_INFRA=1 uv run pytest tests/integration/test_preflight.py::test_l2_minio_access -v
```

**Expected:** L2 tests pass; tables can be written to Iceberg + MinIO, and are discoverable via Spark.

---

## Phase 3: Validate Live

End-to-end validation of the four key user-facing paths: Zeppelin notebooks, Jupyter notebooks, Jenkins CI, and Airflow orchestration.

### 3.1 Run all integration tests

Execute the full infra test suite (includes L1 + L2 + smoke scenarios):

```bash
RUN_INFRA=1 uv run pytest -m infra -q
```

**Expected:** All tests pass. Output summary shows 0 failures.

### 3.2 Zeppelin: Run a Scala Spark notebook

Launch a notebook scenario in Zeppelin:

1. Navigate to Zeppelin UI: `http://localhost:8080` (or from `.env` ZEPPELIN_PORT).
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

1. Navigate to JupyterHub: `http://localhost:8000` (or from `.env` JUPYTER_PORT).
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

### 3.4 Jenkins: Publish and trigger the Maven JAR build

Build and publish the `nyc-taxi-etl` Spark application via Jenkins.

#### Step 1: Seed the Jenkins job (one-time)

From the repo root, run the seed script:

```bash
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

Via Jenkins UI (at `http://localhost:8080`, or from `.env` JENKINS_PORT):
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

### 3.5 Airflow: Trigger the NYC Taxi ETL DAG

Run the end-to-end lakehouse pipeline via Airflow.

1. Navigate to Airflow UI: `http://localhost:8080` (Airflow port, not Jenkins—check `.env` for `AIRFLOW_PORT`).
2. Find the `nyc_taxi_etl` DAG (auto-discovered from `dags/` mount).
3. Manually trigger the DAG (or check auto-scheduling from the DAG definition).
4. Monitor the run:
   - **Task 1:** `load_nyc_taxi_csv` — reads `data/nyc_taxi_2024.csv` into `s3a://landing/nyc_taxi_trips/`.
   - **Task 2:** `spark_submit_to_bronze` — runs the Scala JAR (`s3a://jars/nyc-taxi-etl/0.1.0/app.jar`) on the Spark cluster with `--deploy-mode cluster`, reads from `landing`, writes to `lakehouse.bronze.nyc_taxi_trips`.
   - **Task 3:** `check_bronze_table` — validates the table exists and has rows.

**Expected output:**
- DAG run completes with status **Success**.
- Spark driver logs show:
  ```
  [spark-submit] ... Reading nyc_taxi_trips from s3a://landing/nyc_taxi_trips/ ...
  [spark-submit] ... Writing to iceberg table lakehouse.bronze.nyc_taxi_trips ...
  [spark-submit] ... Written 100000 rows to bronze (example) ...
  ```
- Verify the table in Spark (via Zeppelin or Jupyter):
  ```scala
  // In Zeppelin
  spark.sql("SELECT COUNT(*) FROM lakehouse.bronze.nyc_taxi_trips").show()
  ```
  Expected output: Row count > 0.

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
   - Jenkins successfully builds and publishes the Maven JAR to MinIO.
   - Airflow DAG completes; `lakehouse.bronze.nyc_taxi_trips` has rows.
4. ✅ **Phase 4:** No blocking manual setup (JDBC interpreter optional).

If all above pass, the Atlas enablement is **validated for production use** and the lakehouse is ready for Phase 5 (Trino + Redpanda scenarios).

---

## Troubleshooting

### Service X is not reachable

- **Iceberg REST (port 63020):** Check that `iceberg-rest-source` is enabled. Verify Supabase Postgres is running (`docker ps | grep supabase`). If not, re-run `start-all.sh` with `--iceberg-rest-source container`.
- **Jenkins (port 8080):** Check `JENKINS_SOURCE` is enabled. Verify the Jenkins container has sufficient memory (Jenkins needs 1GB+).
- **JupyterHub (port 8000):** Check the JupyterHub container logs: `docker logs $(docker ps -q -f "name=jupyterhub")`.

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

*Maintained by `data-eng-lab`.* Latest update: Phase 4 Task 4 (go-live runbook authoring).
