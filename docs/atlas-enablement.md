# Atlas Enablement Requests — for the `data-eng-lab` project

**Status:** draft contract `v0.2` · **Consumer repo:** `data-eng-lab` (private) · **Target:** [`thekaveh/atlas`](https://github.com/thekaveh/atlas)

> This document is a hand-off for the Atlas maintainer/worker. It enumerates the
> infrastructure enhancements that `data-eng-lab` needs from Atlas. `data-eng-lab`
> is a **pure downstream consumer** — it vendors Atlas as a pinned git submodule at
> `infra/` and **never edits Atlas source**. Every gap below is therefore raised as
> an upstream feature request (and, where practical, a PR made through the submodule),
> so the capability lands where it belongs: in Atlas, reusable by any project.
>
> Until each item is merged to Atlas `main`, `data-eng-lab` pins its submodule to a
> feature branch (`feat/data-eng-lab-enablement`) that carries these changes, and/or
> reproduces the *effect* at bootstrap time (e.g. seeding an interpreter via the
> Zeppelin REST API) — bootstrap actions only, never edits to Atlas source. Once
> merged, the interim shims are removed and the submodule re-pins to a release tag.

---

## Context: how `data-eng-lab` uses Atlas

- Launches Atlas via its own machinery: `./infra/start.sh --track data-eng --no-tui …`.
- Adds nothing to the Atlas tree except a gitignored symlink in `services/_user/data-eng-lab/compose.yml`
  (Atlas auto-discovers `services/_user/*/compose.yml` in `bootstrapper/core/docker_manager.py`).
- Injects config through the public `infra/.env` contract only (`PROJECT_NAME`, bucket names, catalog URI, branding).
- Bind-mounts its own notebooks / DAGs / datasets into the existing Atlas service containers and runs
  post-launch steps via `docker exec` after health-gating (the `rag-showcase` pattern).

The lakehouse target: **medallion architecture** (`landing` → Iceberg `bronze`/`silver`/`gold`)
on MinIO, cataloged by an **Iceberg REST catalog**, queried from **Zeppelin (Scala Spark)**,
**JupyterHub (PySpark)**, and orchestrated by **Airflow**, with **Maven Scala Spark** apps
built by **Jenkins** and published as JARs to a MinIO bucket.

### Atlas conventions these requests should follow

Please implement each new capability "the Atlas way", as observed in the current tree:

- **Manifest-driven services** — every service declares `services/<name>/service.yml` (env vars,
  `secret: true` / `auto_managed: true`, `runtime_sc:` mirror) + a `services/<name>/compose.yml`
  fragment `include:`d from the root `docker-compose.yml`.
- **Ports** — allocated as `BASE_PORT + offset` by `bootstrapper/services/topology.py`; do not hardcode host ports.
- **Secrets** — generated into `.env` by `bootstrapper/utils/key_generator.py`; `.env.example` is **generated** (edit the manifest, not the file).
- **Buckets** — created idempotently by `services/minio/init/scripts/init-minio.sh` (with scoped `mc` service accounts).
- **Spark jars** — baked into the image at build time with SHA-512 verification (`services/spark/build/Dockerfile`).
- **Tracks** — service membership curated in `bootstrapper/tracks.yml` (parsed by `bootstrapper/tracks.py`).
- **Source toggles** — each service exposes a `*_SOURCE` var (`container` / `disabled`) surfaced as a `--<svc>-source` CLI flag in `bootstrapper/start.py`.

There is prior art for the lakehouse direction in
`docs/research/candidates/iceberg-duckdb.md` (proposes an `iceberg-rest-catalog` + `analytics` bucket).

---

## Summary

| ID | Request | Priority | Unblocks |
|----|---------|----------|----------|
| **A1** | Iceberg REST catalog service + `lakehouse` / `jars` / `checkpoints` MinIO buckets | **P0** | All lakehouse work |
| **A2** | Iceberg Spark runtime jar baked into the Spark image (+ default catalog config) | **P0** | Iceberg over Spark Connect |
| **A3** | Zeppelin Spark interpreter auto-seeded (`spark.remote` + catalog + S3A) | **P1** | "Readily run" Scala notebooks |
| **A4** | `boto3` / `s3fs` / `pyiceberg` / `duckdb` in the JupyterHub image | **P1** | PySpark + PyIceberg notebooks |
| **A5** | `jenkins` service (JDK 21 + Maven + `mc`, JCasC) | **P2** | Build/publish Scala JAR apps |
| **A6** | Airflow able to `spark-submit` an S3A JAR to the standalone master | **P1** | Run JAR apps from DAGs |
| **A7** | *(stretch)* `trino` query engine over the Iceberg REST catalog | **P3** | Multi-engine / BI SQL scenario |
| **A9** | *(fast-follow)* `redpanda` broker + `spark-sql-kafka` connector jar (+ optional Debezium) | **P3** | Broker-backed streaming: windows/watermarks, stateful, CDC |
| **A8** | `data-eng` track updated to include the new services | **P1** | One-command launch |

Critical path: **A1 → A2** (lakehouse core), then A3/A4 (notebook UX) and A6/A5 (JAR CI/CD).

---

## A1 — Iceberg REST catalog service + lakehouse buckets  · P0

**Need.** A shared Iceberg catalog so Spark, PySpark, PyIceberg (and later Trino) all see the
same tables, with table data + metadata on MinIO.

**Current state.** No Iceberg anywhere in the stack (no REST catalog, no Hive Metastore, no Nessie).
Only a proposal doc: `docs/research/candidates/iceberg-duckdb.md`. MinIO buckets today
(`services/minio/init/scripts/init-minio.sh`): `comfyui`, `backend`, `n8n`, `jupyter`, `docling`;
`spark-history` is created separately by `spark-init`.

**Proposed change.**
1. New service `services/iceberg-rest/` (`service.yml` + `compose.yml`), image
   `apache/iceberg-rest-fixture:<pin>` (or equivalent). Port `ICEBERG_REST_PORT:8181` via the slot allocator.
   Joins `backend-network`. `depends_on`: `minio-init` (buckets) and, if JDBC-backed, `supabase-db`.
2. **Durable catalog backend via Supabase Postgres** (mirrors how Airflow uses `supabase-db`): a JDBC
   catalog in a dedicated `iceberg` database/role, created by a small init step analogous to
   `services/airflow/init/scripts/init-airflow.sh`. (An in-memory catalog is acceptable as a
   quick-start fallback but loses tables on restart — please prefer JDBC.)
   Typical env for `apache/iceberg-rest-fixture`:
   ```
   CATALOG_WAREHOUSE=s3a://lakehouse/
   CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO
   CATALOG_S3_ENDPOINT=http://minio:9000
   CATALOG_S3_PATH__STYLE__ACCESS=true
   AWS_REGION=us-east-1
   AWS_ACCESS_KEY_ID=${MINIO_ROOT_USER}
   AWS_SECRET_ACCESS_KEY=${MINIO_ROOT_PASSWORD}
   # JDBC-backed (preferred):
   CATALOG_CATALOG__IMPL=org.apache.iceberg.jdbc.JdbcCatalog
   CATALOG_URI=jdbc:postgresql://supabase-db:5432/iceberg
   CATALOG_JDBC_USER=${ICEBERG_DB_USER}
   CATALOG_JDBC_PASSWORD=${ICEBERG_DB_PASSWORD}
   ```
   *(Confirm exact env keys against the pinned image version.)*
3. **Buckets.** Extend `init-minio.sh` to create (idempotently, with scoped service accounts in the
   existing style): `lakehouse` (Iceberg data + metadata), `jars` (built Maven artifacts),
   `checkpoints` (Structured Streaming). Optionally `landing` too, though `data-eng-lab` can create
   `landing` itself at bootstrap if you'd rather keep it project-scoped.

**Acceptance criteria.**
- `curl http://iceberg-rest:8181/v1/config` returns a valid catalog config.
- Buckets `lakehouse`, `jars`, `checkpoints` exist after `start.sh`.
- Catalog survives a stack restart (tables persist) when JDBC-backed.

---

## A2 — Iceberg Spark runtime on the Spark image  · P0

**Need.** Notebooks talk to the **shared Spark Connect server** (`sc://spark-connect:15002`), whose
classpath is fixed at image build time — so `--packages` at session start **cannot** add Iceberg.
The `iceberg-spark-runtime` jar must be **baked into the image**.

**Current state.** `services/spark/build/Dockerfile` (`FROM apache/spark:4.1.2`) bakes
`hadoop-aws-3.4.2.jar` + AWS SDK v2 `bundle-2.29.52.jar` (SHA-512 verified). No Iceberg jar.

**Proposed change.**
1. Bake the Iceberg Spark runtime jar matching **Spark 4.1 / Scala 2.13** into `/opt/spark/jars`
   with SHA-512 verification (same pattern as the existing jars). Add `iceberg-aws-bundle` if
   `S3FileIO` is used rather than S3A.
   > ⚠️ **Open question / compat check:** Iceberg publishes runtime artifacts per Spark minor
   > (e.g. `iceberg-spark-runtime-4.0_2.13`). Please verify which Iceberg release supports
   > **Spark 4.1.2** specifically and pin the correct artifact; flag back if 4.1 is not yet supported
   > (we may need to align on a Spark version that has an Iceberg runtime).
2. Bake **default catalog config** into the Spark Connect server conf so notebooks need zero
   boilerplate (inert if `iceberg-rest` isn't running):
   ```
   spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog
   spark.sql.catalog.lakehouse.type=rest
   spark.sql.catalog.lakehouse.uri=http://iceberg-rest:8181
   spark.sql.catalog.lakehouse.warehouse=s3a://lakehouse/
   spark.sql.catalog.lakehouse.io-impl=org.apache.iceberg.aws.s3.S3FileIO
   spark.sql.catalog.lakehouse.s3.endpoint=http://minio:9000
   spark.sql.catalog.lakehouse.s3.path-style-access=true
   spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
   ```

**Acceptance criteria.**
- From Spark Connect: `spark.sql("SHOW NAMESPACES IN lakehouse")` runs; `CREATE TABLE lakehouse.bronze.t (...) USING iceberg` writes metadata under `s3a://lakehouse/`.
- Iceberg extension SQL (`MERGE INTO`, `CALL lakehouse.system.rewrite_data_files`, time-travel `VERSION AS OF`) works.

---

## A3 — Zeppelin Spark interpreter auto-seeded  · P1

**Need.** A fresh Zeppelin should run `%spark` Scala against the cluster with **no manual UI setup**.

**Current state.** `services/zeppelin/README.md` §1.2 documents a one-time manual step: Interpreter →
`spark` → add `spark.remote = sc://spark-connect:15002`. There is no `conf/interpreter.json` bind-mount.
`SPARK_SUBMIT_OPTIONS` already carries S3A conf.

**Proposed change.** Seed the `spark` interpreter at provision time — either a mounted
`conf/interpreter.json` (or `interpreter-setting.json`) or an init step that POSTs to the Zeppelin REST
API — pre-setting `spark.remote=sc://spark-connect:15002` plus the Iceberg catalog + S3A properties from A2.

**Acceptance criteria.** Fresh Zeppelin → new `%spark` paragraph → `spark.version` and a
`spark.sql("SHOW NAMESPACES IN lakehouse").show()` succeed with no interpreter configuration.

*(Interim: `data-eng-lab`'s `start-all.sh` will seed this via the Zeppelin REST API until merged.)*

---

## A4 — Data libraries in the JupyterHub image  · P1

**Need.** PySpark + **PyIceberg** notebooks and direct MinIO access from Python.

**Current state.** `services/jupyterhub/build/requirements.txt` has `pyspark-client==4.1.2`,
`weaviate-client`, `neo4j`, `pandas`. No `boto3` / `s3fs` / `pyiceberg`. `SPARK_REMOTE` is already wired.

**Proposed change.** Add `boto3`, `s3fs`, `pyiceberg[s3fs]`, `pyarrow`, and `duckdb` to the image
requirements. Optionally pre-seed env so `boto3`/`pyiceberg` work zero-config: MinIO S3 endpoint +
credentials and `ICEBERG_REST_URI=http://iceberg-rest:8181`.

**Acceptance criteria.** In a Jupyter kernel: `import boto3, s3fs, pyiceberg, duckdb` succeed, and
`pyiceberg.catalog.load_catalog("rest", **{"uri": ICEBERG_REST_URI})` lists namespaces.

---

## A5 — Jenkins CI service  · P2

**Need.** Build the Maven Scala Spark apps (`mvn package`), run their tests, and publish the shaded
JAR to `s3a://jars/<app>/<version>/app.jar`.

**Current state.** No Jenkins in Atlas (CI is GitHub Actions). This is a genuinely new service.

**Proposed change.** New service `services/jenkins/` built `FROM jenkins/jenkins:lts-jdk21`
(Spark 4 needs JDK 17+/21) with **Maven** and **`mc`** (or aws-cli) baked in, plus **JCasC**
(Configuration-as-Code) and a default plugin set (pipeline/workflow, git, configuration-as-code,
job-dsl). Port `JENKINS_PORT:8080` (+ agent `50000`) via the slot allocator; volume `jenkins-home`;
admin password via `key_generator` (`JENKINS_ADMIN_PASSWORD`); MinIO endpoint + creds in env so
pipelines can upload artifacts. Source-toggled (`JENKINS_SOURCE`, default `disabled`).

> **Scope boundary:** Atlas provides the **Jenkins server + JCasC seam**. The **job definitions**
> (Jenkinsfiles, a seed job pointing at the `data-eng-lab` repo) are `data-eng-lab` content, injected
> via JCasC/Job-DSL — not baked into Atlas.

**Acceptance criteria.** Jenkins UI reachable; a pipeline can `mvn -q package` and
`mc cp target/*.jar <alias>/jars/<app>/<ver>/app.jar` against the in-network MinIO.

---

## A6 — Airflow as an S3A-capable `spark-submit` client  · P1

**Need.** A DAG must run a **Scala JAR** (from `s3a://jars/...`) on the Spark cluster, reading/writing MinIO.

**Current state.** Airflow image (3.2.2) has the `apache-spark` + `amazon` providers and
`pyspark[connect]==4.1.2`; `init-airflow.sh` seeds `spark_default` (`spark://spark-master:7077`) and
`minio_default`. It is not currently a full S3A-capable Spark client.

**Proposed change (pick the robust path).** Enable `SparkSubmitOperator` to submit an S3A app JAR to the
standalone master. Recommended: bake `hadoop-aws` + the AWS SDK bundle into the Airflow image's
`pyspark` jars dir (symmetric with A2's Spark image) **and/or** document + enable **standalone
`--deploy-mode cluster`** (driver runs on a worker that already has hadoop-aws, so the submitting
container needs only `spark-submit`). Either makes:
```
spark-submit --master spark://spark-master:7077 --deploy-mode cluster \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 --conf spark.hadoop.fs.s3a.path.style.access=true \
  s3a://jars/nyc-taxi-etl/<ver>/app.jar
```
work end to end.

**Acceptance criteria.** A DAG `SparkSubmitOperator` runs an app JAR fetched from `s3a://jars/...` that
reads from `landing` and writes an Iceberg table under `lakehouse`, and the run appears in Spark History.

> **Open question:** confirm whether standalone **cluster** deploy mode is enabled on the master
> (REST submission / `spark.master.rest`), or whether client mode + hadoop-aws on Airflow is preferred.

---

## A7 — *(stretch)* Trino query engine over Iceberg REST  · P3

**Need.** A second, BI-style SQL engine over the same lakehouse to demonstrate multi-engine Iceberg.

**Proposed change.** New service `services/trino/` with an `iceberg` connector catalog pointing at
`iceberg-rest` (`connector.name=iceberg`, `iceberg.catalog.type=rest`,
`iceberg.rest-catalog.uri=http://iceberg-rest:8181`, S3 → MinIO). Port via slot allocator; add to the
`data-eng` track. Lower priority — include only after A1–A6.

**Acceptance criteria.** `trino> SELECT * FROM iceberg.gold.<mart>` returns rows written by Spark.

---

## A9 — *(fast-follow)* Redpanda broker for streaming  · P3

**Need.** Broker-backed **Structured Streaming** scenarios: windowed aggregations with **watermarks**,
stateful stream-stream joins / dedup-with-state, and real **CDC**. (v1's file-source streaming needs
nothing new; this item unlocks the *interesting* streaming semantics.)

**Current state.** No message broker in Atlas. Structured Streaming is limited to file/rate sources today.

**Proposed change.**
1. New service `services/redpanda/` — **Redpanda** (Kafka-API compatible, single binary, no ZooKeeper).
   Image `redpandadata/redpanda:<pin>`. In-network `redpanda:9092` (Kafka API); optional console UI.
   Volume `redpanda-data`; port(s) via the slot allocator; source-toggled `REDPANDA_SOURCE`.
   *Rationale:* Kafka-wire-compatible, so the Spark Kafka connector and every scenario transfer 1:1 to
   Apache Kafka later, at a fraction of the footprint — ideal for a lab.
2. **Bake the Spark Kafka connector** (`spark-sql-kafka-0-10` + transitive `kafka-clients`,
   `commons-pool2`) matching **Spark 4.1 / Scala 2.13** into the Spark image, SHA-512-verified — same
   constraint and pattern as **A2** (Spark Connect's classpath is fixed at build; `--packages` at
   session won't work).
3. **Optional CDC sub-item** (for the `cdc_streaming` scenario): a Kafka Connect + **Debezium**
   capability streaming row changes from Supabase Postgres into Redpanda. This is a follow-on; the
   non-CDC streaming scenarios do not need it.

**Acceptance criteria.**
- From Spark Connect: `spark.readStream.format("kafka").option("kafka.bootstrap.servers","redpanda:9092")…`
  runs end to end into an Iceberg sink with checkpoints under `s3a://checkpoints/`.
- *(CDC sub-item)* a Debezium connector publishes Postgres row changes to a Redpanda topic consumable by Spark.

---

## A8 — Track membership  · P1

Update `bootstrapper/tracks.yml` so the `data-eng` track pulls the new services in one command.
Current: `data-eng: [spark, airflow, jupyterhub, zeppelin, minio, weaviate, neo4j]`.
Proposed additions: `iceberg-rest` (P0), `jenkins` (P2), `redpanda` (P3), and `trino` (P3, when they land). Ensure the
corresponding `--<svc>-source` flags exist in `bootstrapper/start.py` for non-interactive launch.

**Acceptance criteria.** `./start.sh --track data-eng --no-tui` brings up the full lakehouse stack
(Spark + Iceberg REST + Zeppelin + Jupyter + Airflow + MinIO, and Jenkins when enabled).

---

## Open questions for the Atlas worker

1. **Iceberg × Spark 4.1.2 compatibility** — is there an Iceberg Spark runtime published for Spark 4.1
   / Scala 2.13? If not, what Spark version should we align on? *(Blocks A2.)*
2. **REST catalog backend** — OK to add an `iceberg` database in Supabase Postgres for a JDBC catalog,
   or do you prefer a different persistence choice?
3. **Airflow submit path** — standalone cluster deploy mode vs. hadoop-aws-in-Airflow for client mode? *(A6.)*
4. **Jenkins in Atlas** — is a first-class `jenkins` service in-scope for Atlas, or would you rather
   `data-eng-lab` run Jenkins as its own `services/_user/` overlay? Either works for us; A5 assumes the
   former per the "upstream the infra" preference.

---

*Maintained by `data-eng-lab`. As the `data-eng-lab` design finalizes, minor additions may appear
(e.g. streaming source, Trino details). Each item above is intended to be filed as an individual Atlas
issue/PR; this file is the umbrella.*
