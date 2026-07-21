# Atlas Enablement Requests — for the `data-eng-lab` project

**Status:** draft contract `v0.2` · **Consumer repo:** `data-eng-lab` (private) · **Target:** [`thekaveh/atlas`](https://github.com/thekaveh/atlas)

> **➡️ The authoritative hand-off is now [`atlas-expectations.md`](atlas-expectations.md)** — it reflects the delivered reality (A1–A9, all delivered). This file remains the terse A1–A9 origin ledger.

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

data-eng-lab consumes Atlas through one committed **`atlas.consumer.yml`** at the
repo root (Atlas's consumer contract — `infra/docs/deployment/reusing-atlas.md` §6.1).
The manifest declares the project name, brand, env values (including `BASE_PORT: auto`
and the nine containerized `*_SOURCE` selections), the compose overlay
(`compose/data-eng-lab.yml`, appended to Atlas's compose invocation — no symlink into
`infra/services/_user/`), and the `lakehouse-test` bucket (provisioned by Atlas's
minio-init with scoped credentials). `scripts/start-all.sh` is a thin wrapper:
stale-symlink cleanup → `env backfill` → consumer `doctor` → `start.sh --consumer
… --track data-eng --no-tui --detach` → namespace registration → preflight.
Atlas materializes `infra/.env` from the manifest on every start; nothing in this
repo writes to it.

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

## A1 — Iceberg REST catalog service + lakehouse buckets   · P0 · **DELIVERED**

**Delivered shape:**
- Service: `apache/iceberg-rest-fixture:1.10.1` (Postgres-JDBC layer → Supabase `iceberg` DB).
- Catalog: `lakehouse` with warehouse `s3://lakehouse/` (server-side).
- Port: `ICEBERG_REST_PORT=63020` (host port via slot allocator).
- Buckets: `lakehouse`, `jars`, `checkpoints` created at bootstrap.
- **Deviation:** No namespaces pre-seeded; Atlas init creates the catalog only. Namespaces (`bronze`, `silver`, `gold`) are created at go-live by `scripts/register_iceberg.py` (host-side, see go-live runbook).

---

## A2 — Iceberg Spark runtime on the Spark image   · P0 · **DELIVERED**

**Delivered shape:**
- Spark version: **4.1.2** (base image `apache/spark:4.1.2`).
- Iceberg runtime: **`iceberg-spark-runtime-4.1_2.13:1.11.0`** + `iceberg-aws-bundle-1.11.0` + `hadoop-aws-3.4.2` (all SHA-512 verified, baked at image build).
- Default catalog config injected into Spark Connect server conf:
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

---

## A3 — Zeppelin Spark interpreter auto-seeded   · P1 · **DELIVERED, with deviation**

**Delivered shape:**
- Zeppelin uses **standalone `spark.master=spark://spark-master:7077` (client mode)**, NOT Spark Connect (`spark.remote`).
   - Rationale: Spark 4 rejects mixing Connect and standalone master in a single interpreter; the default `%spark` Scala uses the standalone master for notebook data locality.
   - **Implication:** Zeppelin notebooks author against a private Spark session (per notebook), not the shared Connect server. This is acceptable for development scenarios and decouples Zeppelin from the Connect server's fixed classpath.
- Interpreter auto-seeded at provision via Zeppelin REST API; JDBC + S3A properties included.
- **Deviation:** No Spark Connect bridging in Zeppelin. Authors should use Jupyter (Connect + PySpark) for shared-session scenarios.

---

## A4 — Data libraries in the JupyterHub image   · P1 · **DELIVERED**

**Delivered shape:**
- Libraries: `boto3`, `s3fs`, `pyiceberg[s3fs]`, `pyarrow`, `duckdb` all baked into the image.
- Spark Connect client: `pyspark-client==4.1.2` + `SPARK_REMOTE=sc://spark-connect:15002` (auto-configured).
- MinIO auto-configured: Jupyter user account `jupyter`-scoped (separate from root); `boto3`/`s3fs` auto-discover MinIO endpoint.
- PyIceberg auto-configured: `ICEBERG_REST_URI=http://iceberg-rest:8181` pre-set.

---

## A5 — Jenkins CI service   · P2 · **DELIVERED**

**Delivered shape:**
- Base: `jenkins/jenkins:lts-jdk21` with Maven + `mc` + JCasC + plugin set (pipeline, git, config-as-code, job-dsl).
- Port: `JENKINS_PORT=63080` (host-published; container-internal port is `8080`). Agent port `50000`.
- Credentials: auto-generated `JENKINS_ADMIN_PASSWORD` + MinIO endpoint + creds pre-configured.
- Source-toggled: `JENKINS_SOURCE` flag (default `disabled`).
- **Scope:** Atlas provides server + JCasC seam only. No job definitions shipped; `data-eng-lab` injects via `jenkins/seed-job.sh` (JCasC/Job-DSL).
- **Deviation:** No jobs/aliases shipped upstream; seed job and Jenkinsfiles authored by `data-eng-lab`.

---

## A6 — Airflow as an S3A-capable `spark-submit` client   · P1 · **DELIVERED**

**Delivered shape:**
- Airflow image (3.2.2): `apache-spark` + `amazon` providers + `pyspark-client==4.1.2`.
- Connections: `spark_default` (`spark://spark-master:7077`) + `minio_default` (MinIO endpoint + creds).
- S3A capability: `hadoop-aws-3.4.2` + AWS SDK baked into Airflow's pyspark jars dir (mirrors A2).
- Deploy mode: **`--deploy-mode cluster`** (driver runs on a worker; only `spark-submit` needed on Airflow).
- Reference smoke DAG provided.

---

## A7 — *(stretch)* Trino query engine over Iceberg REST   · P3 · **DELIVERED**

**Delivered shape:**
- Service: `trinodb/trino:482` with Iceberg REST connector pointed at the `lakehouse` catalog (`iceberg-rest:8181`).
- Catalog: `lakehouse`; warehouse `s3://lakehouse/`; supports CTAS.
- Port: `TRINO_PORT=63029` (host port via slot allocator); in-net `trino:8080`.
- Zeppelin interpreter: **`%trino`** (group `jdbc`, auto-seeded; note: Atlas correctly uses interpreter name as prefix, not `%jdbc(trino)`). JDBC URL `jdbc:trino://trino:8080`, catalog `lakehouse`, user `atlas` (no auth).
- Python client: reaches `localhost:$TRINO_PORT` from host.
- Member of `data-eng` track; `--trino-source` flag (default `disabled`).
- Consumers: `scenarios/federated_query-nyc_taxi-trino-iceberg/`, `bi_query-tpch-trino-iceberg` (roadmap), live tests `tests/scenarios/test_trino_query_live.py`.
- **Deviation:** Interpreter is `%trino`, not `%jdbc(trino)` (Zeppelin 0.12.1 semantics; also no auth, user convention `atlas`). See [`atlas-feedback-a7a9.md`] for the full delivery feedback.

---

## A9 — *(fast-follow)* Redpanda broker for streaming   · P3 · **DELIVERED**

**Delivered shape:**
- Service: `redpandadata/redpanda:v26.1.12` (Kafka-API compatible broker).
- Broker: in-net `redpanda:9092`; host port `REDPANDA_KAFKA_PORT=63010`.
- Optional console: `REDPANDA_CONSOLE_PORT=63011`.
- Topic seeding: `REDPANDA_DEMO_TOPICS` (default `atlas_stream_events`) creates topics at bootstrap via `rpk topic create`. Downstream can override in `.env`.
- Spark Kafka connector: `org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2` (+ dependencies) SHA-512-verified and baked into the single Spark image (master/worker/connect/history) — `readStream.format("kafka")` works with no `--packages`.
- Checkpoints: use existing `s3a://checkpoints/` bucket (MinIO).
- Member of `data-eng` track; `--redpanda-source` flag (default `disabled`).
- Consumers: `scenarios/streaming_ingest-events-spark-iceberg/`, `producer.py` (auto-creates topics), live tests `tests/scenarios/test_streaming_live.py`.
- **Note:** Demo-topic default is only `atlas_stream_events`; projects override `REDPANDA_DEMO_TOPICS` or rely on `auto_create_topics_enabled`. See [`atlas-feedback-a7a9.md`] for the full delivery feedback.

---

## A8 — Track membership   · P1 · **DELIVERED**

**Delivered shape:**
- `data-eng` track members: `spark`, `airflow`, `jupyterhub`, `zeppelin`, `minio`, `iceberg-rest`, `jenkins`, `supavisor`, `weaviate`, `neo4j`.
- New source-toggle flags: `--iceberg-rest-source` and `--jenkins-source` (both default `disabled`).
- Enables: `./scripts/start-all.sh` launches the full lakehouse stack (~20+ containers).

---

## Key deviations from our original assumptions

1. **Namespaces not pre-seeded:** Atlas init creates the Iceberg catalog (`lakehouse`) but no namespaces (`bronze`, `silver`, `gold`). They are created at go-live by `scripts/register_iceberg.py` (host-side), a one-time setup step before live validation (see `go-live.md`).

2. **Zeppelin uses standalone Spark, not Spark Connect:** The seeded `%spark` Scala interpreter uses `spark.master=spark://spark-master:7077` (client mode, private to the notebook session) instead of Spark Connect. Rationale: Spark 4 rejects mixing Connect and standalone in a single interpreter. PySpark users should use Jupyter (Connect + `pyspark-client`) for shared-session semantics.

3. **Warehouse scheme: `s3://` server-side, `s3a://` client-side:** The Iceberg REST catalog is seeded with `warehouse=s3://lakehouse/` (server-side, used by the REST API for metadata writes), while Spark clients submit `s3a://lakehouse/` (client-side, using hadoop-aws). This is a deliberate split: the server speaks native S3, clients speak S3A. Both resolve to the same MinIO bucket.

4. **Jenkins ships no jobs:** Atlas provides the Jenkins server + JCasC seam only. All job definitions, Jenkinsfiles, and seed job logic are authored by `data-eng-lab` and injected via `jenkins/seed-job.sh` (JCasC/Job-DSL). No seed job alias is baked upstream.

---

## Open questions for the Atlas worker

1. **Iceberg × Spark 4.1.2 compatibility** — is there an Iceberg Spark runtime published for Spark 4.1 / Scala 2.13? If not, what Spark version should we align on? *(Blocks A2.)*
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
