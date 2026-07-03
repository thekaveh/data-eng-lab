# data-eng-lab → Atlas: Expectations & Enablement Hand-off

**Audience:** the engineer(s) working on **Atlas** (`thekaveh/atlas`).
**Purpose:** a single, authoritative statement of everything `data-eng-lab` expects from Atlas — what is already **delivered** (so you don't undo it), what is **outstanding** (with build specs), and the **Iceberg/Spark capabilities** the scenario catalog relies on. This supersedes the ad-hoc `docs/atlas-enablement.md` ledger as the hand-off reference; that file remains the terse A1–A9 status table.

`data-eng-lab` consumes Atlas as a **pinned submodule** at `infra/` (currently atlas `62eb6df`) and **never edits it** — enhancements come to you as issues/PRs. Everything below was verified against atlas `62eb6df` unless marked OUTSTANDING.

---

## 0. TL;DR status

| Item | Capability | Status | Blocks |
|---|---|---|---|
| **A1** | Iceberg REST catalog + buckets | ✅ delivered (#245) | — |
| **A2** | Spark 4.1.2 + Iceberg runtime on the cluster | ✅ delivered (#246) | — |
| **A3** | Zeppelin Spark interpreter seeding | ✅ delivered | — |
| **A4** | JupyterHub lakehouse Python clients | ✅ delivered | — |
| **A5** | Jenkins Maven Spark builder | ✅ delivered (#253) | — |
| **A6** | Airflow S3A spark-submit path | ✅ delivered (#249) | — |
| **A7** | **Trino query engine** | 🔴 **OUTSTANDING** — [atlas#268](https://github.com/thekaveh/atlas/issues/268) | `federated_query-*-trino`, `bi_query-tpch-trino` |
| **A8** | `data-eng` track membership | ✅ delivered (#250) | — |
| **A9** | **Redpanda (Kafka API) + Spark kafka connector** | 🔴 **OUTSTANDING** — [atlas#269](https://github.com/thekaveh/atlas/issues/269) | `streaming_ingest-events` (Kafka), CDC/stateful streaming |

**Key takeaway:** A1–A6 + A8 are live. **Most of the remaining scenario catalog needs only what's already delivered** (Spark + Iceberg). Only the **Trino** scenarios (A7) and the **Kafka**-based streaming scenarios (A9) are blocked — and note that **file-source** Structured Streaming (`streaming_ingest-gh_archive`) needs *neither* (it reads landing files, not Kafka).

---

## 1. Delivered capabilities — confirmed reality (do not regress)

### A1 — Iceberg REST catalog + buckets
- Service `iceberg-rest`: base `apache/iceberg-rest-fixture:1.10.1`, locally rebuilt to layer the **Postgres JDBC driver** (`ICEBERG_REST_POSTGRES_JDBC_VERSION`, default `42.7.12`) onto the classpath. In-network `http://iceberg-rest:8181`; host-published on **`ICEBERG_REST_PORT` (default 63020)**.
- Backing store: **JDBC catalog → Supabase Postgres** (`jdbc:postgresql://supabase-db:5432/iceberg`), seeded by `iceberg-rest-init` (creates the `iceberg` DB + role). Catalog **name `lakehouse`** (a client concern; every Atlas client uses `lakehouse`).
- Warehouse: **`s3://lakehouse/`** server-side (S3FileIO with `s3.endpoint=http://minio:9000`, path-style). Spark clients address it as **`s3a://lakehouse/`**.
- MinIO buckets created by `minio-init` under a scoped account (`MINIO_ICEBERG_ACCESS_KEY`/`_SECRET_KEY`): **`landing`, `lakehouse`, `jars`, `checkpoints`** (+ `spark-history` from `spark-init`). `data-eng-lab` adds only its own `lakehouse-test`.
- **GOTCHA — no namespaces are seeded.** `bronze`/`silver`/`gold` do NOT exist on a fresh stack. `data-eng-lab` creates them host-side via `scripts/register_iceberg.py`; apps also self-create (`CREATE NAMESPACE IF NOT EXISTS`). Please keep it this way (app-owned namespaces) unless you decide to seed them — if you seed, tell us.

### A2 — Spark + Iceberg runtime
- **Spark `4.1.2`, Scala `2.13`.** Baked into `/opt/spark/jars` (SHA-verified): **`iceberg-spark-runtime-4.1_2.13:1.11.0`**, `iceberg-aws-bundle:1.11.0`, `hadoop-aws:3.4.2`, AWS SDK v2 `bundle:2.29.52`.
- **Spark Connect** at `sc://spark-connect:15002` (backend-only). Standalone **master** `spark://spark-master:7077`, N workers, history server.
- The full catalog config (`spark.sql.extensions=…IcebergSparkSessionExtensions`, `spark.sql.catalog.lakehouse.*` = rest/uri/warehouse/S3FileIO/creds/region) is injected into the **`spark-connect`** service.
- **GOTCHA — standalone master/workers do NOT inherit that catalog config.** Cluster-mode `spark-submit` jobs (i.e. Airflow) must pass `spark.sql.catalog.lakehouse.*` themselves. `data-eng-lab` does this in its DAGs (copied from your reference `services/airflow/dags/lakehouse_spark_submit_smoke.py`) and has a CI guard test enforcing it. If you ever make the master/workers carry the catalog defaults, that would simplify our DAGs — but is not required.

### A3 — Zeppelin
- The `spark` interpreter is auto-seeded (init container merges Atlas-owned properties via REST). **GOTCHA — Zeppelin uses standalone `spark.master=spark://spark-master:7077` (client mode), NOT Spark Connect** (`spark.remote` is explicitly removed; Spark 4 rejects mixing). Our `%spark` Scala notebooks rely on this seeded interpreter.
- **We now also need `%jdbc(trino)`** for the Trino scenarios — see A7. Today the JDBC interpreter is a manual one-time UI step per your README; A7 should seed it.

### A4 — JupyterHub
- Image adds `boto3`, `s3fs`, **`pyiceberg[s3fs]==0.11.1`**, `duckdb`, `pyarrow` (+ `pyspark-client==4.1.2`). Env: `SPARK_REMOTE=sc://spark-connect:15002`, `ICEBERG_REST_URI`, PyIceberg auto-config.
- **GOTCHA — Jupyter uses the `jupyter`-scoped MinIO account**, not the iceberg one. Direct PyIceberg *writes* to `lakehouse` may be denied; the Spark-Connect path (correctly scoped) is the write path our notebooks use. If you can grant the jupyter account read on `lakehouse`, our Jupyter read demos get simpler.

### A5 — Jenkins
- `jenkins/jenkins:lts-jdk21` + Maven + MinIO `mc`. JCasC only; **no seed jobs, no `mc` alias shipped** (by design — "downstream provides jobs"). Host port **`JENKINS_PORT` (default 63080)**; admin `JENKINS_ADMIN_USER`/`_PASSWORD`.
- Publish contract (documented in your `services/jenkins/README.md`): `mc alias set atlas "$MINIO_ENDPOINT" "$MINIO_ICEBERG_ACCESS_KEY" "$MINIO_ICEBERG_SECRET_KEY"` then `mc cp target/*.jar "atlas/$MINIO_BUCKET_ICEBERG_JARS/<app>/<version>/app.jar"`. `data-eng-lab` supplies its own job via `jenkins/seed-job.sh` (REST create-item) + `jenkins/nyc-taxi-etl-job.xml`. **No change requested** — this split works; just don't start shipping conflicting jobs.

### A6 — Airflow
- `apache/airflow:3.2.2` with **`hadoop-aws` + the Iceberg runtime baked** (client or cluster submit). Providers: apache-spark, amazon. Default `ATLAS_LAKEHOUSE_SPARK_DEPLOY_MODE=cluster`.
- Connections seeded when the sources are `container`: **`spark_default`** (`spark://spark-master:7077`, extra `deploy-mode: cluster`) and **`minio_default`** (aws, `http://minio:9000`). Reference DAG `dags/lakehouse_spark_submit_smoke.py` — our DAGs mirror its `conf`.

### A8 — Track & launch
- `data-eng` track = `[spark, airflow, jupyterhub, zeppelin, jenkins, supavisor, minio, iceberg-rest, weaviate, neo4j]`. New launch flags `--iceberg-rest-source`, `--jenkins-source` (default `disabled`). `supavisor` is an optional Postgres pooler (not a lakehouse dependency).

---

## 2. OUTSTANDING — please build these

### A7 — Trino query engine  ([atlas#268](https://github.com/thekaveh/atlas/issues/268))
A `trino` service in the `data-eng` track that federates SQL over the **existing** Iceberg lakehouse. Exact expectations:
- Trino with the **Iceberg connector** pointed at the existing REST catalog:
  `iceberg.catalog.type=rest`, `iceberg.rest-catalog.uri=http://iceberg-rest:8181`, warehouse `s3a://lakehouse/`, `s3.endpoint=http://minio:9000` (path-style), iceberg-scoped MinIO creds.
- Catalog reachable as **`lakehouse.<namespace>.<table>`** (name `lakehouse`).
- Coordinator in-net **`trino:8080`**, host-published (`TRINO_PORT`). Must support **CTAS** (`CREATE TABLE lakehouse.gold.x AS SELECT …`) writing Iceberg — our `federated_query` scenario writes a gold table.
- **Seed the Zeppelin `%jdbc(trino)` interpreter** (JDBC URL `jdbc:trino://trino:8080`, catalog `lakehouse`) so `%jdbc(trino)` works with no manual UI step. The `trino` Python client (used in our Jupyter notebook) should reach `trino:8080` in-net / `localhost:$TRINO_PORT` from host.
- `--trino-source` flag; member of `data-eng`; default `disabled`.
- **data-eng-lab consumers:** `scenarios/federated_query-nyc_taxi-trino-iceberg/` (built, live-gated) and the roadmap `bi_query-tpch-trino-iceberg`; `tests/scenarios/test_trino_query_live.py`.

### A9 — Redpanda (Kafka API) + Spark kafka connector  ([atlas#269](https://github.com/thekaveh/atlas/issues/269))
A `redpanda` broker + the Spark Kafka connector, so Structured Streaming can read a topic and write Iceberg. Exact expectations:
- `redpanda` broker in-net **`redpanda:9092`** (Kafka API) + host port (`REDPANDA_KAFKA_PORT`); optional console.
- A **topic-creation seam** (init container / `rpk topic create` / auto-create) for demo topics (e.g. `events`).
- **`org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2`** (+ `kafka-clients`, `commons-pool2`, token-provider) baked into the Spark image `/opt/spark/jars` (SHA-verified), so `spark.readStream.format("kafka")` works in **both Spark Connect and standalone** with no `--packages`.
- Streaming checkpoints use the existing **`checkpoints`** bucket (`s3a://checkpoints/`).
- `--redpanda-source` flag; member of `data-eng`; default `disabled`.
- **data-eng-lab consumers:** `scenarios/streaming_ingest-events-spark-iceberg/` (built, live-gated) + `producer.py`; `tests/scenarios/test_streaming_live.py`; roadmap CDC/stateful streaming.

---

## 3. Iceberg / Spark capabilities the scenario catalog relies on

These should already be satisfied by **A2** (Spark 4.1.2 + Iceberg 1.11.0 runtime + `IcebergSparkSessionExtensions`). Listed so you can **verify** them when the stack comes up — each maps to a scenario. **None of these need new Atlas services** (they need only the delivered Spark/Iceberg):

| Capability | Needed by | Provided by | Verify |
|---|---|---|---|
| `MERGE INTO` (upserts/CDC) | `incremental_upsert`, `scd2` (online_retail) | Iceberg SparkSQL extension | `MERGE INTO lakehouse.silver.x …` runs |
| Snapshots / `VERSION AS OF` / rollback | `time_travel` (nyc_taxi) | Iceberg + `system.rollback_to_snapshot` | `SELECT … VERSION AS OF <id>`; `CALL lakehouse.system.rollback_to_snapshot(...)` |
| **Branch/tag (WAP)** | `time_travel` | Iceberg 1.11 branches; `spark.wap.branch` | `ALTER TABLE … CREATE BRANCH`; write to branch |
| Maintenance procedures | `table_maintenance` (nyc_taxi) | Iceberg `system.*` procedures | `CALL lakehouse.system.rewrite_data_files / expire_snapshots / remove_orphan_files` |
| Schema evolution (add/rename/reorder) | `schema_evolution` (gh_archive) | Iceberg | `ALTER TABLE … ADD/RENAME/ALTER COLUMN`; read old+new snapshots |
| Nested JSON → columns, `explode` | `json_flatten` (gh_archive) | Spark SQL | `from_json`/`explode` on GH Archive events |
| **Structured Streaming (file source)** | `streaming_ingest-gh_archive` | Spark (no Kafka) | `readStream.format("json").load("s3a://landing/…")` → Iceberg + checkpoint |
| Dimensional / star schema | `star_schema` (tpch) | Spark SQL | fact/dim gold marts |
| Multi-engine read | `federated_query`, `bi_query` | **A7 (Trino)** | see §2 |

> **Action for the worker:** confirm the `system.*` Iceberg procedures (`rewrite_data_files`, `expire_snapshots`, `remove_orphan_files`, `rollback_to_snapshot`) and **branch/tag / WAP** are usable through the seeded Zeppelin interpreter and Spark Connect. If any require the `SparkSessionCatalog` (session-catalog) wiring rather than the `SparkCatalog` you configured, please note it — a couple of maintenance/time-travel scenarios depend on it.

---

## 4. Deviations & gotchas (recap — the things that surprised us)

1. **Namespaces are not seeded** — apps/bootstrap must `CREATE NAMESPACE`. (§A1)
2. **Cluster-mode jobs don't inherit the catalog config** — only `spark-connect` has it; DAGs must carry `spark.sql.catalog.lakehouse.*`. (§A2)
3. **Zeppelin is standalone Spark, not Spark Connect.** (§A3)
4. **Warehouse scheme differs** — `s3://lakehouse/` server-side vs `s3a://lakehouse/` for Spark clients. Harmless with S3FileIO, but a footgun; consider documenting.
5. **Jupyter uses the `jupyter`-scoped MinIO account**, which can't write `lakehouse` via PyIceberg. (§A4)
6. **Jenkins ships no jobs/alias** — downstream provides. (§A5)
7. **New services default `disabled`** — must pass `--iceberg-rest-source container` / `--jenkins-source container` (and future `--trino-source` / `--redpanda-source`).

---

## 5. How we verify your delivery (so you can self-check)

`data-eng-lab` ships an executable form of this contract:
- **`scripts/register_iceberg.py`** — creates `bronze/silver/gold` (must run first; you seed none).
- **`tests/infra/` preflight** — Layer 1 (service existence/health) + Layer 2 (integration matrix: Spark↔MinIO, Spark↔iceberg-rest, Airflow↔MinIO, etc.) via `docker exec` probes.
- **`RUN_INFRA=1 uv run pytest -m infra`** — runs the live probes + scenario execution/parity + the Trino/streaming live tests (`tests/scenarios/test_*_live.py`) once A7/A9 land.
- **`docs/go-live.md`** — the runbook that brings the stack up and drives all of the above.

When you deliver A7/A9, we flip `--trino-source`/`--redpanda-source` on and these gated tests validate the contract end-to-end. If a delivered shape differs from §2, the live tests will point at exactly which expectation broke.

---

*Maintained by `data-eng-lab`. Questions → open an issue on `thekaveh/data-eng-lab` or comment on atlas#268 / atlas#269.*
