# data-eng-lab → Atlas: Expectations & Enablement Hand-off

**Audience:** the engineer(s) working on **Atlas** (`thekaveh/atlas`).
**Purpose:** a single, authoritative statement of everything `data-eng-lab` expects from Atlas — what is already **delivered** (so you don't undo it), what is **outstanding** (with build specs), and the **Iceberg/Spark capabilities** the scenario catalog relies on. This supersedes the ad-hoc `docs/atlas-enablement.md` ledger as the hand-off reference; that file remains the terse A1–A9 status table.

`data-eng-lab` consumes Atlas as a **pinned submodule** at `infra/` (currently atlas `85ff46b`) and **never edits it** — enhancements come to you as issues/PRs. Everything below was verified against atlas `72e30d1`, and Atlas's consumer-doc clarifications from **#281** (`85ff46b`) resolved our A7/A9 feedback (see `docs/atlas-feedback-a7a9.md` §F).

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
| **A7** | **Trino query engine** | ✅ **delivered** ([#270](https://github.com/thekaveh/atlas/pull/270)) | — |
| **A8** | `data-eng` track membership | ✅ delivered (#250) | — |
| **A9** | **Redpanda (Kafka API) + Spark kafka connector** | ✅ **delivered** | — |

**Key takeaway:** A1–A9 are now live. **The full scenario catalog can execute** (Spark + Iceberg + Trino + Kafka streaming). See ["Delivered deviations"](#2-delivered-deviations-from-our-a7a9-asks) for the `%trino` interpreter name and topic-seeding notes.

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

### A7 — Trino query engine
- Trino **`trinodb/trino:482`** with Iceberg REST connector pointed at the existing lakehouse catalog.
- Catalog reachable as **`lakehouse.<namespace>.<table>`** (name `lakehouse`).
- Coordinator in-net **`trino:8080`**, host-published on **`TRINO_PORT` (default 63029)**.
- Iceberg connector config: `iceberg.catalog.type=rest`, `iceberg.rest-catalog.uri=http://iceberg-rest:8181`, warehouse `s3://lakehouse/`, `s3.endpoint=http://minio:9000` (path-style), iceberg-scoped MinIO creds.
- Supports **CTAS** (`CREATE TABLE lakehouse.<schema>.<table> AS SELECT …`) for our `federated_query`/`bi_query` scenarios.
- Zeppelin **`%trino` interpreter** (group `jdbc`, JDBC URL `jdbc:trino://trino:8080`, catalog `lakehouse`, user `atlas`, no auth) auto-seeded — no manual UI step. (Note: interpreter name is `trino`, not `jdbc(trino)` — Zeppelin 0.12.1 uses the interpreter name as paragraph prefix.)
- Python/Jupyter client reaches `trino:8080` in-net / `localhost:$TRINO_PORT` from host.
- `--trino-source` flag; member of `data-eng`; default `disabled`.

### A8 — Track & launch
- `data-eng` track = `[spark, airflow, jupyterhub, zeppelin, jenkins, supavisor, minio, iceberg-rest, trino, redpanda, weaviate, neo4j]`. New launch flags `--iceberg-rest-source`, `--jenkins-source`, `--trino-source`, `--redpanda-source` (all default `disabled`). `supavisor` is an optional Postgres pooler (not a lakehouse dependency).

### A9 — Redpanda (Kafka API) + Spark kafka connector
- **`redpandadata/redpanda:v26.1.12`** broker with Kafka API.
- Broker in-net **`redpanda:9092`** (Kafka API port), host-published on **`REDPANDA_KAFKA_PORT` (default 63010)**.
- Optional console on **`REDPANDA_CONSOLE_PORT`** (default 63011).
- Topic-creation seam: `REDPANDA_DEMO_TOPICS` (default `atlas_stream_events`) defines topics pre-created at bootstrap via `rpk topic create` (idempotent). Downstream can override in `.env` to seed their own topics; topics not in that list rely on `auto_create_topics_enabled` (enabled in `dev-container` mode).
- Spark Kafka connector (`org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2`) baked into the single Spark image (used by spark-master, spark-worker, spark-connect, spark-history) with `spark-token-provider-kafka-0-10_2.13:4.1.2`, `kafka-clients:3.9.1`, `commons-pool2:2.12.1` (all SHA-512-verified) — `readStream.format("kafka")` works in **both Spark Connect and standalone with no `--packages`**.
- Streaming checkpoints use the existing **`s3a://checkpoints/`** bucket (MinIO).
- `--redpanda-source` flag; member of `data-eng`; default `disabled`.
- **data-eng-lab consumers:** `scenarios/streaming_ingest-events-spark-iceberg/` (built, live-gated) + `producer.py`; `tests/scenarios/test_streaming_live.py`; roadmap CDC/stateful streaming.

---

## 2. Delivered deviations from our A7/A9 asks

### Divergence 1: Zeppelin interpreter is `%trino`, not `%jdbc(trino)`
We requested `%jdbc(trino)` per older Zeppelin semantics. **Atlas correctly documented** that Zeppelin 0.12.1 uses the interpreter **name** as the paragraph prefix — so the named `trino` interpreter surfaces as **`%trino`**. This is the intended Zeppelin 0.12.1 happy path. Our notebooks now use `%trino`. No Atlas change required; this is better UX.

### Divergence 2: Trino user convention is `atlas` (no auth)
The Trino coordinator (catalog/lakehouse.properties) has **no authenticator configured**; any user string is accepted. Atlas conventions + our examples use **`user='atlas'`** (the convention worker name). Our queries pass this; no auth challenge. Fine — just documenting the convention.

### Divergence 3: Redpanda default only seeds `atlas_stream_events` topic
We asked for a topic-creation seam; Atlas delivered it via `REDPANDA_DEMO_TOPICS` (default `atlas_stream_events`). Our scenario topics (`events`, `online_retail_cdc`, etc.) rely on **`REDPANDA_DEMO_TOPICS` override in `.env`** or Redpanda `auto_create_topics_enabled` (which is on). We've adapted by documenting "run `producer.py` first to auto-create" in scenario READMEs.

---

## 4. Iceberg / Spark capabilities the scenario catalog relies on

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

## 5. Deviations & gotchas (recap — the things that surprised us)

1. **Namespaces are not seeded** — apps/bootstrap must `CREATE NAMESPACE`. (§A1)
2. **Cluster-mode jobs don't inherit the catalog config** — only `spark-connect` has it; DAGs must carry `spark.sql.catalog.lakehouse.*`. (§A2)
3. **Zeppelin is standalone Spark, not Spark Connect.** (§A3)
4. **Warehouse scheme differs** — `s3://lakehouse/` server-side vs `s3a://lakehouse/` for Spark clients. Harmless with S3FileIO, but a footgun; consider documenting.
5. **Jupyter uses the `jupyter`-scoped MinIO account**, which can't write `lakehouse` via PyIceberg. (§A4)
6. **Jenkins ships no jobs/alias** — downstream provides. (§A5)
7. **New services default `disabled`** — must pass `--trino-source container` / `--redpanda-source container` to enable them. Same for `--iceberg-rest-source` / `--jenkins-source`.
8. **Zeppelin Trino interpreter is `%trino`, not `%jdbc(trino)`** — Zeppelin 0.12.1 uses the interpreter name as the paragraph prefix. (§2, Divergence 1.)
9. **Trino user is `atlas` (no auth).** Convention; any user accepted. (§2, Divergence 2.)
10. **Redpanda demo-topic seed only includes `atlas_stream_events`.** Downstream projects override `REDPANDA_DEMO_TOPICS` in `.env` or rely on `auto_create_topics_enabled`. (§2, Divergence 3.)

---

## 6. How we verify your delivery (so you can self-check)

`data-eng-lab` ships an executable form of this contract:
- **`scripts/register_iceberg.py`** — creates `bronze/silver/gold` (must run first; you seed none).
- **`tests/infra/` preflight** — Layer 1 (service existence/health) + Layer 2 (integration matrix: Spark↔MinIO, Spark↔iceberg-rest, Airflow↔MinIO, etc.) via `docker exec` probes.
- **`RUN_INFRA=1 uv run pytest -m infra`** — runs the live probes + scenario execution/parity + the Trino/streaming live tests (`tests/scenarios/test_*_live.py`) once A7/A9 land.
- **`docs/go-live.md`** — the runbook that brings the stack up and drives all of the above.

When you deliver A7/A9, we flip `--trino-source`/`--redpanda-source` on and these gated tests validate the contract end-to-end. If a delivered shape differs from §1–§4, the live tests will point at exactly which expectation broke.

---

**A7/A9 delivery feedback:** See [`docs/atlas-feedback-a7a9.md`](atlas-feedback-a7a9.md) for a detailed feedback report on the delivered Trino + Redpanda services (atlas `72e30d1`), including what matched the contract, the intentional deviations we've adapted to, and optional documentation polish suggestions.

---

*Maintained by `data-eng-lab`. Questions → open an issue on `thekaveh/data-eng-lab` or comment on atlas#268 / atlas#269.*
