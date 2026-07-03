# data-eng-lab — Design Spec

**Date:** 2026-07-02 · **Status:** approved (design) · **Author:** Kaveh + Claude
**Companion doc:** [`docs/atlas-enablement.md`](../../atlas-enablement.md) — the Atlas feature-request contract (A1–A9)

---

## 1. Purpose & philosophy

`data-eng-lab` is a **notebook- and pipeline-driven data-engineering lab** — a curated,
self-contained portfolio of standard and interesting data-engineering scenarios running on a real
lakehouse stack. It is the data-engineering sibling of [`ml-lab`](../../../../ml-lab) and reuses the
[`atlas`](../../../../atlas) platform as its infrastructure, in the same downstream-consumer style as
[`rag-showcase`](../../../../rag-showcase).

Three overlapping roles (mirroring `ml-lab`):

- **Personal lab** — prototype data-eng patterns quickly on a real Spark + Iceberg + MinIO stack.
- **Portfolio** — each scenario reads as a standalone, honest demonstration of a technique.
- **Educational resource** — every scenario carries narrative, ships in **both Scala and PySpark**, and
  (for several) also as a **production Maven JAR** run by Airflow — so a learner sees the same logic
  across interactive, orchestrated, and productionized delivery modes.

Quality bar, inherited from `ml-lab`: flat self-contained units, a config-driven **verifier oracle**,
**comprehensive tiered tests for every artifact**, honest documentation that owns its limitations, and
reproducibility via pinned images/deps and SCALE knobs.

### Goals

1. Launch Atlas's **`data-eng` track** (Spark, Zeppelin, JupyterHub, Airflow, MinIO) as a pinned submodule.
2. Download a curated set of **standard open datasets** into MinIO (`landing` bucket), few-GB scale with knobs.
3. Stand up an **Apache Iceberg lakehouse on MinIO**, cataloged by an **Iceberg REST catalog**, in a
   **medallion** layout (`bronze`/`silver`/`gold`).
4. Ship **Scala Spark scenarios as Zeppelin notebooks** hooked to the Spark engine.
5. Ship the **same scenarios as PySpark Jupyter notebooks** (Scala↔PySpark parity).
6. Ship **curated Airflow DAGs** runnable end-to-end, plus **Maven Scala Spark apps** that build with
   standard conventions, publish their JAR to a MinIO bucket via **Jenkins**, and are run by Airflow.
7. Add more interesting scenarios: Iceberg time-travel/schema-evolution/maintenance, streaming, CDC,
   dimensional modeling, multi-engine query (Trino).
8. **Comprehensive test coverage of all artifacts**, including an **infra preflight** that proves every
   service exists, is initialized, and is properly integrated with its neighbors.

### Non-goals

- Editing Atlas source. All infra gaps are raised as upstream requests (see §3).
- Production-grade security/HA. This is a lab.
- Diff-gating committed notebook outputs (they require the live lakehouse; we test via execution instead).

---

## 2. Key decisions (resolved)

| # | Decision | Choice | Rationale |
|---|---|---|---|
| D1 | Atlas relationship | **Consumer via pinned submodule**; gaps filed as Atlas feature requests (A1–A9), ideally PR'd through the submodule; build against the assumed contract | Keeps `data-eng-lab` a pure consumer; fixes land where they belong (reusable by any Atlas project) |
| D2 | Iceberg catalog | **Iceberg REST catalog service** | Simplest *consumer* experience (one URI); full multi-engine (Spark, PySpark, PyIceberg, Trino); safe commits. Complexity absorbed by Atlas |
| D3 | Data scale | **Few-GB with SCALE knobs** (`tiny`/`small`/`medium`) | Exercises real shuffle/partitioning/compaction; `tiny` keeps CI/e2e fast |
| D4 | Repo layout | **Scenario-centric flat folders** + sibling `spark-apps/` | ml-lab-style; per-scenario Scala/PySpark parity visible in one place |
| D5 | Streaming | **File-source in v1; Redpanda broker as fast-follow** (A9) | v1 unblocked with no new infra; real streaming (windows/CDC) via lightweight Kafka-compatible broker later |
| D6 | Testing | **Comprehensive, tiered, all-artifact coverage + infra preflight** | Explicit quality pillar; preflight = executable form of the Atlas contract |

---

## 3. Architecture & the Atlas contract

### 3.1 How `data-eng-lab` consumes Atlas

Proven by `rag-showcase`:

- **Submodule** `infra/` → `thekaveh/atlas`, pinned to `feat/data-eng-lab-enablement` (carrying A1–A9)
  until those land on Atlas `main`, then re-pinned to a release tag. Atlas source is **never edited**.
- **One overlay** `compose/data-eng-lab.yml`, symlinked by `setup-overlay.sh` into the gitignored
  `infra/services/_user/data-eng-lab/compose.yml` (Atlas auto-discovers `services/_user/*/compose.yml`).
  The overlay merges into existing Atlas services by name: bind-mounts our `scenarios/`, `spark-apps/`,
  and DAGs into Zeppelin/Jupyter/Airflow; points them at the Iceberg catalog + buckets.
- **Config via `infra/.env`** only (idempotent helpers): `PROJECT_NAME=data-eng-lab`, bucket names,
  `ICEBERG_REST_URI`, branding.
- **Launch:** `./infra/start.sh --track data-eng --no-tui …` then health-gate + `docker exec` bootstrap.

### 3.2 The enhanced-Atlas contract (A1–A9)

Enumerated in [`docs/atlas-enablement.md`](../../atlas-enablement.md). Summary:

| ID | Atlas enhancement | Priority |
|----|---|---|
| A1 | Iceberg REST catalog service + `lakehouse`/`jars`/`checkpoints` buckets | P0 |
| A2 | Iceberg Spark runtime jar baked into the Spark image (+ default catalog config) | P0 |
| A3 | Zeppelin Spark interpreter auto-seeded (`spark.remote` + catalog + S3A) | P1 |
| A4 | `boto3`/`s3fs`/`pyiceberg`/`duckdb` in the JupyterHub image | P1 |
| A5 | `jenkins` service (JDK21 + Maven + `mc`, JCasC) | P2 |
| A6 | Airflow as an S3A-capable `spark-submit` client (standalone cluster mode) | P1 |
| A7 | *(stretch)* `trino` query engine over the Iceberg REST catalog | P3 |
| A9 | *(fast-follow)* `redpanda` broker + `spark-sql-kafka` connector jar (+ optional Debezium) | P3 |
| A8 | `data-eng` track updated to include the new services | P1 |

**Sequencing model:** we author `data-eng-lab` against this contract now. The **infra preflight** (§7.1)
flips red→green as Atlas delivers each item. Where an item is trivially bootstrap-shimmable (e.g. A3),
`start-all.sh` applies the effect in the interim — bootstrap actions only, never Atlas source edits.

**Open risks (tracked in the enablement doc):**
- **A2 / Iceberg × Spark 4.1.2 compatibility** — must confirm an Iceberg Spark runtime exists for Spark
  4.1 / Scala 2.13, or align on a Spark version that has one. *Highest-risk item.*
- **A6 submit path** — standalone `--deploy-mode cluster` vs. hadoop-aws-in-Airflow for client mode.

---

## 4. Lakehouse & storage

**Iceberg REST catalog** (`iceberg-rest`, e.g. `apache/iceberg-rest-fixture`) backed by a dedicated
`iceberg` DB in Atlas's Supabase Postgres (durable JDBC catalog), warehouse on `s3a://lakehouse/`. One
catalog shared by Spark (Connect), PySpark, PyIceberg, and later Trino. Default catalog config baked into
Spark (A2), so notebooks need zero catalog boilerplate.

**MinIO buckets** (created idempotently at bootstrap):

| Bucket | Purpose |
|---|---|
| `landing` | raw downloaded datasets, as-is |
| `lakehouse` | Iceberg tables — medallion namespaces `bronze`/`silver`/`gold` |
| `jars` | built Maven JARs (`s3a://jars/<app>/<version>/app.jar`) |
| `checkpoints` | Structured Streaming checkpoints |
| `spark-history` | provided by Atlas |
| `lakehouse-test` | throwaway namespaces for integration tests (isolation) |

**Medallion is the spine:** `landing` → **bronze** (ingested to Iceberg) → **silver** (conformed, deduped,
typed) → **gold** (aggregated marts / star schemas). Iceberg levers the scenarios exploit: hidden
partitioning, `MERGE INTO`, schema evolution, time-travel/snapshots, branch/tag (WAP), and maintenance
(`rewrite_data_files`, `expire_snapshots`, orphan cleanup).

---

## 5. Datasets

`datasets/registry.yaml` (name, source/generator, format, license, per-SCALE params, `landing` prefix,
checksums) drives `scripts/download_datasets.py` (idempotent; uploads to MinIO). SCALE tiers: `tiny`
(CI/e2e), `small` (default dev), `medium` (few-GB target).

| Dataset | Shape it teaches | Format | SCALE dimension | License |
|---|---|---|---|---|
| NYC TLC Taxi | Columnar analytical, time-partitioned | Parquet | # months | Public |
| TPC-H | Benchmark / relational / star-schema | generated → Parquet (DuckDB `dbgen`) | scale factor | Spec; generated OK |
| GH Archive | Semi-structured JSON event stream (+ streaming source) | JSON.gz hourly | # hours/days | Public |
| Online Retail II (UCI) | Transactional retail (upserts, SCD, DQ) | CSV (~1M rows) | fixed/small | Open/UCI |
| MovieLens | Ratings + joins + ML features | CSV | ml-100k → ml-25m | Research-use (downloaded, not redistributed) |
| NOAA GHCN-Daily | Weather time-series + station dim | CSV | # years/stations | Public domain |

GH Archive doubles as the **streaming source** (replay hourly files), so v1 streaming needs no Kafka.

---

## 6. Scenario catalog & Maven apps

### 6.1 Scenario folders (scenario-centric flat)

Named **`[pattern]-[dataset]-[engine]-[format]`**. Each folder is self-contained and carries **both
languages** for parity:

```
scenarios/medallion-nyc_taxi-spark-iceberg/
├── README.md                 # 6-section template (like ml-lab)
├── zeppelin/notebook.zpln    # Scala Spark (Zeppelin, %spark)
├── jupyter/notebook.ipynb    # PySpark (JupyterHub, Spark Connect)
└── dag.py                    # optional Airflow orchestration of this scenario
```

PySpark transformation logic is factored into importable `lib/` modules where useful, so it can be
unit-tested off-cluster. Airflow discovers DAGs by recursively scanning the bind-mounted `scenarios/` +
`spark-apps/` trees (`.py` only) — a scenario's pieces stay co-located.

**Catalog** — ★ = v1 core-10:

| # | Scenario | Teaches | v1 |
|---|---|---|---|
| 1 | `batch_ingest-nyc_taxi-spark-iceberg` | Land Parquet → Iceberg bronze; hidden partitioning | ★ |
| 2 | `medallion-nyc_taxi-spark-iceberg` | bronze→silver→gold spine | ★ |
| 3 | `incremental_upsert-online_retail-spark-iceberg` | `MERGE INTO` / CDC upserts, idempotency | ★ |
| 4 | `scd2-online_retail-spark-iceberg` | Slowly Changing Dimension Type 2 | ★ |
| 5 | `schema_evolution-gh_archive-spark-iceberg` | Add/rename/reorder columns; read old+new | ★ |
| 6 | `time_travel-nyc_taxi-spark-iceberg` | Snapshots, `VERSION AS OF`, rollback, branch/tag (WAP) | ★ |
| 7 | `table_maintenance-nyc_taxi-spark-iceberg` | Compaction, `expire_snapshots`, orphan cleanup | ★ |
| 8 | `json_flatten-gh_archive-spark-iceberg` | Nested JSON → typed columns, explode arrays | ★ |
| 9 | `streaming_ingest-gh_archive-spark-iceberg` | Structured Streaming (file source) → Iceberg + checkpoints, exactly-once | ★ |
| 10 | `star_schema-tpch-spark-iceberg` | Dimensional modeling, fact/dim gold marts | ★ |
| 11 | `data_quality-nyc_taxi-spark-iceberg` | Validation, quarantine table, metrics | roadmap |
| 12 | `sessionization-gh_archive-spark-iceberg` | Window functions, gap-based sessions | roadmap |
| 13 | `join_optimization-tpch-spark-iceberg` | Broadcast vs SMJ, skew, AQE (SCALE-knob perf) | roadmap |
| 14 | `feature_engineering-movielens-spark-iceberg` | ML feature marts (bridges to ml-lab) | roadmap |
| 15 | `bi_query-tpch-trino-iceberg` | Multi-engine read via Trino (needs A7) | roadmap |
| — | `streaming_windows` / `stateful_streaming` / `cdc_streaming` `-gh_archive`/`online_retail` | Windows/watermarks, stateful joins, Debezium CDC (needs A9) | roadmap |

### 6.2 Maven Scala Spark apps + Jenkins CI/CD (req #6)

`spark-apps/<app>/` — standard-convention Maven projects:

```
spark-apps/nyc-taxi-etl/
├── pom.xml                    # Maven · Scala 2.13 · Spark 4.1 + Iceberg (provided) · scalatest · shade
├── Jenkinsfile                # build → test → shade → publish JAR to s3a://jars
├── README.md
├── src/main/scala/com/thekaveh/dataeng/nyctaxi/
│   ├── NycTaxiEtl.scala        # entrypoint (config-driven: landing prefix → catalog table, SCALE arg)
│   └── transforms/…            # pure functions — fast, cluster-free unit tests
├── src/test/scala/…            # scalatest against a local[*] SparkSession
├── src/main/resources/         # application.conf, log4j2.properties
└── dag.py                      # Airflow DAG (SparkSubmitOperator runs the published JAR)
```

Conventions: Spark/Iceberg deps `provided`; `maven-shade-plugin` uber-JAR; pure transforms unit-tested;
config-driven + **idempotent writes** (Iceberg `MERGE`/partition overwrite) so retries/backfills are safe.

**The 3 apps are productionized twins of notebook scenarios** (the through-line):

| App | Production of | Airflow schedule |
|---|---|---|
| `nyc-taxi-etl` | scenario #2 (medallion) | daily |
| `tpch-marts` | scenario #10 (star schema) | on-demand |
| `gh-archive-ingest` | scenario #3/#8 (incremental MERGE) | hourly + backfill |

**End-to-end loop (req #6):**

```
git push ─▶ Jenkins (A5): mvn test → mvn package (shade) → mc cp target/*.jar  s3a://jars/<app>/<ver>/app.jar
Airflow DAG (SparkSubmitOperator, A6) ── spark-submit s3a://jars/<app>/<ver>/app.jar ──▶ Spark cluster
                              reads s3a://landing/… ──▶ writes Iceberg lakehouse.{bronze,silver,gold}
```

Jenkins **job definitions live in this repo** (`jenkins/` — JCasC + a Job-DSL seed job pointing at
`data-eng-lab`), config-as-code; `start-all.sh` triggers the seed job after health-gating. Atlas supplies
only the Jenkins **server** (A5).

---

## 7. Testing & quality (comprehensive)

**Principle:** every artifact is tested, tiered static→unit→integration→e2e; CI split honestly — fast,
cluster-free tests gate every PR; stack-dependent tests run scheduled/dispatch or locally.

### 7.1 Infra preflight & integration matrix (the "stack doctor") — Tier P

`tests/infra/`, run **first** against a live stack, is the **executable form of the A1–A9 contract**.

**Layer 1 — existence & initialization** (declarative expected-service manifest, driven by track + source
flags): each expected service up + healthy **and initialized** — MinIO buckets present; Spark master
healthy **and ≥1 worker registered** + connect/history/init done; Iceberg-REST `/v1/config` responds;
Zeppelin `spark` interpreter **seeded**; Airflow webserver/scheduler/dag-processor + **DB migrated** +
connections seeded; Jenkins JCasC applied + **seed jobs present**; Supabase-Postgres has `iceberg`+`airflow`
DBs; Redpanda/Trino when enabled. Missing expected service → **fail loudly**; intentionally-disabled →
skip *by manifest*.

**Layer 2 — integration / connectivity matrix** — a real **round-trip** per edge:

| Consumer | Must prove |
|---|---|
| Spark | workers registered → distributed job runs · → MinIO `s3a` roundtrip · → Iceberg REST create→insert→select→drop · → Redpanda\* Kafka r/w |
| Zeppelin | → Spark `%spark` paragraph computes (via REST) · → MinIO `s3a` roundtrip · → Iceberg catalog op · → Postgres `%jdbc` |
| Jupyter | → Spark Connect PySpark job · → MinIO `boto3`/`s3fs` roundtrip · → Iceberg `pyiceberg` list/read |
| Airflow | → MinIO `S3Hook` (`minio_default`) · → Spark `spark_default` submit · → Iceberg read · → Postgres |
| Jenkins | → MinIO upload+read to `jars` · → Git checkout · → Maven resolves |
| Iceberg REST | → Postgres catalog persists · → MinIO warehouse metadata written |
| Trino\* | → Iceberg queries a Spark-written table |

\* when A9 / A7 land.

**Mechanics:** runs as `start-all.sh`'s final readiness gate and ahead of CI e2e; `make preflight`
(`make doctor`) on demand; **fail-loud with diagnostics** (prints the matrix + broken edge + remediation
hint); **dependency-aware** (a down service marks its downstream edges *blocked*, not failed); readable +
JSON output. Subsumes the A1–A9 capability checks.

### 7.2 Coverage matrix — every artifact × every layer

| Artifact | Static / structural | Unit (no cluster) | Integration (live stack) | E2E |
|---|---|---|---|---|
| Dataset registry + downloader | YAML-schema; license/field presence | pytest + mocked HTTP/S3 (moto); SCALE math; generator determinism | download tiny → objects in `landing` + checksums | ✓ |
| Compose overlay | `docker compose config`; yamllint; only-known-services | — | overlay merges; bind-mounts resolve | ✓ |
| Bootstrap scripts | shellcheck; `bash -n` | `bats`: arg parse, idempotent `.env`, symlink | live: health-gates, buckets/namespaces/interpreter created, idempotent re-run | ✓ |
| Zeppelin Scala notebooks | `.zpln` JSON valid; sections; no abs paths | — | execute via Zeppelin REST (tiny) → all paragraphs FINISHED; assert table | ✓ |
| Jupyter PySpark notebooks | nbformat valid; sections; no creds | pytest + local SparkSession + `chispa` on `lib/` modules | papermill (tiny) + parametrized assert cell | ✓ |
| Airflow DAGs | DagBag import (0 errors); no cycles; unique task-ids | task-wiring + callable unit tests | `airflow dags test` (tiny) → success + expected output | ✓ |
| Maven Scala apps | scalafmt + scalastyle; pom valid | scalatest transforms + `local[*]` suites; **scoverage ≥ 80%** | build JAR → spark-submit tiny → assert table | ✓ |
| Jenkins pipelines | Jenkinsfile declarative-lint; JCasC/Job-DSL schema | seed-job renders expected jobs (dry-run) | live: build→test→publish JAR to `jars` | ✓ |
| Iceberg tables / data | expected-schema specs | DQ rules on fixtures | data-quality assertions on live tables | ✓ |
| Enhanced-Atlas contract | — | — | preflight Tier P (§7.1) | ✓ |
| Verifier + tooling | — | pytest self-tests | — | — |
| Docs | markdownlint; link + anchor check; table↔folder sync | — | — | — |

### 7.3 Cross-cutting test types (what makes it comprehensive)

- **Cross-language parity** — per scenario, run Scala and PySpark paths (tiny) and assert output Iceberg
  tables equivalent (schema + row count + sorted-row checksum). Directly tests the parity requirement.
- **Idempotency / retry** — re-run every ingest/`MERGE`/SCD2/CDC path; assert stable counts, correct SCD2
  versions, zero dupes.
- **Iceberg-feature assertions** — time-travel returns prior snapshot; schema-evolution reads old+new;
  compaction reduces file count; `expire_snapshots` removes snapshots; hidden-partition pruning.
- **Data-quality assertions** — PK uniqueness, no nulls in keys, fact→dim referential integrity, row
  ranges, partition layout. (Doubles as the `data_quality` scenario engine.)
- **Streaming semantics** — file-source (v1): drop batches → assert rows, then kill/restart to prove
  exactly-once checkpoint recovery. Redpanda (fast-follow): windows/watermarks/late-data; CDC propagation.

### 7.4 Test data, isolation, gating, tooling

- **Fixtures:** deterministic tiny slices — some committed (≈100 taxi rows, TPC-H SF0.01, a few GH events)
  so unit tests need no download; larger tiny sets generated for integration.
- **Isolation:** integration tests write to throwaway `lakehouse.test_<runid>` / `lakehouse-test` and tear down.
- **Tooling:** scalatest + scoverage; pytest + pytest-cov + chispa + moto; papermill; bats; shellcheck;
  yamllint; hadolint; markdownlint + link-checker; Jenkins declarative-linter. Coverage thresholds enforced.
- **CI split:** PR gate = Tier 0 + Tier 1 (static + unit, no Docker). Scheduled/dispatch = Tier P + 2 + 3.
  Local: `make test` (0+1), `make preflight`, `make test-int` (2), `make smoke` (3).
- **Repo impact:** `tests/{unit,contract→infra,integration,e2e,fixtures}/` + each app's `src/test/scala/` +
  `tests/conftest.py` with shared SparkSession/stack fixtures.

---

## 8. Repo structure & tooling

```
data-eng-lab/
├── README.md · CONTRIBUTING.md · CHANGELOG.md · LICENSE · Makefile · pyproject.toml · .python-version
├── .gitmodules                 # infra → thekaveh/atlas (feat branch, later a tag)
├── infra/                      # Atlas submodule (never edited)
├── compose/data-eng-lab.yml    # THE overlay → symlinked into infra/services/_user/data-eng-lab/
├── datasets/registry.yaml      # + adapters/generators
├── scenarios/<pattern>-<dataset>-<engine>-<format>/   # README + zeppelin/*.zpln + jupyter/*.ipynb [+ dag.py]
├── spark-apps/<app>/           # Maven project + Jenkinsfile + dag.py
├── jenkins/                    # JCasC + seed job (CI-as-code)
├── scripts/                    # setup-overlay · start-all · stop-all · download_datasets · register_iceberg
│                               #   · seed_zeppelin_interpreter (interim A3 shim) · verify_repo.py
├── tests/                      # infra(preflight) · unit · integration · e2e · fixtures · conftest.py
├── docs/                       # atlas-enablement.md · architecture(+diagram) · datasets · scenarios · env-setup · superpowers/
└── .github/workflows/ci.yml
```

- **Verifier** (`scripts/verify_repo.py`, config-driven via `verify_repo_config.yaml`): scenario naming
  convention; each scenario has a 6-section README + `zeppelin/notebook.zpln` + `jupyter/notebook.ipynb`;
  notebook JSON valid; DAGs parse; every dataset a scenario references exists in `registry.yaml`; each
  spark-app has `pom.xml`/`src/main/scala`/`Jenkinsfile`/`dag.py`; README scenario-table ↔ folders. Exit 0
  ⇔ no errors. Has pytest self-tests.
- **Makefile:** `setup` · `up`/`down` · `datasets` · `verify` · `test` · `preflight` · `test-int` · `smoke`
  · `lint` (ruff + scalafmt) · `build-apps` (mvn) · `fmt`.
- **CI** (`.github/workflows/ci.yml`): PR = static + unit (fast, no Docker); scheduled/dispatch = full
  stack at SCALE=tiny → preflight → scenario/DAG/app/parity/DQ/streaming suites. Honestly flagged as
  resource-heavy; primary gate is static+unit.
- **Docs:** plain markdown (like ml-lab) + one architecture diagram; per-scenario 6-section READMEs;
  `docs/scenarios.md` master table; `docs/atlas-enablement.md` is the Atlas contract.

---

## 9. Implementation phasing

Authored against the assumed contract; the infra preflight flips green as Atlas delivers each item.

| Phase | Deliverables | Atlas dep | Exit criteria |
|---|---|---|---|
| **0 — Foundation & harness** | Repo + private GitHub repo created & pushed; submodule; overlay + `setup-overlay`; `start-all`/`stop-all`; bucket creation; base tooling (Makefile, uv/ruff, verifier + CI skeletons, README/CONTRIBUTING/CHANGELOG); preflight **Layer 1** | data-eng track boots | `make up` healthy; preflight L1 green; static CI green |
| **1 — Lakehouse + integration + data** | `register_iceberg.py`; preflight **Layer 2** matrix; `registry.yaml` + `download_datasets.py` + fixtures; dataset tests | A1, A2, A3, A4, A6, A8 | preflight L2 green; `make datasets` lands data |
| **2 — Core notebook scenarios** | ★ core-10, each Zeppelin+Jupyter+README(+DAG); full per-scenario test coverage incl. parity | A1–A4 | core-10 pass Tier 0/1 (PR) + Tier 2 (stack); parity green |
| **3 — Maven apps + Jenkins CI/CD** | `spark-apps` (3) + scalatest/shade; `jenkins/` JCasC + seed + Jenkinsfiles → publish JAR; app DAGs | A5, A6 (+A1/A2) | Jenkins publishes a JAR; DAG spark-submits it e2e |
| **4 — Orchestration + full e2e** | recursive DAG mount; wire DAGs; full e2e smoke (tiny) via DAG *and* JAR; scheduled CI e2e | A1–A6, A8 | `make smoke` green; nightly e2e green |
| **5 — Fast-follow / stretch** | Redpanda streaming theme; Trino `bi_query` + preflight edge; roadmap scenarios | A9, A7 | streaming/CDC green; Trino reads Spark tables |
| **6 — Docs & polish** | Full README; `architecture.md` + diagram; `datasets/scenarios/env-setup` docs; branding; final hardening | — | verifier + link-check green; repo curated |

**Critical path:** 0 → 1 (A1/A2) → 2 → 3 (A5/A6) → 4. Phase 5 rides on A7/A9. Phase 2 scenarios
parallelize once Phase 1's lakehouse + preflight are green.

---

## 10. Risks & open questions

1. **Iceberg × Spark 4.1.2** (A2) — confirm a matching Iceberg Spark runtime exists, or align Spark version. *Highest risk; blocks the lakehouse.*
2. **Airflow submit path** (A6) — standalone cluster deploy mode vs. hadoop-aws-in-Airflow client mode.
3. **Full-stack e2e in CI** — resource-heavy; may run nightly/dispatch or locally rather than per-PR.
4. **MovieLens license** — research-use; we download at runtime and never redistribute.
5. **External dependency on Atlas delivery** — Phases 1+ go fully green only as A1–A9 merge; preflight makes the gap explicit at all times.

---

## 11. References

- `docs/atlas-enablement.md` — the A1–A9 Atlas contract (companion to this spec).
- `atlas` — `docs/deployment/reusing-atlas.md`, `bootstrapper/tracks.yml`, `services/{spark,zeppelin,airflow,minio,jupyterhub}/`, `docs/research/candidates/iceberg-duckdb.md`.
- `rag-showcase` — `docs/atlas-reuse-assessment.md`, `scripts/start-all.sh`, `compose/rag-overlay.yml` (the downstream-consumer pattern).
- `ml-lab` — `scripts/verify_repo.py` + `verify_repo_config.yaml`, `tests/nnx_surface/`, per-task README template (the quality bar).
