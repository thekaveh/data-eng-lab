# NYC Taxi Data Quality Production Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a snapshot-bound NYC Taxi quality Spark application and serialized Airflow DAG that create a null-safe Silver partition, durable governed Gold facts, and a fixed live-validated Trino query surface.

**Architecture:** One Maven Scala application validates one stable Bronze Iceberg snapshot, deterministically replaces clean and quarantine tables, then idempotently MERGEs a complete eight-signal fact set. One daily Airflow DAG waits for the exact same-logical-date ETL task and submits the JAR through Atlas's REST-confirming Spark operator; fixed SELECT-only Trino files expose latest, trend, and operator-attention views.

**Tech Stack:** Scala 2.13.14, Spark 4.1.2, Iceberg REST/S3A, Maven/ScalaTest, Airflow 3.3 with `ExternalTaskSensor` and Spark provider 5.6.0, Atlas `RestConfirmingSparkHook`, Trino 482, pytest, Jenkins, MkDocs/wiki projections.

## Global Constraints

- Work only on `codex/91-nyc-taxi-data-quality` from develop `774db8fb825f4f36e6c9dfb2fff69579ea2d34ff`; do not push or open a PR before independent reviews.
- Do not modify `uv.lock`, `datasets/registry.yaml`, the `infra` gitlink or nested Atlas files, or the protected untracked `docs/superpowers/plans/2026-07-21-atlas-submodule-modernization.md`.
- Do not implement upstream five-key Bronze hardening. `lakehouse.bronze.nyc_taxi_trips` remains explicitly snapshot-bound and never gains or implies plan/publication/manifest provenance in #91.
- Preserve Docker volumes. A live gate must reject any pre-existing project container, stop only its owned stack, and finish with zero project containers in every state.
- Production writes are supported only through serialized Airflow (`max_active_runs=1`); direct concurrent JAR invocation is unsupported.
- The exact policy version is `nyc_taxi_quality_v1`; thresholds and SQL are checked-in constants, never DagRun configuration.
- Use strict TDD for every implementation task: named RED evidence before production code, focused GREEN, then commit.
- Keep reports and `.superpowers/sdd` progress artifacts ignored; keep `graphify-out/` excluded from commits.

## File structure

- `spark-apps/nyc-taxi-data-quality/pom.xml` — exact Spark/Scala/test/shade build.
- `spark-apps/nyc-taxi-data-quality/src/main/scala/com/thekaveh/dataeng/quality/QualityContract.scala` — argument, timestamp, schema, run-ID, rule, threshold, fact, and status contracts.
- `spark-apps/nyc-taxi-data-quality/src/main/scala/com/thekaveh/dataeng/quality/QualityTransforms.scala` — null-safe split, exact multiset/fingerprint validation, and signal evaluation.
- `spark-apps/nyc-taxi-data-quality/src/main/scala/com/thekaveh/dataeng/quality/QualityStore.scala` — Iceberg metadata/read/write/property/MERGE/readback boundary plus an injectable interface.
- `spark-apps/nyc-taxi-data-quality/src/main/scala/com/thekaveh/dataeng/quality/NycTaxiDataQuality.scala` — ordered application orchestration and failure recovery.
- `spark-apps/nyc-taxi-data-quality/src/test/scala/com/thekaveh/dataeng/quality/*Spec.scala` — local Spark contract, transform, store, and failure-order tests.
- `spark-apps/nyc-taxi-data-quality/dag.py` — same-logical-date sensor and Atlas REST-confirmed Spark submission.
- `spark-apps/nyc-taxi-data-quality/queries/{latest,trend,operator_attention}.sql` — fixed dashboard/query registry.
- `spark-apps/nyc-taxi-data-quality/{Jenkinsfile,README.md}` — publish and operator contract.
- `tests/scenarios/test_nyc_taxi_data_quality_{app_contract,dag,queries,live}.py` — executable repository and live acceptance.
- Canonical scenario notebooks/README, matrix, generated docs/wiki, runbook, diagram, app index, changelog — reconciled only after live acceptance.

---

### Task 1: Maven boundary, arguments, schemas, run IDs, and rules

**Files:**
- Create: `spark-apps/nyc-taxi-data-quality/pom.xml`
- Create: `spark-apps/nyc-taxi-data-quality/src/main/resources/log4j2.properties`
- Create: `spark-apps/nyc-taxi-data-quality/src/main/scala/com/thekaveh/dataeng/quality/QualityContract.scala`
- Create: `spark-apps/nyc-taxi-data-quality/src/test/scala/com/thekaveh/dataeng/quality/QualityContractSpec.scala`

**Interfaces:**
- Produces: `Arguments(logicalDate: Instant, dataIntervalEnd: Instant, upstreamDagId: String)`.
- Produces: `QualityContract.parseArguments(args: Array[String]): Arguments`.
- Produces: `QualityContract.qualityRunId(logicalDate: Instant, snapshot: Option[Long]): String`.
- Produces: `SourceSnapshot(id: Long, committedAt: Instant, schemaSha256: String)`.
- Produces: `QualityFact` with the exact 23 design fields and `RuleDefinition` registry of eight rows.
- Produces: exact `bronzeSchema: StructType`, `factsSchema: StructType`, compact sorted-key
  `canonicalSchemaJson`, and its frozen `schemaSha256: String`.

- [ ] **Step 1: Write RED contract tests**

Add ScalaTest cases that assert the exact 20-column mixed-case Bronze schema, the exact 23-column
facts schema, eight exact rule IDs/owners/threshold strings, half-up scale-9 ratios, severity/status
precedence, strict whole-second UTC parsing, deterministic SHA-256 run ID, and rejection of missing,
duplicate, extra, reordered, over-128-byte, fractional, offset, whitespace, invalid-calendar,
control/non-ASCII, arbitrary-table, arbitrary-upstream, and arbitrary-threshold arguments.

```scala
val parsed = QualityContract.parseArguments(Array(
  "--logical-date", "2026-08-13T01:00:00Z",
  "--data-interval-end", "2026-08-14T00:00:00Z",
  "--upstream-dag-id", "nyc_taxi_etl"
))
assert(parsed.upstreamDagId == "nyc_taxi_etl")
assert(QualityContract.rules.map(_.ruleId) == QualityContract.ExpectedRuleIds)
assert(QualityContract.qualityRunId(parsed.logicalDate, Some(6090932775096319165L)) ==
  QualityContract.sha256("nyc_taxi\n2026-08-13T01:00:00Z\n6090932775096319165\nnyc_taxi_quality_v1"))
```

- [ ] **Step 2: Run RED**

Run: `mvn -q -B -f spark-apps/nyc-taxi-data-quality/pom.xml test`

Expected: compilation failure because `QualityContract` and its types do not exist.

- [ ] **Step 3: Implement the minimal immutable contract**

Copy only the dependency/plugin versions from `spark-apps/gh-archive-pipeline/pom.xml`. Implement
strict `DateTimeFormatter` parsing with `ResolverStyle.STRICT`, `MessageDigest` SHA-256, exact Spark
schemas, closed enums/sets, ratio rounding, and the eight literal rule definitions from the design.
No network, filesystem, catalog, Spark session, environment-variable, or clock read belongs in this
file.

- [ ] **Step 4: Run GREEN and package**

Run:

```bash
mvn -q -B -f spark-apps/nyc-taxi-data-quality/pom.xml test
mvn -q -B -f spark-apps/nyc-taxi-data-quality/pom.xml package
```

Expected: all `QualityContractSpec` tests pass and
`target/nyc-taxi-data-quality-0.1.0.jar` exists.

- [ ] **Step 5: Commit**

```bash
git add spark-apps/nyc-taxi-data-quality/pom.xml \
  spark-apps/nyc-taxi-data-quality/src/main/resources/log4j2.properties \
  spark-apps/nyc-taxi-data-quality/src/main/scala/com/thekaveh/dataeng/quality/QualityContract.scala \
  spark-apps/nyc-taxi-data-quality/src/test/scala/com/thekaveh/dataeng/quality/QualityContractSpec.scala
git commit -m "feat(quality): define NYC taxi policy contract"
```

### Task 2: Null-safe partition, exact validation, and deterministic fingerprints

**Files:**
- Create: `spark-apps/nyc-taxi-data-quality/src/main/scala/com/thekaveh/dataeng/quality/QualityTransforms.scala`
- Create: `spark-apps/nyc-taxi-data-quality/src/test/scala/com/thekaveh/dataeng/quality/QualityTransformsSpec.scala`

**Interfaces:**
- Consumes: `QualityContract.bronzeSchema`, `QualityContract.rules`, `SourceSnapshot`, `QualityFact`.
- Produces: `SplitResult(clean: DataFrame, quarantine: DataFrame)`.
- Produces: `RowFingerprint(rowCount: Long, sumA: BigDecimal, xorA: Long, sumB: BigDecimal, xorB: Long)`.
- Produces: `QualityTransforms.split(bronze: DataFrame): SplitResult`.
- Produces: `QualityTransforms.assertExactSchema`, `assertPartition`, `assertReadback`, and `fingerprint`.
- Produces: `QualityTransforms.evaluateBronze` and `evaluateSilver` returning exact keyed facts.

- [ ] **Step 1: Write RED transform tests**

Use local Spark UTC mode. Cover positive and zero/negative fare, passenger values 0/1/6/7, null fare,
null passenger, both null, NaN/infinite doubles, duplicate rows, and fully null non-rule columns.
Assert `coalesce(predicate,false)`, clean/quarantine disjointness, exact `exceptAll` conservation in
both directions, duplicate multiplicity, stable fingerprints across repartition/input order, and
schema rejection for missing/extra/reordered/wrong-type/wrong-nullability/mixed-case field drift.
Add exact threshold-edge cases for 1% and 5%, all status-precedence combinations, zero denominator,
and the eight fixed readback checks.

```scala
val split = QualityTransforms.split(bronze)
assert(split.clean.count + split.quarantine.count == bronze.count)
assert(split.quarantine.filter($"passenger_count".isNull).count == expectedNullPassengers)
QualityTransforms.assertPartition(bronze, split.clean, split.quarantine)
```

- [ ] **Step 2: Run RED**

Run: `mvn -q -B -f spark-apps/nyc-taxi-data-quality/pom.xml test`

Expected: compilation failure because `QualityTransforms` does not exist.

- [ ] **Step 3: Implement the minimal transforms**

Select the exact 20 fields in order, evaluate `coalesce(rule, false)`, preserve complete rows, and
validate with count plus bidirectional `exceptAll`. Build two salted `xxhash64` columns across all
fields and aggregate count, decimal sums, XORs, minima, and maxima without driver collection; hash
the bounded aggregate tuple to the report checksum. Treat NaN/infinite fare or passenger values as
invalid. Implement policy evaluation with exact integer numerators/denominators and scale-9 decimal
values.

- [ ] **Step 4: Run GREEN twice under repartition variation**

Run:

```bash
mvn -q -B -f spark-apps/nyc-taxi-data-quality/pom.xml test
mvn -q -B -f spark-apps/nyc-taxi-data-quality/pom.xml -Dtest=QualityTransformsSpec test
```

Expected: all transform, duplicate, null, schema, fingerprint, and threshold tests pass twice.

- [ ] **Step 5: Commit**

```bash
git add spark-apps/nyc-taxi-data-quality/src/main/scala/com/thekaveh/dataeng/quality/QualityTransforms.scala \
  spark-apps/nyc-taxi-data-quality/src/test/scala/com/thekaveh/dataeng/quality/QualityTransformsSpec.scala
git commit -m "feat(quality): implement null-safe NYC taxi checks"
```

### Task 3: Iceberg store, metadata binding, properties, and idempotent fact MERGE

**Files:**
- Create: `spark-apps/nyc-taxi-data-quality/src/main/scala/com/thekaveh/dataeng/quality/QualityStore.scala`
- Create: `spark-apps/nyc-taxi-data-quality/src/test/scala/com/thekaveh/dataeng/quality/QualityStoreSpec.scala`

**Interfaces:**
- Consumes: `SourceSnapshot`, `QualityFact`, exact schemas and `RowFingerprint`.
- Produces: trait `QualityStore` with `ensureFactsTable`, `captureSource`, `readBronze`,
  `replaceClean`, `replaceQuarantine`, `readSilver`, `mergeFacts`, and `readFacts`.
- Produces: `SparkQualityStore(spark: SparkSession)` implementing the exact three-table contract.
- Produces: `QualityProperties.forRun(runId: String, snapshotId: Long): Map[String,String]`.

- [ ] **Step 1: Write RED store tests**

Test exact metadata SQL, one positive `main` snapshot, UTC commit parsing, standard-property allowance,
explicit rejection of any misleading `data_eng_lab.dataset*` dependence, exact five quality
properties, namespace/table identifiers, exact `createOrReplace` order, strict properties readback,
facts-table schema validation, MERGE composite key `(quality_run_id, rule_id)`, duplicate incoming or
stored keys, same-run replacement without count growth, new-snapshot history, partial diagnostic
subsets, and exact eight-row accepted readback. Use an injected SQL executor/table adapter so local
tests do not require Iceberg.

- [ ] **Step 2: Run RED**

Run: `mvn -q -B -f spark-apps/nyc-taxi-data-quality/pom.xml test`

Expected: compilation failure because `QualityStore` is absent.

- [ ] **Step 3: Implement the Spark/Iceberg boundary**

Query `lakehouse.bronze.nyc_taxi_trips.refs` and `.snapshots`, require `main`, and bind its snapshot
ID to the latest metadata row. Create/validate namespaces and the exact facts table. Replace each
Silver table, apply only the five quality properties with escaped fixed keys/validated values, then
reread them. Register a bounded temporary facts view and execute a literal target-table MERGE whose
update/insert column list contains all 23 fields. Always drop the view in `finally`; preserve a
primary exception over cleanup failures.

- [ ] **Step 4: Run GREEN**

Run: `mvn -q -B -f spark-apps/nyc-taxi-data-quality/pom.xml test`

Expected: contract, transforms, and store suites pass.

- [ ] **Step 5: Commit**

```bash
git add spark-apps/nyc-taxi-data-quality/src/main/scala/com/thekaveh/dataeng/quality/QualityStore.scala \
  spark-apps/nyc-taxi-data-quality/src/test/scala/com/thekaveh/dataeng/quality/QualityStoreSpec.scala
git commit -m "feat(quality): add snapshot-bound Iceberg store"
```

### Task 4: Ordered application, fatal diagnostics, and deterministic recovery

**Files:**
- Create: `spark-apps/nyc-taxi-data-quality/src/main/scala/com/thekaveh/dataeng/quality/NycTaxiDataQuality.scala`
- Create: `spark-apps/nyc-taxi-data-quality/src/test/scala/com/thekaveh/dataeng/quality/NycTaxiDataQualitySpec.scala`

**Interfaces:**
- Consumes: all Task 1–3 interfaces.
- Produces: `QualityPipeline(store: QualityStore).run(arguments: Arguments): RunResult`.
- Produces: `NycTaxiDataQuality.main(args: Array[String]): Unit`.

- [ ] **Step 1: Write RED ordering and failure-injection tests**

Implement a recording fake store. Assert the exact happy order, warning success only after facts
readback, and primary failure/no-later-write behavior for: facts-table creation, missing source,
source read, schema, stale snapshot, invalid-ratio fail, clean write/readback, quarantine
write/readback, between-write failure, Bronze postcheck, property mismatch, facts MERGE, and facts
readback. Assert best-effort diagnostic subsets for safely reachable missing/stale/fail states,
catalog-unavailable truthfulness, cleanup errors not masking primaries, same-run retry convergence,
and no false complete facts after a partial Silver write.

- [ ] **Step 2: Run RED**

Run: `mvn -q -B -f spark-apps/nyc-taxi-data-quality/pom.xml test`

Expected: compilation failure because `QualityPipeline` and entrypoint do not exist.

- [ ] **Step 3: Implement the exact state machine**

Follow design steps 1–8 without catch-all success conversion. Persist only the facts computable at a
fatal boundary, then rethrow a bounded closed-message `QualityFailure`. After both Silver readbacks,
require unchanged Bronze snapshot/commit/schema, MERGE all eight facts, and accept only exact
readback. `main` sets UTC, creates Spark once, delegates, emits only bounded run ID/status/counts,
and always stops Spark without masking the primary exception.

- [ ] **Step 4: Run GREEN and package**

Run:

```bash
mvn -q -B -f spark-apps/nyc-taxi-data-quality/pom.xml test
mvn -q -B -f spark-apps/nyc-taxi-data-quality/pom.xml package
```

Expected: all Scala suites pass and the shaded JAR contains the exact entrypoint.

- [ ] **Step 5: Commit**

```bash
git add spark-apps/nyc-taxi-data-quality/src/main/scala/com/thekaveh/dataeng/quality/NycTaxiDataQuality.scala \
  spark-apps/nyc-taxi-data-quality/src/test/scala/com/thekaveh/dataeng/quality/NycTaxiDataQualitySpec.scala
git commit -m "feat(quality): orchestrate durable quality runs"
```

### Task 5: Same-logical-date Airflow DAG and REST-confirmed Spark task

**Files:**
- Create: `spark-apps/nyc-taxi-data-quality/dag.py`
- Create: `tests/scenarios/test_nyc_taxi_data_quality_dag.py`
- Modify: `tests/test_dag_catalog_conf.py`

**Interfaces:**
- Produces: DAG `nyc_taxi_data_quality` with task IDs `wait_for_matching_nyc_taxi_etl` and
  `submit_nyc_taxi_data_quality`.
- Produces: `AtlasSparkSubmitOperator` whose `_get_hook()` wraps with `RestConfirmingSparkHook`.

- [ ] **Step 1: Write RED DAG tests**

Import the DAG in an isolated module with fake Airflow/provider/Atlas modules. Assert `@daily`, UTC
start, `catchup=False`, `max_active_runs=1`, one retry/two minutes, exact sensor DAG/task,
same-logical-date default, `allowed_states=['success']`, fixed failed states, `reschedule`, 60-second
poke, 3,600-second timeout, existence check, exact dependency edge, `spark_default`, cluster mode,
wait completion, event log, Iceberg/S3A conf, exact JAR/class, strict whole-second templates, no
resolver/network call, no arbitrary DagRun threshold/table/SQL, and REST hook ownership.

- [ ] **Step 2: Run RED**

Run: `uv run pytest tests/scenarios/test_nyc_taxi_data_quality_dag.py tests/test_dag_catalog_conf.py -q`

Expected: failure because the DAG file is absent.

- [ ] **Step 3: Implement the production DAG**

Follow `spark-apps/gh-archive-pipeline/dag.py` for the Atlas operator/config, but omit all resolver
code. Use `ExternalTaskSensor` with no execution-date override so Airflow uses the exact logical
date. Render strict UTC values with fixed `strftime` expressions and fixed application arguments.

- [ ] **Step 4: Run GREEN and compile import**

Run:

```bash
uv run pytest tests/scenarios/test_nyc_taxi_data_quality_dag.py tests/test_dag_catalog_conf.py -q
uv run python -m py_compile spark-apps/nyc-taxi-data-quality/dag.py
```

Expected: all DAG tests pass without network access.

- [ ] **Step 5: Commit**

```bash
git add spark-apps/nyc-taxi-data-quality/dag.py \
  tests/scenarios/test_nyc_taxi_data_quality_dag.py tests/test_dag_catalog_conf.py
git commit -m "feat(quality): schedule after matching NYC taxi ETL"
```

### Task 6: Fixed Trino dashboard/query registry

**Files:**
- Create: `spark-apps/nyc-taxi-data-quality/queries/latest.sql`
- Create: `spark-apps/nyc-taxi-data-quality/queries/trend.sql`
- Create: `spark-apps/nyc-taxi-data-quality/queries/operator_attention.sql`
- Create: `tests/scenarios/test_nyc_taxi_data_quality_queries.py`

**Interfaces:**
- Consumes: `lakehouse.gold.nyc_taxi_quality_facts` exact schema.
- Produces: bounded deterministic latest, 90-run trend, and 100-row operator-attention query files.

- [ ] **Step 1: Write RED SQL contract tests**

Parse the three files and require a single SELECT/WITH statement, literal exact table, explicit
columns, deterministic ORDER BY, latest completeness `count(*)=8`, accepted status restriction,
trend complete-run restriction/90 limit, attention status precedence/100 limit, and exact aliases.
Reject semicolons, comments, braces/interpolation, SELECT star, DDL/DML/CALL/SET, unordered LIMIT,
and any mutable query parameter.

- [ ] **Step 2: Run RED**

Run: `uv run pytest tests/scenarios/test_nyc_taxi_data_quality_queries.py -q`

Expected: failure because all query files are absent.

- [ ] **Step 3: Add the three literal queries**

Use an exact eight-pair values registry and CTEs to select only complete accepted eight-row run IDs
for latest/trend; reject missing, duplicate, foreign, or lineage-inconsistent sets. Cast decimals to
`decimal(38,9)` and timestamps to canonical UTC whole-second strings at the projection boundary.
Operator attention includes diagnostic code, owner, source snapshot, and threshold fields without
exception text.

- [ ] **Step 4: Run GREEN**

Run: `uv run pytest tests/scenarios/test_nyc_taxi_data_quality_queries.py -q`

Expected: all query allowlist, schema, bound, and ordering tests pass.

- [ ] **Step 5: Commit**

```bash
git add spark-apps/nyc-taxi-data-quality/queries tests/scenarios/test_nyc_taxi_data_quality_queries.py
git commit -m "feat(quality): add durable Trino quality views"
```

### Task 7: Jenkins publication and application contracts

**Files:**
- Create: `spark-apps/nyc-taxi-data-quality/Jenkinsfile`
- Create: `spark-apps/nyc-taxi-data-quality/README.md`
- Create: `tests/scenarios/test_nyc_taxi_data_quality_app_contract.py`
- Modify: `tests/test_verify_repo.py`

**Interfaces:**
- Produces: published artifact `s3a://jars/nyc-taxi-data-quality/0.1.0/app.jar`.
- Produces: operator run/recovery/query documentation bound to the design.

- [ ] **Step 1: Write RED packaging/document tests**

Assert the POM/JAR/class/version, Jenkins test/package/publish commands, fixed MinIO destination,
credential injection without logging, README's snapshot-only warning, exact schemas/tables/rules,
same-logical-date dependency, non-atomic recovery, unsupported direct concurrency, fixed query
retrieval, and explicit deferred upstream five-key hardening.

- [ ] **Step 2: Run RED**

Run: `uv run pytest tests/scenarios/test_nyc_taxi_data_quality_app_contract.py tests/test_verify_repo.py -q`

Expected: failure because Jenkinsfile/README do not exist.

- [ ] **Step 3: Implement publish and README contracts**

Mirror the reviewed per-app Jenkins convention and document exact replay/recovery commands. Do not
add a new global seed job: the existing repository pipeline convention discovers per-app
Jenkinsfiles and all-app Maven CI discovers every `pom.xml`.

- [ ] **Step 4: Run GREEN plus Maven all-app build**

Run:

```bash
uv run pytest tests/scenarios/test_nyc_taxi_data_quality_app_contract.py tests/test_verify_repo.py -q
for pom in spark-apps/*/pom.xml; do mvn -q -B -f "$pom" test; done
```

Expected: focused contracts and all Spark application tests pass.

- [ ] **Step 5: Commit**

```bash
git add spark-apps/nyc-taxi-data-quality/Jenkinsfile spark-apps/nyc-taxi-data-quality/README.md \
  tests/scenarios/test_nyc_taxi_data_quality_app_contract.py tests/test_verify_repo.py
git commit -m "ci(quality): publish NYC taxi quality app"
```

### Task 8: Genuine offline-tested live acceptance harness

**Files:**
- Create: `tests/scenarios/test_nyc_taxi_data_quality_live.py`
- Create: `docs/superpowers/reports/2026-08-13-nyc-taxi-data-quality-live-acceptance.md`

**Interfaces:**
- Produces: `RUN_INFRA=1` exclusive lifecycle proof; skips safely without the environment flag.
- Consumes: hardened pagination/ownership/pause/pointer/driver helpers from prior live harnesses by
  copying only scenario-specific bounded logic, not importing another test as production truth.

- [ ] **Step 1: Write RED offline helper tests**

Add fake Docker, Airflow v2 API, Spark REST, MinIO, Jenkins, Trino, and command-runner tests. Cover
all-state pre-existing-container rejection before mutation; owned cleanup on primary failure; no
dataset refresh/fallback; exact optional pointer absent/present/ambiguous semantics and closed
bounded body; initial DAG pause capture/restore with no unpause; complete bounded DagRun pagination;
unexpected run/driver rejection; exact same logical date; sensor success rather than bypass;
terminal Spark states; bounded/redacted commands/logs; and zero final containers.

- [ ] **Step 2: Run RED**

Run: `uv run pytest tests/scenarios/test_nyc_taxi_data_quality_live.py -q`

Expected: helper tests fail because the harness is incomplete while the `RUN_INFRA` case skips.

- [ ] **Step 3: Implement the full opt-in harness**

Require the verified tiny NYC publication and existing supported prerequisites; never infer that a
generic resolver failure means absence. Build exact local ETL/quality JARs, publish or byte-verify
the exact reviewed artifacts, start the exclusively owned stack, pause both DAGs, trigger matching
ETL/quality pairs through API-visible runs, and gather exact bounded Airflow/Spark/Trino/Iceberg/S3
evidence. Validate source/Silver/facts/query state from live tables rather than report prose.

- [ ] **Step 4: Run offline GREEN**

Run: `uv run pytest tests/scenarios/test_nyc_taxi_data_quality_live.py -q`

Expected: every helper test passes and exactly one infrastructure test skips.

- [ ] **Step 5: Commit harness before live execution**

```bash
git add tests/scenarios/test_nyc_taxi_data_quality_live.py \
  docs/superpowers/reports/2026-08-13-nyc-taxi-data-quality-live-acceptance.md
git commit -m "test(quality): add NYC taxi live acceptance gate"
```

### Task 9: Canonical live ETL, quality, recovery, facts, and query proof

**Files:**
- Modify: `docs/superpowers/reports/2026-08-13-nyc-taxi-data-quality-live-acceptance.md`
- Modify only if live TDD exposes a defect: files and focused tests from Tasks 1–8.

**Interfaces:**
- Produces: frozen exact JAR hashes, pointer identity, DagRun IDs, Spark driver IDs, snapshot IDs,
  counts, fingerprints, fact identities/statuses, and query checksums.

- [ ] **Step 1: Assert the dataset prerequisite without mutation**

Run:

```bash
uv run python scripts/download_datasets.py --scale tiny --only nyc_taxi --verify-only
uv run python scripts/resolve_dataset.py nyc_taxi --scale tiny
```

Expected: both succeed and return the same existing verified generation. If the pointer is absent,
stop and report the explicit prerequisite; do not refresh from the harness.

- [ ] **Step 2: Run canonical acceptance**

Run:

```bash
RUN_INFRA=1 uv run pytest tests/scenarios/test_nyc_taxi_data_quality_live.py -vv -s
```

Expected: matching ETL and quality runs succeed through the sensor; each Spark driver is
`FINISHED/success=true`; same-snapshot retry is idempotent; second ETL snapshot preserves history;
all Silver/fact/query assertions pass; the pointer is unchanged; cleanup leaves zero containers.

- [ ] **Step 3: If live fails, use strict diagnostic TDD**

For each real defect, first add the smallest deterministic offline reproduction to the owning test
suite, record RED, make the minimal production fix, rerun focused GREEN, commit separately, and
restart the canonical harness only after volume-preserving zero-container cleanup.

- [ ] **Step 4: Freeze exact evidence in the report**

Record replayable redacted commands, exact artifact SHA-256 values, all owned run/driver IDs,
Bronze/Silver/Gold snapshots, counts/fingerprints, eight-row fact sets, idempotent/history evidence,
Trino exact schemas/rows/checksums, pointer before/after identity, and zero-container teardown. The
report must distinguish live assertions from historical recovery observations.

- [ ] **Step 5: Commit evidence**

```bash
git add docs/superpowers/reports/2026-08-13-nyc-taxi-data-quality-live-acceptance.md
git commit -m "test(quality): record live NYC taxi acceptance"
```

### Task 10: Reconcile notebooks with the null-safe educational contract

**Files:**
- Modify: `scenarios/data_quality-nyc_taxi-spark-iceberg/jupyter/notebook.ipynb`
- Modify: `scenarios/data_quality-nyc_taxi-spark-iceberg/zeppelin/notebook.zpln`
- Modify: `tests/scenarios/test_execution_modes.py`
- Modify: `tests/scenarios/test_notebook_reproducibility_live.py`

**Interfaces:**
- Produces: both notebooks using the exact null-safe complement, 20-column projection, partition
  conservation, and production-risk warning.

- [ ] **Step 1: Write RED notebook contracts**

Require both languages to use `coalesce(predicate,false)` semantics, preserve duplicates, assert
clean plus quarantine equals source, name the exact three tables, and warn that interactive writes
bypass serialization/snapshot properties/facts and can invalidate production. Reject the old
`NOT(rule) OR fare_amount IS NULL` gap and claims of production equivalence.

- [ ] **Step 2: Run RED**

Run: `uv run pytest tests/scenarios/test_execution_modes.py tests/scenarios/test_notebook_reproducibility_live.py -q`

Expected: notebook contract test fails on the old quarantine expression/warning absence.

- [ ] **Step 3: Update both notebook JSON files mechanically and minimally**

Change only the quality predicate/complement, exact projection/count verification, and markdown
warning. Keep paired Scala/PySpark table outputs and teaching intent aligned.

- [ ] **Step 4: Run GREEN and notebook-build checks**

Run:

```bash
uv run pytest tests/scenarios/test_execution_modes.py tests/scenarios/test_notebook_reproducibility_live.py -q
uv run pytest tests/scenarios/test_build_notebooks.py -q
```

Expected: notebook semantics, JSON validity, and paired projections pass.

- [ ] **Step 5: Commit**

```bash
git add scenarios/data_quality-nyc_taxi-spark-iceberg/{jupyter/notebook.ipynb,zeppelin/notebook.zpln} \
  tests/scenarios/test_execution_modes.py tests/scenarios/test_notebook_reproducibility_live.py
git commit -m "docs(quality): make notebook partition null-safe"
```

### Task 11: Promote canonical docs, matrix, runbook, and architecture diagram

**Files:**
- Modify: `scenarios/data_quality-nyc_taxi-spark-iceberg/README.md`
- Modify: `scenarios/execution-modes.yaml`
- Modify: `scripts/scenario_execution.py`
- Modify: `docs/spark-apps/index.md`
- Modify: `docs/scenarios/index.md`
- Modify: `docs/go-live.md`
- Modify: `docs/CHANGELOG.md`
- Modify: `docs/diagrams/data_quality-nyc_taxi-spark-iceberg.html`
- Regenerate: `docs/diagrams/img/data_quality-nyc_taxi-spark-iceberg.png`
- Modify: `docs/manifest.yaml`
- Modify: `tests/test_docs_content_contract.py`
- Create: `tests/scenarios/test_nyc_taxi_data_quality_docs.py`
- Regenerate: site/wiki projections through repository scripts.

**Interfaces:**
- Consumes: successful Task 9 exact live evidence.
- Produces: one truthful `existing production DAG` classification and synchronized repository/site/wiki.

- [ ] **Step 1: Read and invoke the architecture-diagram and three-surface-docs skills**

Use their render/regeneration workflows for the HTML/SVG/PNG architecture and canonical
README-to-site/wiki projections. The diagram must show matching ETL sensor, snapshot-only trust,
null-safe Silver split, Gold facts, fixed Trino surface, and non-atomic rerun recovery.

- [ ] **Step 2: Write RED docs/matrix tests**

Require exact DAG/JAR/class/schedule/task names, source/Silver/facts schemas, eight signals,
thresholds/statuses, dashboard query files, snapshot-only limitation, deferred five-key hardening,
notebook warning, live evidence link, app/scenario counts, and matrix classification. Reject stale
approved/future/no-DAG/dashboard-absent/three-valued-gap prose across README/site/wiki.

- [ ] **Step 3: Run RED**

Run:

```bash
uv run pytest tests/scenarios/test_nyc_taxi_data_quality_docs.py \
  tests/test_docs_content_contract.py tests/scenarios/test_execution_modes.py -q
```

Expected: failures identify stale pre-production surfaces.

- [ ] **Step 4: Update canonical sources and regenerate projections**

Promote the matrix only because Task 9 passed. Add operator remediation/retrieval commands and
historical trend behavior. Render the diagram PNG from the reviewed HTML and update its SHA marker.
Run the repository's scenario-doc, site, and wiki generation commands; do not hand-edit generated
copies independently.

- [ ] **Step 5: Run GREEN and commit**

Run:

```bash
uv run pytest tests/scenarios/test_nyc_taxi_data_quality_docs.py \
  tests/test_docs_content_contract.py tests/scenarios/test_execution_modes.py -q
make docs-check
make docs-wiki
```

Then stage only the canonical/generated #91 files and commit:

```bash
git commit -m "docs(quality): publish NYC taxi monitoring contract"
```

### Task 12: Full gates, invariant audit, issue evidence, and independent review handoff

**Files:**
- Modify ignored only: `.superpowers/sdd/progress.md`
- Create ignored only: `.superpowers/sdd/review-issue-91.diff`
- Modify: issue #91 body/checklist and Project item only after corresponding evidence exists.

**Interfaces:**
- Produces: exact review package for base `774db8f..HEAD`, zero-finding-ready evidence, and no push.

- [ ] **Step 1: Run focused and all-app gates**

```bash
mvn -q -B -f spark-apps/nyc-taxi-data-quality/pom.xml test
mvn -q -B -f spark-apps/nyc-taxi-data-quality/pom.xml package
for pom in spark-apps/*/pom.xml; do mvn -q -B -f "$pom" test; done
uv run pytest tests/scenarios/test_nyc_taxi_data_quality_app_contract.py \
  tests/scenarios/test_nyc_taxi_data_quality_dag.py \
  tests/scenarios/test_nyc_taxi_data_quality_queries.py \
  tests/scenarios/test_nyc_taxi_data_quality_live.py \
  tests/scenarios/test_nyc_taxi_data_quality_docs.py -q
```

Expected: Maven/focused suites pass with only the opt-in live test skipped offline.

- [ ] **Step 2: Run full repository gates**

```bash
make lint
make test
make verify
make docs-check
make docs-wiki
docker compose -f infra/docker-compose.yml -f compose/data-eng-lab.yml config --quiet
git diff --check 774db8f..HEAD
```

Expected: every command succeeds; verifier reports zero findings/errors.

- [ ] **Step 3: Audit scope and protected invariants**

Assert `uv.lock` SHA-256 `a376ce1b5bd5621290aaded68c22572690395419876da41814e28469bb4186b1`,
protected plan SHA-256 `f7eb55036e3020f419d9acb42bbc502bfe0528219a980a6973ed083591d2ba66`,
registry SHA-256 `093de54a5c7288087e40f679a886cc0b558e750efa00ca24d0f0d888f7f76119`,
Atlas gitlink/nested HEAD `c6cf73d7168db1a7840fc45c9ed3e385071996d8` and clean nested status,
zero project containers, no #83/#91-unrelated implementation, and no staged graph/protected files.

- [ ] **Step 4: Update issue evidence accurately**

Re-read #91, check only satisfied acceptance items, keep it Open/In Progress, and append exact local
commit/test/live evidence without a closure keyword. Do not mutate completed parent/dependency
issues or start upstream hardening.

- [ ] **Step 5: Build exact review package and stop before push**

```bash
git diff --binary 774db8f..HEAD > .superpowers/sdd/review-issue-91.diff
shasum -a 256 .superpowers/sdd/review-issue-91.diff
git status --short
```

Dispatch independent spec-compliance and code-quality/security reviews against the exact HEAD and
package SHA. Fix any verified finding in a new strict-TDD commit and regenerate the exact package.
Do not push or open a PR until both reviews return zero Critical/Important/Minor findings and the
parent authorizes promotion.

## Plan self-review

- **Spec coverage:** Tasks 1–4 implement exact runtime/schema/signals/facts/failure recovery; Task 5
  implements same-logical-date orchestration and REST confirmation; Task 6 implements the durable
  query surface; Tasks 7–9 implement publication and genuine live proof; Tasks 10–11 reconcile all
  educational and documentation surfaces; Task 12 runs the full evidence/review boundary.
- **Scope:** Upstream five-key hardening is explicitly deferred; no registry, Atlas, uv, or unrelated
  child implementation appears in any task.
- **Type/interface consistency:** `Arguments`, `SourceSnapshot`, `QualityFact`, `RowFingerprint`,
  `QualityStore`, the three exact table identifiers, eight rule IDs, 23 fact fields, and composite
  MERGE key are named consistently from producers to consumers.
- **Placeholder scan:** The plan contains no deferred implementation marker or generic test/error
  instruction; each task names files, interfaces, RED command, minimal implementation, GREEN command,
  and commit boundary.
