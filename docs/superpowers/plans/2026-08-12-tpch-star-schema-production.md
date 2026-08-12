# TPC-H Star-Schema Production Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a tested, resolver-gated Spark/Airflow/Jenkins production path that deterministically replaces the TPC-H customer dimension and order fact tables.

**Architecture:** Airflow resolves and validates one complete immutable eight-object TPC-H generation during task execution, then submits all canonical URIs to a Scala application. The application independently freezes the URI boundary, validates source integrity, runs notebook-equivalent transformations, and replaces the two Iceberg tables in a documented recovery-safe order.

**Tech Stack:** Python 3.11, Airflow, Atlas `RestConfirmingSparkHook`, Scala 2.13.14, Spark 4.1.2, Maven, ScalaTest 3.2.19, Iceberg REST, MinIO/S3A, Jenkins, pytest.

## Global Constraints

- Preserve the untracked `docs/superpowers/plans/2026-07-21-atlas-submodule-modernization.md` byte-for-byte and untracked.
- Preserve root `uv.lock` SHA-256 `a376ce1b5bd5621290aaded68c22572690395419876da41814e28469bb4186b1`.
- Preserve Atlas gitlink `c6cf73d7168db1a7840fc45c9ed3e385071996d8` and a clean nested Atlas worktree.
- Do not implement issue #83 or change Atlas source, dataset registry/locks, runtime dependency versions, or infrastructure unless an executable consumer mount is proven missing.
- Use the exact eight-object TPC-H registry order and accept only canonical immutable `s3://` generation URIs.
- Write tests first, run them to observe the expected failure, implement the smallest behavior, and rerun the focused and regression suites before each implementation commit.
- Do not publish, push, open a PR, or change protected branches before independent specification and quality reviews pass.

---

### Task 1: Freeze the Scala source and transformation contract

**Files:**
- Create: `spark-apps/tpch-star-schema/pom.xml`
- Create: `spark-apps/tpch-star-schema/src/main/resources/log4j2.properties`
- Create: `spark-apps/tpch-star-schema/src/main/scala/com/thekaveh/dataeng/tpch/TpchSources.scala`
- Create: `spark-apps/tpch-star-schema/src/main/scala/com/thekaveh/dataeng/tpch/StarSchemaTransforms.scala`
- Create: `spark-apps/tpch-star-schema/src/test/scala/com/thekaveh/dataeng/tpch/TpchStarSchemaSpec.scala`

**Interfaces:**
- Produces: `TpchSources.parse(args: Array[String]): ResolvedSources`, where `ResolvedSources.canonicalUris` retains all eight `s3://` values, `sparkUri(objectName: String)` returns the corresponding `s3a://` value, and `provenance` contains scale, plan ID, publication ID, and manifest SHA-256 cross-checked against the URIs.
- Produces: `StarSchemaTransforms.validateSources(customer, orders, lineitem): Unit`, `dimension(customer): DataFrame`, and `fact(orders, lineitem): DataFrame`.

- [ ] **Step 1: Write failing ScalaTest cases for the URI boundary**

Cover the exact eight names/order, one plan/publication prefix, UUIDv4, duplicate/missing/extra/reordered/flat/wrong-dataset/malformed rejection, exact scheme-only conversion, required metadata flags, scale/digest validation, and URI/flag plan/publication equality.

- [ ] **Step 2: Run the URI tests and verify RED**

Run: `mvn -q -B -f spark-apps/tpch-star-schema/pom.xml test`

Expected: compilation failure because `TpchSources` does not exist.

- [ ] **Step 3: Implement the minimal immutable URI parser**

Use one anchored regular expression for `s3://landing/tpch/_generations/<64 lowercase hex>/<UUIDv4>/<name>`, compare the parsed names to the exact ordered vector, reject duplicate arguments, parse the four exact metadata options after the URIs, cross-check plan/publication, and construct `s3a://` by replacing only the leading scheme.

- [ ] **Step 4: Write failing local-Spark transform and validation tests**

Use explicit TPC-H-shaped schemas and rows. Assert the exact dimension and fact column/type order, keyed rows, decimal revenue sums, line counts, null/duplicate key failures, dangling order/customer failures, dangling lineitem/order failures, and no floating-point revenue conversion.

- [ ] **Step 5: Run the transform tests and verify RED**

Run the same Maven test command and confirm the missing transform implementation is the reason for failure.

- [ ] **Step 6: Implement minimal pure transforms and fail-closed validation**

Project the four dimension columns. Inner-join orders and lineitems and group by the three fact keys with `sum(l_extendedprice).as("revenue")` and `count(lit(1)).as("line_count")`. Validate required names/types and source key integrity before returning writable outputs.

- [ ] **Step 7: Run Task 1 GREEN gates**

Run the Maven tests twice: first the new app, then `make build-apps` to compile all Spark applications.

- [ ] **Step 8: Commit Task 1**

Commit only the new Maven build, resource, source parser/transform, and tests with `feat(tpch): freeze star-schema transforms (#107)`.

### Task 2: Add the deterministic application entrypoint

**Files:**
- Create: `spark-apps/tpch-star-schema/src/main/scala/com/thekaveh/dataeng/tpch/TpchStarSchema.scala`
- Modify: `spark-apps/tpch-star-schema/src/test/scala/com/thekaveh/dataeng/tpch/TpchStarSchemaSpec.scala`

**Interfaces:**
- Consumes: `TpchSources.parse`, `StarSchemaTransforms.validateSources`, `dimension`, and `fact`.
- Produces: `TpchStarSchema.run(spark: SparkSession, arguments: Array[String], writer: TableWriter): RunResult` and `TableWriter.replace(table: String, frame: DataFrame, provenance: Provenance): Unit`; `main` delegates to the production writer.

- [ ] **Step 1: Write failing entrypoint orchestration tests**

Use a recording `TableWriter` to require namespace creation, both outputs materialized before writes, replacement order `dim_customer` then `fct_orders`, exact fixed table names, atomic per-table provenance properties, read-back equality, propagation of read/validation/first-write/second-write/read-back failures, and no second write after a first-write failure. Inject a failure between writes, rerun the same generation, and prove the recorded rows and provenance converge.

- [ ] **Step 2: Verify RED**

Run: `mvn -q -B -f spark-apps/tpch-star-schema/pom.xml test`

Expected: failure because `TpchStarSchema` and `TableWriter` do not exist.

- [ ] **Step 3: Implement the minimal entrypoint**

Parse before creating Spark, read only the named customer/orders/lineitem paths, validate, cache and count both results before writing, create `lakehouse.gold`, replace dimension then fact with table properties in each replacement, read back and compare both provenance sets, report counts, unpersist, and always stop the production Spark session.

- [ ] **Step 4: Verify GREEN and package**

Run Maven test and package for the new app. Inspect the JAR to confirm `com/thekaveh/dataeng/tpch/TpchStarSchema` is present.

- [ ] **Step 5: Commit Task 2**

Commit with `feat(tpch): add deterministic Iceberg writer (#107)`.

### Task 3: Add the resolver-gated operator-owned Airflow DAG

**Files:**
- Create: `spark-apps/tpch-star-schema/dag.py`
- Create: `tests/scenarios/test_tpch_star_schema_dag.py`
- Modify: `tests/test_dag_catalog_conf.py`
- Modify: `tests/test_atlas_usage_contract.py`
- Modify: `tests/datasets/test_consumer_resolution_inventory.py`

**Interfaces:**
- Produces: `_effective_scale(context) -> str`, `_resolve_dataset(dataset, scale) -> tuple[str, ...]`, and `AtlasSparkSubmitOperator.execute(context)` which freezes all eight URIs immediately before provider execution.

- [ ] **Step 1: Write failing DAG contract tests**

Assert exact scale precedence and validation, request bytes, response field/type/depth/size/duplicate-key validation, eight names/order, canonical URI prefix, UUIDv4, exact metadata arguments after the URIs, runtime-only resolver access, import-time network denial, one operator task, Atlas hook wrapping, fixed JAR/class, cluster mode, complete Spark/Iceberg/S3A/event-log configuration, retry policy, `@daily`, and `catchup=False`.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/scenarios/test_tpch_star_schema_dag.py tests/test_dag_catalog_conf.py tests/test_atlas_usage_contract.py tests/datasets/test_consumer_resolution_inventory.py -q`

Expected: failures identifying the missing third production DAG and contract.

- [ ] **Step 3: Implement the DAG**

Follow the current production DAG adapter pattern. Resolve only in `execute`, pass all resolver-ordered immutable URIs as `application_args`, and never perform import-time DNS/HTTP/S3 work.

- [ ] **Step 4: Verify GREEN and import syntax**

Run the focused pytest command, `python -m py_compile spark-apps/tpch-star-schema/dag.py`, and Ruff over the changed Python files.

- [ ] **Step 5: Commit Task 3**

Commit with `feat(tpch): orchestrate verified star-schema runs (#107)`.

### Task 4: Add Jenkins publication and application operations documentation

**Files:**
- Create: `spark-apps/tpch-star-schema/Jenkinsfile`
- Create: `spark-apps/tpch-star-schema/README.md`
- Create: `tests/scenarios/test_tpch_star_schema_app_contract.py`

**Interfaces:**
- Produces: reviewed JAR `target/tpch-star-schema-0.1.0.jar` and published object `s3a://jars/tpch-star-schema/0.1.0/app.jar`.

- [ ] **Step 1: Write failing static publication/runbook tests**

Assert Maven test before package, package before publish, exact artifact and object paths, injected credentials rather than literals, documented argument/source/schema/measure/schedule/terminal-confirmation contracts, both table-provenance properties and Trino `$properties` comparison query, and the cross-table partial-commit recovery procedure.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/scenarios/test_tpch_star_schema_app_contract.py -q`

Expected: missing Jenkinsfile and README failures.

- [ ] **Step 3: Implement Jenkinsfile and README**

Match the repository's existing pipeline stages and document all design contracts, including complete input URI order, result schemas, daily schedule, idempotent result semantics, and rerun recovery after a second-table failure.

- [ ] **Step 4: Verify GREEN and build**

Run the focused pytest file plus the new Maven `test` and `package` commands.

- [ ] **Step 5: Commit Task 4**

Commit with `build(tpch): publish reviewed star-schema jar (#107)`.

### Task 5: Add live acceptance coverage and execute it

**Files:**
- Create: `tests/scenarios/test_tpch_star_schema_live.py`
- Modify if necessary: `tests/scenarios/live_exec.py`
- Create: `docs/superpowers/reports/2026-08-12-tpch-star-schema-live-acceptance.md`

**Interfaces:**
- Produces: an opt-in `RUN_INFRA=1` test that triggers `tpch_star_schema` with explicit `tiny`, observes Airflow terminal success and Spark `FINISHED`/`success=true`, queries both tables, and compares rerun state.

- [ ] **Step 1: Write the opt-in live test and verify collection/skip behavior**

Run without `RUN_INFRA`; expected result is one explicit live skip, not an import or collection error.

- [ ] **Step 2: Build and publish the exact reviewed JAR**

Package the new app and use the repository/Atlas Jenkins convention. Record the artifact SHA-256 and published object metadata.

- [ ] **Step 3: Start the canonical stack and publish TPC-H tiny**

Run the repository preflight, publish/verify `SCALE=tiny`, and prove the resolver returns the exact eight immutable objects.

- [ ] **Step 4: Execute first and second Airflow runs**

Trigger with `{"dataset_scale":"tiny"}`. For each run, capture Airflow success plus Spark terminal status. Between runs query exact schemas, nonzero counts, keyed segment-revenue results, both tables' provenance properties, and deterministic sorted checksums.

- [ ] **Step 5: Record evidence and preserve volumes during teardown**

Write exact commands, timestamps, run/driver IDs, counts, measures, checksums, and any environmental limitation. Stop without cold volume deletion.

- [ ] **Step 6: Commit live coverage and evidence**

Commit with `test(tpch): prove live star-schema idempotence (#107)` only when the live gate succeeds. If the environment is unavailable, do not mark the execution-mode row existing and report the blocker instead.

### Task 6: Promote the execution contract and synchronize documentation

**Files:**
- Modify: `scenarios/execution-modes.yaml`
- Regenerate: `docs/scenarios/execution-modes.md`
- Modify: `scenarios/star_schema-tpch-spark-iceberg/README.md`
- Modify: `docs/scenarios/star_schema-tpch-spark-iceberg.md`
- Modify: `docs/notebooks/star_schema-tpch-spark-iceberg.md`
- Modify: `docs/notebooks/index.md`
- Modify: `docs/spark-apps/index.md`
- Modify: `README.md`
- Modify: `docs/go-live.md`
- Modify: `CHANGELOG.md`
- Modify: `docs/CHANGELOG.md`
- Modify: relevant documentation manifest inputs
- Modify: `docs/diagrams/star_schema-tpch-spark-iceberg.html`
- Regenerate: `docs/diagrams/img/star_schema-tpch-spark-iceberg.png`
- Modify: focused documentation/execution-mode tests

**Interfaces:**
- Produces: one canonical row classified `existing production DAG`, entrypoint `spark-apps/tpch-star-schema/dag.py`, and a daily schedule after successful live evidence; generated site and wiki projections derive from canonical sources.

- [ ] **Step 1: Write failing documentation and execution-mode assertions**

Require the existing classification, exact entrypoint/schedule, three production DAG count, accurate output schemas and measures, resolver boundary, Jenkins URI, live evidence, downstream #83 dependency, and no stale "no production DAG" claim.

- [ ] **Step 2: Verify RED**

Run focused execution-mode and docs tests; confirm failures are the stale approved-state/public claims.

- [ ] **Step 3: Update canonical sources and diagram master**

Change the matrix only because Task 5 passed. Reconcile scenario/app/notebook indexes, README, runbook, changelogs, and the HTML diagram without changing paired notebook computation.

- [ ] **Step 4: Regenerate all derived surfaces**

Render the execution matrix, diagram PNG, MkDocs input/site, and wiki projection with repository commands. Never hand-edit generated projections.

- [ ] **Step 5: Verify documentation GREEN**

Run `make docs-check`, `make docs-wiki`, strict MkDocs build, diagram checks, content contract tests, and execution-mode tests.

- [ ] **Step 6: Commit Task 6**

Commit with `docs(tpch): publish production execution contract (#107)`.

### Task 7: Full verification and independent review handoff

**Files:**
- Modify only files required by discovered regressions; every fix needs a failing regression test first.

**Interfaces:**
- Produces: a clean feature-branch review package; no remote mutation.

- [ ] **Step 1: Run focused and full application builds**

Run the new Maven test/package commands, `make build-apps`, and inspect the packaged entrypoint.

- [ ] **Step 2: Run repository gates**

Run `make lint`, `make test`, `make verify`, `make docs-check`, `make docs-wiki`, and assembled Compose validation. Run live acceptance again if a runtime-affecting fix was made.

- [ ] **Step 3: Audit invariants and scope**

Verify the protected untracked plan hash/status, root lock hash, Atlas gitlink/nested status, no #83 implementation, no secrets, no generated build artifacts staged, and no unexpected infrastructure diff.

- [ ] **Step 4: Prepare independent specification and quality review inputs**

Generate one base-to-head diff and a requirements checklist covering every #107 acceptance item and every design section. Do not push or open a PR.

- [ ] **Step 5: Resolve review findings test-first**

For each Critical or Important finding, reproduce it with a failing test, implement the smallest fix, rerun relevant/full gates, and request re-review until both reviewers report no blocking findings.

- [ ] **Step 6: Report readiness to the parent lifecycle owner**

Provide commit hashes, RED/GREEN evidence, Maven/JAR evidence, live run/driver/table evidence, complete gates, invariant hashes, and independent review verdicts. Remote PR/promotion remains owned by the parent after review.
