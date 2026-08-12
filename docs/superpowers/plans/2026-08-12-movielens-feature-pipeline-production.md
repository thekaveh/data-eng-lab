# MovieLens Feature-Pipeline Production Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task with the stated review checkpoints.

**Goal:** Deliver a tested, resolver-gated Spark/Airflow/Jenkins production path that deterministically replaces the notebook-faithful MovieLens user and movie feature tables.

**Architecture:** Airflow resolves and validates one complete immutable, scale-specific MovieLens publication during task execution and submits every canonical URI plus provenance to Scala. The application independently validates the generation, reads only `ratings.csv` using an explicit schema, computes the two notebook-equivalent aggregates, and replaces both Iceberg tables in a serialized, recovery-tested order.

**Tech Stack:** Python 3.11, Airflow, Atlas `RestConfirmingSparkHook`, Scala 2.13.14, Spark 4.1.2, Maven, ScalaTest 3.2.19, Iceberg REST, MinIO/S3A, Jenkins, pytest, Trino.

## Global constraints

- Preserve untracked `docs/superpowers/plans/2026-07-21-atlas-submodule-modernization.md` byte-for-byte and untracked.
- Preserve root `uv.lock` SHA-256 `a376ce1b5bd5621290aaded68c22572690395419876da41814e28469bb4186b1`.
- Preserve Atlas gitlink `c6cf73d7168db1a7840fc45c9ed3e385071996d8` and a clean nested Atlas worktree.
- Do not change the dataset registry/lock, Atlas source, dependency versions, or implement another child issue.
- Freeze the current registry's scale-specific object-name/schema-ID order; do not sort it.
- Use the exact #107/#83 five-property convention on both output tables.
- Preserve duplicate ratings as distinct events contributing independently to `avg` and `count(*)`.
- Serialize the production path with `max_active_runs=1`; direct concurrent JAR execution is unsupported.
- Write each regression test first, run it to observe the intended failure, implement the smallest behavior, and rerun focused/regression gates before committing.
- Do not push, open a PR, or mutate protected branches before independent specification and quality reviews pass.

---

### Task 1: Freeze the Scala source and transform contract

**Files:**
- Create: `spark-apps/movielens-feature-pipeline/pom.xml`
- Create: `spark-apps/movielens-feature-pipeline/src/main/resources/log4j2.properties`
- Create: `spark-apps/movielens-feature-pipeline/src/main/scala/com/thekaveh/dataeng/movielens/MovieLensSources.scala`
- Create: `spark-apps/movielens-feature-pipeline/src/main/scala/com/thekaveh/dataeng/movielens/FeatureTransforms.scala`
- Create: `spark-apps/movielens-feature-pipeline/src/test/scala/com/thekaveh/dataeng/movielens/MovieLensFeaturePipelineSpec.scala`

**Interfaces:**
- Produces: `MovieLensSources.parse(args: Array[String]): ResolvedSources`; the result retains all canonical URIs, exposes a `ratingsUri` converted only at the Spark boundary, and carries scale/plan/publication/manifest provenance.
- Produces: `FeatureTransforms.ratingsSchema`, `validateRatings(frame)`, `userFeatures(frame)`, and `movieFeatures(frame)`.

- [ ] **Step 1: Write failing parser tests**

Cover tiny/small exact sequence `links.csv`, `tags.csv`, `ratings.csv`, `README.txt`, `movies.csv` with the five `movielens_latest_small_*` schema IDs; cover medium exact sequence `tags.csv`, `links.csv`, `README.txt`, `ratings.csv`, `genome-tags.csv`, `genome-scores.csv`, `movies.csv` with the seven `movielens_25m_*` schema IDs. Assert scale-specific order, canonical generation prefix, UUIDv4, plan/publication flag equality, lowercase digests, duplicate/missing/extra/reordered/flat/wrong-dataset/cross-generation rejection, exact scheme-only conversion, and required metadata flags.

- [ ] **Step 2: Verify parser RED**

Run: `mvn -q -B -f spark-apps/movielens-feature-pipeline/pom.xml test`

Expected: compilation failure because `MovieLensSources` does not exist.

- [ ] **Step 3: Implement the immutable parser**

Use scale-indexed immutable vectors for object names/schema IDs and one anchored generation-URI parser. Parse one exact URI sequence followed by four unique metadata options, reject positional/option ambiguity, validate one generation, and expose `ratings.csv` only after full-publication validation.

- [ ] **Step 4: Write failing local-Spark transform tests**

Build ratings DataFrames with explicit types. Assert exact source schema/order, exact output schema/order, keyed aggregates, finite averages, positive counts, sums of both count columns equal source count, input-order independence, null/missing/extra/wrong-type/empty/non-finite failures, and deliberate preservation of duplicate rating rows.

- [ ] **Step 5: Verify transform RED**

Run the same Maven command and confirm the missing `FeatureTransforms` behavior is the failure cause.

- [ ] **Step 6: Implement minimal notebook-equivalent transforms**

Use `groupBy("userId").agg(avg("rating").as("avg_rating"), count(lit(1)).as("num_ratings"))` and the analogous movie aggregation with `movie_avg`/`popularity`. Do not deduplicate or join movies/genres.

- [ ] **Step 7: Run Task 1 GREEN gates**

Run the app Maven tests and `make build-apps`. Confirm all existing applications still compile.

- [ ] **Step 8: Commit Task 1**

Commit with `feat(movielens): freeze feature transforms (#108)`.

### Task 2: Add ordered Iceberg replacement and readback

**Files:**
- Create: `spark-apps/movielens-feature-pipeline/src/main/scala/com/thekaveh/dataeng/movielens/MovieLensFeaturePipeline.scala`
- Modify: `spark-apps/movielens-feature-pipeline/src/test/scala/com/thekaveh/dataeng/movielens/MovieLensFeaturePipelineSpec.scala`

**Interfaces:**
- Produces: `MovieLensFeaturePipeline.run(spark, arguments, writer): RunResult` and a replace/readback boundary testable without a production catalog.
- Replaces: `lakehouse.gold.ml_user_features`, then `lakehouse.gold.ml_movie_features`.

- [ ] **Step 1: Write failing orchestration tests**

Use a recording/failing writer to require namespace creation; a single explicit `ratings.csv` read; both results materialized and validated before writes; exact table names/order; exact five properties; no writes after source/read/transform validation failure; no second write after first-write failure; second-write failure propagation; exact schema/row-key/count/property readback; and failure on missing or mismatched readback.

- [ ] **Step 2: Write the recovery RED**

Inject a failure between the user and movie replacements. Assert the run fails with the documented partial state, then rerun the same immutable generation and require both final keyed row sets, schemas, row keys, count invariants, and provenance to converge.

- [ ] **Step 3: Verify orchestration RED**

Run the app Maven tests and confirm the absent entrypoint/writer is the expected cause.

- [ ] **Step 4: Implement the minimal entrypoint**

Parse arguments before Spark work, read CSV with explicit schema and fail-fast options, validate and cache/count both outputs before the first replacement, create `lakehouse.gold`, replace user then movie with properties, read both back, compare expected schemas/keys/counts/properties, unpersist, and stop the production session in `main`.

- [ ] **Step 5: Verify GREEN and package**

Run Maven `test` and `package`. Inspect `target/movielens-feature-pipeline-0.1.0.jar` for `com/thekaveh/dataeng/movielens/MovieLensFeaturePipeline`.

- [ ] **Step 6: Commit Task 2**

Commit with `feat(movielens): add deterministic Iceberg writer (#108)`.

### Task 3: Add the resolver-gated serialized Airflow DAG

**Files:**
- Create: `spark-apps/movielens-feature-pipeline/dag.py`
- Create: `tests/scenarios/test_movielens_feature_pipeline_dag.py`
- Modify: `tests/test_dag_catalog_conf.py`
- Modify: `tests/test_atlas_usage_contract.py`
- Modify: `tests/datasets/test_consumer_resolution_inventory.py`

**Interfaces:**
- Produces: `_effective_scale(context)`, `_resolve_dataset(dataset, scale)`, and an `AtlasSparkSubmitOperator` execution path that freezes one validated resolver response immediately before provider execution.

- [ ] **Step 1: Write failing DAG tests**

Assert scale precedence/validation; exact request; response byte/depth/field/type/duplicate-key bounds; positive `size_bytes`; both scale-specific name/schema-ID orders; duplicate/deep/extra/malformed/type/size/digest/order/generation failures; runtime-only access; no import-time network; exact URI and provenance arguments; one task; fixed JAR/class; `spark_default`; cluster mode; complete Spark/Iceberg/S3A/event-log settings; Atlas REST confirmation; retry once; `@daily`; `catchup=False`; and `max_active_runs=1`.

- [ ] **Step 2: Verify DAG RED**

Run: `uv run pytest tests/scenarios/test_movielens_feature_pipeline_dag.py tests/test_dag_catalog_conf.py tests/test_atlas_usage_contract.py tests/datasets/test_consumer_resolution_inventory.py -q`

Expected: failures identifying the missing production DAG and contract inventory.

- [ ] **Step 3: Implement the DAG**

Adapt the reviewed #107 operator pattern without cross-DAG names. Keep resolver calls inside `execute`, retain registry order, pass all URIs before metadata flags, and wrap the provider hook with Atlas terminal REST confirmation.

- [ ] **Step 4: Verify GREEN and import safety**

Run the focused pytest command, `python -m py_compile spark-apps/movielens-feature-pipeline/dag.py`, and Ruff over changed Python files.

- [ ] **Step 5: Commit Task 3**

Commit with `feat(movielens): orchestrate verified feature runs (#108)`.

### Task 4: Add Jenkins publication and application runbook

**Files:**
- Create: `spark-apps/movielens-feature-pipeline/Jenkinsfile`
- Create: `spark-apps/movielens-feature-pipeline/README.md`
- Create: `tests/scenarios/test_movielens_feature_pipeline_app_contract.py`

**Interfaces:**
- Produces: reviewed `target/movielens-feature-pipeline-0.1.0.jar` and `s3a://jars/movielens-feature-pipeline/0.1.0/app.jar`.

- [ ] **Step 1: Write failing static contract tests**

Require test-before-package-before-publish; exact local/published JAR; injected credentials; exact object/schema orders; source and output schemas; duplicate-event semantics; fixed five properties; concrete Trino `$properties` comparison query; serialized schedule; unsupported direct concurrency; and between-table recovery procedure.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/scenarios/test_movielens_feature_pipeline_app_contract.py -q`

Expected: missing Jenkinsfile/README failures.

- [ ] **Step 3: Implement Jenkinsfile and README**

Follow repository publication stages and document the entire production, recovery, and notebook trust-boundary contract.

- [ ] **Step 4: Verify GREEN and build**

Run the focused contract, app Maven `test`, app Maven `package`, and `make build-apps`.

- [ ] **Step 5: Commit Task 4**

Commit with `build(movielens): publish reviewed feature jar (#108)`.

### Task 5: Add a genuine isolated live-acceptance harness

**Files:**
- Create: `tests/scenarios/test_movielens_feature_pipeline_live.py`
- Create: `tests/scenarios/test_movielens_feature_pipeline_live_harness.py`
- Modify only if necessary: `tests/scenarios/live_exec.py`
- Create: `docs/superpowers/reports/2026-08-12-movielens-feature-pipeline-live-acceptance.md`

**Interfaces:**
- Produces: a `RUN_INFRA=1` opt-in acceptance that owns a stopped stack, never mutates a dataset pointer, leaves the production DAG paused, and proves exactly two real MovieLens pipeline executions.

- [ ] **Step 1: Write offline harness RED tests**

With fake command/API clients assert: safe skip without `RUN_INFRA`; all-container ownership preflight including stopped/exited/created; zero mutation on pre-existing containers; cleanup on owned failure; no cold/down-volume command; initial pause-state capture/restore; no unpause; active-baseline rejection; complete bounded Airflow-v2 pagination; malformed/repeated/nonprogress/duplicate/over-limit rejection; exact before/after run-set differences; unexpected second-page run rejection; exactly two owned run IDs; no third run; no unexpected active run; bounded/redacted command output; and resolver failure causes no refresh, verify, or second resolve.

- [ ] **Step 2: Verify harness RED and collection behavior**

Run the two focused live files without `RUN_INFRA`. Require one explicit skip and offline helper failures only for missing behavior.

- [ ] **Step 3: Implement the MovieLens-specific harness**

Reuse reviewed structural helpers from #107 while changing every namespace, DAG, artifact, table, query, and assertion to MovieLens. The prerequisite is an already verified tiny publication. Never infer absence from a generic resolver failure and never invoke `--refresh` from the test.

- [ ] **Step 4: Build/publish and start an exclusively owned stack**

Run preflight, package the reviewed JAR, publish through the reviewed repository convention, confirm no project containers exist, start the canonical stack, record initial DAG pause state, and verify the exact tiny resolver inventory.

- [ ] **Step 5: Execute exactly two controlled paused-DAG runs**

Use two unique whole-second logical dates with `airflow dags test --use-executor`. Before/after each, paginate the complete API inventory and require an exact one-run set difference. Require one new Spark driver and terminal `FINISHED`/`success=true` per run.

- [ ] **Step 6: Query schemas, measures, provenance, and idempotence**

Through Trino require exact schemas, nonempty keyed tables, finite averages, positive counts, equal `sum(num_ratings)`/`sum(popularity)`, all exact five properties, and identical keyed counts/checksums/results after the unchanged rerun. Snapshot IDs may differ.

- [ ] **Step 7: Record replayable evidence and teardown safely**

Record redacted exact commands, artifact digest, pointer metadata before/after, run/driver IDs, API inventories, schemas, row counts, measures, properties, checksums, and teardown. Restore the initial pause state, reject active/unexpected runs, stop only the owned stack, preserve volumes, and verify zero containers.

- [ ] **Step 8: Commit Task 5**

Commit with `test(movielens): prove live feature idempotence (#108)` only after the live gate passes. If the prerequisite/environment is unavailable, do not promote the execution matrix and report the blocker.

### Task 6: Promote the execution contract and reconcile documentation

**Files:**
- Modify: `scenarios/execution-modes.yaml`
- Regenerate: `docs/scenarios/execution-modes.md`
- Modify: `scenarios/feature_engineering-movielens-spark-iceberg/README.md`
- Modify: `docs/scenarios/feature_engineering-movielens-spark-iceberg.md`
- Modify: `docs/notebooks/feature_engineering-movielens-spark-iceberg.md`
- Modify: `docs/notebooks/index.md`
- Modify: `docs/spark-apps/index.md`
- Create: `docs/spark-apps/movielens-feature-pipeline.md`
- Modify: `README.md`
- Modify: `docs/go-live.md`
- Modify: `CHANGELOG.md`
- Modify: `docs/CHANGELOG.md`
- Modify: relevant `docs/manifest.yaml` source/projection inputs
- Modify: `docs/diagrams/feature_engineering-movielens-spark-iceberg.html`
- Regenerate: `docs/diagrams/img/feature_engineering-movielens-spark-iceberg.png`
- Modify: focused docs and execution-mode tests

**Interfaces:**
- Produces: one canonical row classified `existing production DAG`, entrypoint `spark-apps/movielens-feature-pipeline/dag.py`, and `@daily` only after Task 5 succeeds.

- [ ] **Step 1: Write failing documentation assertions**

Require existing classification, exact entrypoint/schedule, accurate two-Gold-table schemas and duplicate semantics, resolver/provenance boundary, Jenkins URI, live evidence, production concurrency boundary, and warnings that the notebooks are untrusted direct writers. Reject stale three-Silver/rating-deviation/genre/interactions claims.

- [ ] **Step 2: Verify documentation RED**

Run focused execution-mode and content-contract tests and confirm only stale approved/public documentation fails.

- [ ] **Step 3: Update canonical sources**

Promote the matrix because Task 5 passed. Reconcile scenario/app/notebook indexes, README, runbook, changelogs, and diagram master without changing notebook computation.

- [ ] **Step 4: Use the architecture-diagram skill and regenerate derived surfaces**

Update the dark HTML/SVG master to show resolver-verified immutable MovieLens input, serialized Airflow/Spark, two Gold outputs, provenance equality/readback, and recovery. Regenerate PNG/fingerprint, execution matrix, site, and wiki from canonical sources; do not hand-edit generated projections.

- [ ] **Step 5: Verify documentation GREEN**

Run `make docs-check`, `make docs-wiki`, strict MkDocs build, diagram tests, content contract tests, and execution-mode tests.

- [ ] **Step 6: Commit Task 6**

Commit with `docs(movielens): publish production execution contract (#108)`.

### Task 7: Full verification and independent review handoff

**Files:**
- Modify only files required by proven regressions; every fix begins with a failing test.
- Create ignored review/report artifacts under `.superpowers/sdd/` as needed.

**Interfaces:**
- Produces: a clean feature-branch review package and evidence report; no remote mutation.

- [ ] **Step 1: Run application and focused gates**

Run app Maven `test`/`package`, `make build-apps`, all MovieLens focused pytest contracts, Ruff, and JAR entrypoint inspection.

- [ ] **Step 2: Run full repository gates**

Run `make lint`, `make test`, `make verify`, `make docs-check`, `make docs-wiki`, Compose validation, and live acceptance again if any runtime-affecting code changed.

- [ ] **Step 3: Audit exact scope and invariants**

Verify protected plan hash/status, `uv.lock` hash, Atlas gitlink/nested status, no registry/lock diff, no other-child implementation, no secrets, no staged build artifacts, no containers, and a volume-preserving teardown.

- [ ] **Step 4: Prepare independent review inputs**

Generate the exact base-to-HEAD diff and a #108/design checklist. Record commit hashes, RED/GREEN counts, Maven/JAR evidence, live identifiers/tables/measures/properties/checksums, documentation regeneration, and invariant hashes.

- [ ] **Step 5: Request specification and quality reviews**

Stop before push/PR. For each Critical or Important finding, reproduce with a failing test, implement the smallest correction, rerun focused/full/live gates in proportion to the fix, and request re-review until both verdicts are clear.

- [ ] **Step 6: Report readiness to the parent lifecycle owner**

Provide exact commits, gates, live evidence, worktree state, protected invariants, and independent verdicts. Remote promotion is a separate authorized lifecycle phase.
