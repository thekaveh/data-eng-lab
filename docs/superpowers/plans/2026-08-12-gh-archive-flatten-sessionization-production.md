# GitHub Archive Flatten and Sessionization Production Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task with the stated review checkpoints. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a resolver-gated, two-stage Spark/Airflow/Jenkins production pipeline that deterministically replaces the typed GitHub Archive event and session tables from one immutable generation.

**Architecture:** One Airflow resolver task returns a strict canonical XCom payload that is reused by two ordered Atlas REST-confirming Spark submissions from one Maven JAR. Flatten reads only the exact immutable landing objects and replaces `lakehouse.silver.gh_events`; sessionization first verifies that table's five provenance properties against the same payload, reads only the flat table, and replaces `lakehouse.silver.gh_sessions`.

**Tech Stack:** Python 3.11, Airflow, Atlas `RestConfirmingSparkHook`, Scala 2.13.14, Spark 4.1.2, Maven, ScalaTest 3.2.19, Iceberg REST, MinIO/S3A, Jenkins, pytest, Trino.

## Global constraints

- Preserve untracked `docs/superpowers/plans/2026-07-21-atlas-submodule-modernization.md` byte-for-byte with SHA-256 `f7eb55036e3020f419d9acb42bbc502bfe0528219a980a6973ed083591d2ba66`.
- Preserve root `uv.lock` SHA-256 `a376ce1b5bd5621290aaded68c22572690395419876da41814e28469bb4186b1`.
- Preserve Atlas gitlink `c6cf73d7168db1a7840fc45c9ed3e385071996d8` and a clean nested Atlas worktree.
- Do not change the dataset registry/lock/pointer/history, Atlas source, dependency versions, or implement #83, #91, or another scenario child.
- Freeze the current registry's scale-specific chronological object order and schema ID `gh_archive_consumed_fields`; do not discover or sort objects.
- Use exactly the #107/#108 five table-property keys on both output tables.
- Accept only exact whole-second UTC `created_at` strings and enforce deterministic `(created_at, id)` session order.
- The resolver task must produce one bounded canonical payload; both Spark tasks consume the same XCom value and neither operator resolves again.
- Sessionization must validate `gh_events` properties immediately before the source-table read and fail before any write on absence/mismatch.
- Serialize the production path with `max_active_runs=1`; direct concurrent JAR execution is unsupported.
- Write every regression test first, run it to observe the intended failure, implement the smallest behavior, and rerun the focused gate before committing.
- Do not push, open a PR, or mutate protected branches before independent specification and quality reviews pass.

---

### Task 1: Freeze immutable arguments and pure transforms

**Files:**
- Create: `spark-apps/gh-archive-pipeline/pom.xml`
- Create: `spark-apps/gh-archive-pipeline/src/main/resources/log4j2.properties`
- Create: `spark-apps/gh-archive-pipeline/src/main/scala/com/thekaveh/dataeng/gharchive/GhArchiveSources.scala`
- Create: `spark-apps/gh-archive-pipeline/src/main/scala/com/thekaveh/dataeng/gharchive/GhArchiveRawPreflight.scala`
- Create: `spark-apps/gh-archive-pipeline/src/main/scala/com/thekaveh/dataeng/gharchive/GhArchiveTransforms.scala`
- Create: `spark-apps/gh-archive-pipeline/src/test/scala/com/thekaveh/dataeng/gharchive/GhArchivePipelineSpec.scala`

**Interfaces:**
- Produces: `GhArchiveSources.parse(args: Array[String]): ResolvedSources` with exact canonical URIs and `Provenance(dataset, scale, planId, publicationId, manifestSha256)`; object metadata remains in the Airflow-validated canonical XCom while current registry names/sizes/digests are frozen for raw application preflight rather than invented as flags.
- Produces: bounded `GhArchiveRawPreflight.validate(spark, sources)` before Spark inference.
- Produces: `GhArchiveTransforms.validateNestedSource(frame)`, `flatten(frame)`, `validateEvents(frame)`, `sessionize(frame)`, and duplicate-aware `validateSessions(frame, events)`.
- Produces exact ordered `EventsSchema` and `SessionsSchema` constants.

- [ ] **Step 1: Write failing immutable-parser tests**

  Cover exact tiny, small, and medium URI-name sequences; canonical generation prefix; UUID4;
  lowercase plan/manifest identifiers; and exact generation flags. Reject malformed arguments,
  duplicate/missing/extra/reordered objects, duplicate flags, wrong manifest/UUID/dataset/path/scheme,
  flat landing, encoded traversal, and cross-generation inputs. Require scheme-only `s3://` to
  `s3a://` conversion. Per-object size/digest/schema rejection belongs to Task 3, where the complete
  resolver payload is available.

- [ ] **Step 2: Verify parser RED**

  Run: `mvn -q -B -f spark-apps/gh-archive-pipeline/pom.xml test`

  Expected: compilation/test failure because `GhArchiveSources` is absent.

- [ ] **Step 3: Implement the minimal immutable parser**

  Use scale-indexed immutable vectors:

  ```scala
  val ExpectedObjects = Map(
    "tiny" -> Vector("2023-01-01-0.json.gz"),
    "small" -> Vector("2023-01-01-0.json.gz", "2023-01-01-1.json.gz", "2023-01-01-2.json.gz"),
    "medium" -> Vector("2023-01-01-0.json.gz", "2023-01-01-1.json.gz", "2023-01-01-2.json.gz",
      "2023-01-01-3.json.gz", "2023-01-01-4.json.gz", "2023-01-01-5.json.gz")
  )
  ```

  Parse the exact URI sequence before the four generation flags, require one generation prefix, and
  expose Spark URIs only after the complete publication validates. Do not add size/digest/schema
  command-line flags that are absent from the approved entrypoint contract.

- [ ] **Step 4: Write failing nested-source and flatten tests**

  Construct local Spark frames with `id`, `type`, nested `actor.login`, nested `repo.name`, and
  `created_at` as strings. Assert exact output schema/order, multiplicity-aware source-row
  conservation, preserved identical duplicate IDs, input-order independence, and allowed unrelated
  nested payload fields. Reject absent/wrong source paths, wrong types, null/blank values,
  conflicting records sharing an ID, malformed rows, and empty input.

  Parameterize timestamp cases so only `2023-01-01T00:00:00Z`-style whole-second UTC passes. Require
  rejection of fractional seconds, `+00:00`, other offsets, missing `Z`, lowercase `z`, whitespace,
  invalid dates, leap normalization, local/ambiguous forms, and formatter round-trip mismatch.

  Before DataFrame tests, exercise physical gzip JSON lines: exact required string tokens, mixed
  numeric/string primitives, booleans/nulls, missing and wrong nested shapes, duplicate keys,
  malformed/trailing/multiple documents, oversized/unterminated/deep records, allowed extra fields,
  exact compressed size/SHA locks, and resource closure. Require production main to invoke this
  preflight before `spark.read.json`.

- [ ] **Step 5: Verify flatten RED**

  Run the app Maven test and confirm the absent `GhArchiveTransforms` behavior is the failure cause.

- [ ] **Step 6: Implement strict flatten transforms**

  Configure UTC/corrected parsing, validate the exact consumed nested types, parse with an explicit
  whole-second pattern, round-trip the original literal, and project:

  ```scala
  id: string, type: string, actor_login: string, repo_name: string, created_at: timestamp
  ```

- [ ] **Step 7: Write failing deterministic-session tests**

  Assert exact ordered eight-column schema and types; partition by actor and order by
  `(created_at,id)`; simultaneous-event ID tie-breaks; first-row null previous time and session 1;
  1,799/1,800 seconds remain one session; 1,801 seconds starts another; multiple actors restart at
  1; multiset row conservation; repeatability under shuffled input; and exact `integer`/`long`
  types. Reject null/blank actors, null timestamps/IDs, conflicting records sharing an ID, malformed event schema, invalid
  `previous_created_at` null placement, invalid new-session flags, and discontinuous session IDs.

- [ ] **Step 8: Implement minimal deterministic sessionization**

  Use one window ordered by timestamp and ID for `lag`, and a rows-based cumulative window for
  `sum(new_session).cast(LongType)`. Retain the five event columns and append
  `previous_created_at`, `new_session`, and `session_id`.
  Independently rederive that exact output from the original five event columns and validate the
  complete duplicate-aware multiset with bidirectional `exceptAll`, so physical repartitioning of
  indistinguishable duplicates cannot cause a false rejection.

- [ ] **Step 9: Run Task 1 GREEN gates and commit**

  Run app Maven tests and `make build-apps`. Commit with
  `feat(gh-archive): freeze pipeline transforms (#109)`.

### Task 2: Add independently reviewable Iceberg stages

**Files:**
- Create: `spark-apps/gh-archive-pipeline/src/main/scala/com/thekaveh/dataeng/gharchive/GhArchiveFlatten.scala`
- Create: `spark-apps/gh-archive-pipeline/src/main/scala/com/thekaveh/dataeng/gharchive/GhArchiveSessionization.scala`
- Create: `spark-apps/gh-archive-pipeline/src/main/scala/com/thekaveh/dataeng/gharchive/IcebergTables.scala`
- Modify: `spark-apps/gh-archive-pipeline/src/test/scala/com/thekaveh/dataeng/gharchive/GhArchivePipelineSpec.scala`

**Interfaces:**
- Produces: `GhArchiveFlatten.runResolved(sources, raw, writer): FlattenResult`.
- Produces: `GhArchiveSessionization.runResolved(sources, writer): SessionResult`.
- Replaces: `lakehouse.silver.gh_events`, then `lakehouse.silver.gh_sessions` in separate applications.
- `TableWriter` exposes property read, source-table read, replacement, and readback boundaries so failure ordering is executable.

- [ ] **Step 1: Write failing flatten orchestration tests**

  With a recording/failing writer require namespace creation, complete input materialization and
  validation before replacement, exact event table name, exact five properties,
  schema/multiplicity-aware-rows/count
  readback, and failure propagation. Require zero writes after argument/source/transform failure and
  failure on write/readback/property mismatch.

- [ ] **Step 2: Verify flatten orchestration RED and implement**

  Run Maven tests, observe missing entrypoint/writer failure, then implement one finite FAILFAST JSON
  read over the exact URIs, cached transform validation, `CREATE NAMESPACE IF NOT EXISTS
  lakehouse.silver`, property-bearing `createOrReplace`, and full readback comparison.

- [ ] **Step 3: Write failing session precondition/order tests**

  Record calls and prove the exact order:

  ```text
  parse immutable arguments -> read gh_events properties -> compare five keys -> read gh_events rows
  -> validate/sessionize/materialize -> replace gh_sessions -> read back both property sets
  ```

  Missing, mismatched, or unexpected `data_eng_lab.dataset*` properties must fail with no source-row
  read and no session write. Ordinary Iceberg system properties remain allowed. Source-read,
  transform, write, session-readback, event-property reread, and final cross-table mismatch failures
  must propagate.

- [ ] **Step 4: Verify session RED and implement**

  Run Maven tests, then implement the immediate property check, exact event-schema read, cached
  deterministic session transform, replacement, session readback, and final exact five-key equality
  for both tables.

- [ ] **Step 5: Write the partial-failure and recovery RED**

  Simulate successful flatten followed by session failure. Require the mixed generation to be
  observable through properties and the run to fail. Rerun the same `ResolvedSources` and require
  both exact multiplicity-aware row multisets, schemas, counts, session invariants, and properties to
  converge.
  Also inject a first-stage failure and prove sessionization is never invoked by the supported
  orchestration contract.

- [ ] **Step 6: Verify GREEN, package, inspect, and commit**

  Run Maven `test` and `package`; inspect the JAR for both exact entrypoints. Commit with
  `feat(gh-archive): add deterministic Iceberg stages (#109)`.

### Task 3: Add one resolver XCom and two serialized Spark tasks

**Files:**
- Create: `spark-apps/gh-archive-pipeline/dag.py`
- Create: `tests/scenarios/test_gh_archive_pipeline_dag.py`
- Modify: `tests/test_dag_catalog_conf.py`
- Modify: `tests/test_atlas_usage_contract.py`
- Modify: `tests/datasets/test_consumer_resolution_inventory.py`

**Interfaces:**
- Produces: `_effective_scale(context)`, `_resolve_dataset(scale): str`, and `_parse_resolution(payload, scale): Resolution`.
- Produces: `AtlasResolvedSparkSubmitOperator` that pulls `resolve_gh_archive` XCom and never performs network resolution.
- Produces exact task graph `resolve_gh_archive >> submit_gh_archive_flatten >> submit_gh_archive_sessionization`.

- [ ] **Step 1: Write failing resolver and canonical-payload tests**

  Assert scale precedence, exact POST body, timeout, 1 MiB and depth bounds, duplicate-key/non-finite
  rejection, exact fields/types, positive sizes, exact name/schema order per scale, canonical compact
  sorted JSON output, and no secret/environment/endpoint fields. Cover malformed, duplicate, deep,
  extra, wrong-type, zero-size, digest, UUID, URI, generation, order, and response-bound failures.

- [ ] **Step 2: Write failing operator execution tests**

  Import with network disabled. Execute both operators with a fake TaskInstance that returns the same
  payload. Require one XCom pull from task `resolve_gh_archive`, byte-identical argument lists,
  different exact Java classes, all canonical URIs before metadata flags, no resolver call, no
  import-time network, generic bounded errors, and provider execution only after validation.

- [ ] **Step 3: Write failing DAG ownership/configuration tests**

  Require three tasks and exact dependencies; `@daily`; `catchup=False`; `max_active_runs=1`;
  `spark_default`; cluster mode; exact JAR; both classes; waitAppCompletion; complete
  Spark/Iceberg/S3A/event-log settings; one retry/two-minute delay; Atlas REST confirmation; and no
  streaming/checkpoint/Redpanda dependency.

- [ ] **Step 4: Verify DAG RED**

  Run:

  ```bash
  uv run pytest tests/scenarios/test_gh_archive_pipeline_dag.py \
    tests/test_dag_catalog_conf.py tests/test_atlas_usage_contract.py \
    tests/datasets/test_consumer_resolution_inventory.py -q
  ```

  Expected: failures identify the absent production DAG and inventory.

- [ ] **Step 5: Implement the minimal DAG**

  Adapt the reviewed #107/#108 bounds and hook without inheriting their one-task resolution behavior.
  Use one Python resolver task, canonical JSON XCom, and an operator whose `execute` only validates
  the XCom and delegates to the provider through `RestConfirmingSparkHook`.

- [ ] **Step 6: Verify GREEN and commit**

  Run focused pytest, `python -m py_compile`, and Ruff. Commit with
  `feat(gh-archive): orchestrate coupled production stages (#109)`.

### Task 4: Add Jenkins publication and the production runbook

**Files:**
- Create: `spark-apps/gh-archive-pipeline/Jenkinsfile`
- Create: `spark-apps/gh-archive-pipeline/README.md`
- Create: `tests/scenarios/test_gh_archive_pipeline_app_contract.py`

**Interfaces:**
- Produces: `target/gh-archive-pipeline-0.1.0.jar` and `s3a://jars/gh-archive-pipeline/0.1.0/app.jar`.
- Documents a concrete two-table `$properties` comparison query using the exact five keys.

- [ ] **Step 1: Write failing Jenkins/runbook contract tests**

  Require Maven test before package before publish; injected MinIO credentials; exact local and
  remote JAR paths; two main classes; exact scale inventories; strict UTC source contract; both exact
  table schemas; row conservation; five properties; concrete `$properties` fail-closed SQL; daily
  serialized DAG; no streaming path; unsupported direct concurrency; and same-generation recovery.

- [ ] **Step 2: Verify RED and implement**

  Run the focused contract, confirm missing files, then follow the established Jenkins stages and
  document complete operator/build/recovery/notebook trust-boundary behavior.

- [ ] **Step 3: Verify GREEN/build and commit**

  Run focused pytest, Maven `test`/`package`, and `make build-apps`. Commit with
  `build(gh-archive): publish reviewed pipeline jar (#109)`.

### Task 5: Add genuine four-driver live acceptance

**Files:**
- Create: `tests/scenarios/test_gh_archive_pipeline_live.py`
- Create: `tests/scenarios/test_gh_archive_pipeline_live_harness.py`
- Modify only if proven necessary: `tests/scenarios/live_exec.py`
- Create: `docs/superpowers/reports/2026-08-12-gh-archive-pipeline-live-acceptance.md`

**Interfaces:**
- Produces: a `RUN_INFRA=1` opt-in test that owns a stopped stack, never refreshes GH Archive, keeps the daily DAG paused, and proves exactly two real two-stage DagRuns.

- [ ] **Step 1: Write offline harness RED tests**

  With fake command/API clients require safe offline skip; all-state container ownership preflight;
  zero mutation on pre-existing containers; owned-failure cleanup; no volume deletion; pause-state
  capture/restore without unpause; complete bounded Airflow v2 pagination; exact two owned run IDs;
  unexpected active/window run rejection; exactly two driver deltas per run and four unique total;
  bounded/redacted errors; resolver failure with no refresh/verify/second resolve; pointer body+ETag
  equality; and cleanup that preserves primary failures.

- [ ] **Step 2: Verify harness RED and implement helpers**

  Run both live files without `RUN_INFRA`, requiring one explicit infra skip and focused helper
  failures only for absent behavior. Implement the independently named DAG/table/artifact harness by
  reusing hardened structural patterns, not report-string assertions.

- [ ] **Step 3: Add independent immutable-source validation**

  Stream each resolver URI from object storage, prove exact byte size and SHA-256, decompress with
  bounded iteration, parse every JSON object, validate exact consumed paths/types/timestamps,
  preserve/count identical duplicate IDs, reject conflicting duplicate IDs, and compute the
  authoritative source count and deterministic source checksum. Never infer a source count from
  output tables.

- [ ] **Step 4: Start an exclusively owned canonical stack**

  Fail if any project container already exists, record active-pointer bytes and ETag, require an
  existing verified tiny publication, build/publish and hash-match the reviewed JAR, start the stack,
  and record the initial DAG pause state without unpausing it.

- [ ] **Step 5: Execute exactly two controlled DagRuns**

  Use unique whole-second logical dates with the paused-DAG supported test mechanism. Before/after
  each trigger paginate the complete run inventory and require an exact one-run set difference.
  Require resolver, flatten, and session tasks to succeed and exactly two new ordered, distinct Spark
  drivers with terminal `FINISHED`/`success=true` per run.

- [ ] **Step 6: Query exact outputs and deterministic rerun**

  Through Trino query exact schemas, duplicate multiplicity/distinct-ID measures, strict session
  invariants, type/session measures, five properties, snapshots, row counts, and deterministic
  multiplicity-aware row checksums. Assert independently
  `source count == gh_events count == gh_sessions count`. On the second run require identical logical
  counts/measures/checksums and advancing snapshots. Require four distinct Spark drivers total.

- [ ] **Step 7: Record evidence and teardown**

  Record exact redacted replayable commands, JAR digest, resolver/pointer identity, owned DagRuns,
  four drivers, schemas/counts/measures/properties/checksums/snapshots, pause state, and teardown.
  Restore the initial pause state, reject unexpected active runs, stop only the owned stack, assert
  zero project containers in all states, and preserve named volumes.

- [ ] **Step 8: Commit live proof**

  Commit with `test(gh-archive): prove coupled live idempotence (#109)` only after the genuine live
  gate passes. If the verified publication or environment is unavailable, do not promote the matrix.

### Task 6: Reconcile notebooks and promote all documentation surfaces

**Files:**
- Modify: `scenarios/sessionization-gh_archive-spark-iceberg/jupyter/notebook.ipynb`
- Modify: `scenarios/sessionization-gh_archive-spark-iceberg/zeppelin/notebook.zpln`
- Modify: `scenarios/json_flatten-gh_archive-spark-iceberg/README.md`
- Modify: `scenarios/sessionization-gh_archive-spark-iceberg/README.md`
- Modify: `scenarios/execution-modes.yaml`
- Regenerate: `docs/scenarios/execution-modes.md`
- Modify/regenerate: both matching `docs/scenarios/` and `docs/notebooks/` pages
- Modify: `docs/notebooks/index.md`, `docs/scenarios/index.md`, `docs/spark-apps/index.md`
- Create: `docs/spark-apps/gh-archive-pipeline.md`
- Modify: `README.md`, `docs/index.md`, `docs/go-live.md`, `CHANGELOG.md`, `docs/CHANGELOG.md`, `docs/manifest.yaml`
- Modify: `docs/diagrams/json_flatten-gh_archive-spark-iceberg.html`
- Modify: `docs/diagrams/sessionization-gh_archive-spark-iceberg.html`
- Regenerate: corresponding PNG and SHA-256 files
- Create/modify: focused docs, notebook, matrix, projection, and diagram tests

**Interfaces:**
- Produces two canonical matrix rows classified `existing production DAG` with entrypoint `spark-apps/gh-archive-pipeline/dag.py` and `@daily` only after Task 5 passes.
- Preserves notebook education while making production trust boundaries explicit.

- [ ] **Step 1: Write failing notebook-parity and documentation assertions**

  Require session notebooks to read exact `gh_events`, validate its five-column schema, order by
  `(created_at,id)`, and write the exact eight-column sessions table. Require both notebook pairs to
  warn they directly replace production table names without production provenance/serialization.
  Require scenario pages to say `id string`, sessionization consumes the flat table, and streaming
  ingest remains intentionally notebook-only.

- [ ] **Step 2: Verify RED and correct both notebooks**

  Run focused notebook/content tests. Update Jupyter and Zeppelin sessionization cells equivalently;
  do not add resolver or landing reads to sessionization. Preserve the educational section structure
  and correct only stale semantics needed for parity.

- [ ] **Step 3: Write failing production-documentation assertions**

  Require existing-production classification, exact entrypoint/schedule/task graph/JAR/classes,
  schemas, strict UTC behavior, row conservation, five properties, Jenkins publication, recovery,
  four-driver live evidence, app count, and trust-boundary prose across repository/site/wiki sources.

- [ ] **Step 4: Update canonical documentation after live success**

  Promote both matrix rows, add the Spark-app page, reconcile READMEs/indexes/go-live/changelogs and
  remove stale claims that sessionization rereads landing input or emits a four-column table.

- [ ] **Step 5: Regenerate diagrams and three surfaces**

  Use the architecture-diagram skill to update the two dark HTML/SVG masters with one resolver XCom,
  ordered Spark stages, exact Silver outputs, matching provenance, and recovery. Regenerate PNG/hash,
  execution-mode projection, MkDocs site, and wiki from canonical sources; do not hand-edit generated
  projections.

- [ ] **Step 6: Verify GREEN and commit**

  Run focused notebook/docs tests, `make docs-check`, `make docs-wiki`, strict MkDocs, and diagram
  gates. Commit notebook corrections separately from the final documentation promotion when each is
  independently reviewable.

### Task 7: Full verification and independent review handoff

**Files:**
- Modify only files required by a reproduced regression; every fix begins with a failing test.
- Create ignored review/report artifacts under `.superpowers/sdd/` as needed.

**Interfaces:**
- Produces a clean, exact base-to-HEAD review package and evidence report; no remote mutation.

- [ ] **Step 1: Run application and focused gates**

  Run app Maven `test`/`package`, `make build-apps`, all GH Archive pipeline pytest contracts, Ruff,
  notebook parity, docs projections, and JAR entrypoint inspection.

- [ ] **Step 2: Run full repository gates**

  Run `make lint`, `make test`, `make verify`, `make docs-check`, `make docs-wiki`, strict MkDocs,
  Compose validation, and the live gate again if any runtime-affecting behavior changed after its last
  pass.

- [ ] **Step 3: Audit scope and protected invariants**

  Verify the protected plan and `uv.lock` hashes, Atlas gitlink/nested cleanliness, no registry/lock/
  pointer diff, no #83/#91 implementation, no secrets/build artifacts, zero project containers, and
  preserved named volumes.

- [ ] **Step 4: Prepare exact review inputs**

  Generate the binary diff from base `e0a2c01b9c3d5147aa1c5bddbcb430e8ed0868ed` to HEAD and record
  its SHA-256. Record all commits, named RED/GREEN evidence, Maven/JAR results, two DagRuns/four
  drivers, counts/measures/properties/checksums/snapshots, pointer equality, docs regeneration, and
  invariant hashes.

- [ ] **Step 5: Request specification and quality reviews**

  Stop before push/PR. Reproduce every Critical or Important finding with a failing test, implement
  the smallest correction, rerun proportional focused/full/live gates, and request re-review until
  both verdicts are clear.

- [ ] **Step 6: Report readiness**

  Provide exact commits, tests, live identifiers, worktree/protected state, review package hash, and
  both independent verdicts to the lifecycle owner. Remote promotion is a separate authorized phase.
