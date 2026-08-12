# GitHub Archive Flatten and Sessionization Production Design

**Issue:** #109  
**Date:** 2026-08-12  
**Status:** Approved for implementation

## 1. Decision

Productionize the two approved GitHub Archive scenarios as one coupled batch pipeline with two
independently submitted Spark stages:

1. `flatten` reads one resolver-verified immutable GitHub Archive generation and replaces
   `lakehouse.silver.gh_events`.
2. `sessionize` consumes that exact flat-event table generation and replaces
   `lakehouse.silver.gh_sessions`.

Both stages ship in one Maven-built Scala JAR and run as separate Atlas
`SparkSubmitOperator` tasks in one Airflow DAG. A preceding resolver task produces one bounded,
canonical payload. Both Spark tasks consume that same XCom value; sessionization never performs a
second independent resolution. The DAG is the supported production concurrency boundary and is
serialized with `max_active_runs=1`.

This is a bounded batch pipeline scheduled `@daily`. It does not productionize the separate
`streaming_ingest-gh_archive-spark-iceberg` notebook. That scenario intentionally wraps a finite
locked file generation in Structured Streaming and waits indefinitely for teaching purposes. It
remains unscheduled and notebook-only. The synthetic Redpanda `events` producer belongs to another
scenario and is not a dependency of this pipeline.

## 2. Alternatives considered

### 2.1 One JAR, two Spark tasks in one DAG — selected

This keeps shared resolution, parsing, provenance, Spark, Iceberg, and Jenkins behavior in one
reviewed artifact while preserving an observable stage boundary. Airflow ordering and one immutable
XCom payload make the generation handoff explicit. Each transformation and entrypoint remains
independently testable.

### 2.2 Two JARs and two DAGs — rejected

Separate deployment units would isolate release cadence, but would duplicate the resolver and
runtime contracts and require an external cross-DAG generation handshake. The additional state and
scheduling race are not justified for two small, tightly coupled transforms.

### 2.3 One Spark submission for both stages — rejected

One driver would simplify orchestration, but would hide the required stage boundary, weaken
stage-specific failure evidence, and conflict with #109's independently reviewable acceptance
contract.

## 3. Repository and runtime components

The implementation adds `spark-apps/gh-archive-pipeline/` with:

- one `pom.xml` and reviewed shaded application JAR;
- shared strict source/provenance parsing;
- a flatten transformation and `GhArchiveFlatten` main class;
- a sessionization transformation and `GhArchiveSessionization` main class;
- Iceberg writers with readback validation;
- `dag.py` containing the resolver task and two Atlas REST-confirming Spark tasks;
- a Jenkinsfile that tests, packages, and publishes the exact reviewed JAR; and
- a README defining production use, recovery, trust boundaries, and consumer checks.

Airflow uses `spark_default`, cluster deploy mode,
`spark.standalone.submit.waitAppCompletion=true`, the Atlas `RestConfirmingSparkHook`, S3A and
Iceberg REST settings, and Spark event logs. Spark success is not inferred from submission alone:
each task must observe its distinct Spark driver in terminal `FINISHED` state with
`success=true` through the standalone REST endpoint on port 6066.

The DAG identifier is `gh_archive_flatten_sessionization`. Its task order is exact:

```text
resolve_gh_archive -> submit_gh_archive_flatten -> submit_gh_archive_sessionization
```

The exact Java entrypoints are
`com.thekaveh.dataeng.gharchive.GhArchiveFlatten` and
`com.thekaveh.dataeng.gharchive.GhArchiveSessionization`. Both receive the same arguments in this
order: every canonical `s3://` object URI in registry order, `--dataset-scale <scale>`,
`--plan-id <plan_id>`, `--publication-id <publication_id>`, and
`--manifest-sha256 <manifest_sha256>`. Sessionization validates the complete immutable inventory but
does not read those landing URIs; it reads only the provenance-matched flat table.

The schedule is `@daily`, `catchup=False`, and `max_active_runs=1`. The scale precedence is:

1. `dag_run.conf.dataset_scale` when present;
2. `DATASET_SCALE` when present;
3. `small` otherwise.

Only `tiny`, `small`, and `medium` are accepted. The DAG has one retry per Spark stage with a
two-minute retry delay. Retrying or rerunning uses the resolver payload retained for that DagRun;
it does not resolve a new generation between stages.

## 4. Immutable resolver contract

The resolver task sends exactly this request to the internal dataset resolver:

```json
{"dataset":"gh_archive","expected_scale":"tiny|small|medium"}
```

The request and response are bounded using the existing production limits: a 120-second request
timeout, at most 1 MiB of response bytes, at most 16 levels of JSON nesting, unique JSON keys, no
non-finite JSON values, and exact response fields:

```text
dataset, scale, plan_id, manifest_sha256, publication_id, objects
```

The resolver task validates the response and returns it as one canonical UTF-8 JSON string using
lexicographically sorted keys and compact separators. The canonical XCom value is limited to 1 MiB
and contains no credentials, tokens, endpoints, environment values, or other secrets. It contains
only the public dataset identifier, requested scale, immutable identifiers, object names, canonical
S3 URIs, sizes, SHA-256 digests, and schema IDs returned by the resolver.

Each Spark operator pulls the same `resolve_gh_archive` XCom value, rejects a non-string,
over-limit, non-canonical, duplicate-key, malformed, deeply nested, extra-field, or otherwise invalid
payload, and constructs arguments only after repeating the exact semantic validation. The flatten
and sessionization tasks therefore receive byte-identical generation identity and inventory. Neither
operator calls the resolver.

### 4.1 Exact registry inventory

The current reviewed registry order is chronological artifact order, not an inferred directory or
lexicographic listing:

| Scale | Exact object order |
|---|---|
| `tiny` | `2023-01-01-0.json.gz` |
| `small` | `2023-01-01-0.json.gz`, `2023-01-01-1.json.gz`, `2023-01-01-2.json.gz` |
| `medium` | `2023-01-01-0.json.gz`, `2023-01-01-1.json.gz`, `2023-01-01-2.json.gz`, `2023-01-01-3.json.gz`, `2023-01-01-4.json.gz`, `2023-01-01-5.json.gz` |

Every object must have schema ID `gh_archive_consumed_fields`, a positive integer `size_bytes`, a
64-character lowercase hexadecimal SHA-256, and the exact URI:

```text
s3://landing/gh_archive/_generations/<plan_id>/<publication_id>/<object_name>
```

`plan_id` and `manifest_sha256` are 64-character lowercase hexadecimal values.
`publication_id` is a lowercase 32-character UUID4-style hexadecimal value. Duplicate, missing,
extra, reordered, flat-landing, wrong-schema, malformed, or cross-generation objects fail closed.
Only after validation does the application convert the exact leading `s3://` to `s3a://` for Spark.
There is no flat landing fallback, directory discovery, resolver bypass, network package install, or
automatic dataset refresh.

## 5. Flatten stage contract

### 5.1 Source validation

Spark reads every exact resolver URI as a finite gzip JSON-lines batch in resolver order with
`mode=FAILFAST`. The application validates the inferred nested schema rather than accepting
position- or coercion-based parsing. These consumed fields must exist with these exact source types:

| JSON path | Source type | Required |
|---|---|---|
| `id` | string | yes |
| `type` | string | yes |
| `actor.login` | string | yes |
| `repo.name` | string | yes |
| `created_at` | string | yes |

The registry schema is deliberately `minimum`, so unrelated GitHub payload fields are allowed and
ignored. A malformed JSON record, missing or wrongly typed required path, null or blank required
value, or duplicate `id` rejects the complete stage before any table replacement.

`created_at` accepts only the exact whole-second UTC representation
`yyyy-MM-dd'T'HH:mm:ss'Z'`, for example `2023-01-01T00:17:42Z`. Fractional seconds, numeric offsets,
missing `Z`, local timestamps, leading or trailing whitespace, invalid calendar dates, and ambiguous
or normalized timestamps are rejected. Spark runs with `spark.sql.session.timeZone=UTC` and corrected
time parsing. Validation requires all three conditions:

1. the input matches `^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$`;
2. strict parsing produces a non-null timestamp; and
3. formatting the timestamp back to the same literal UTC pattern equals the original string.

### 5.2 Exact output

The flatten stage deterministically replaces exactly
`lakehouse.silver.gh_events` with this ordered schema:

| Column | Spark type | Nullability | Meaning |
|---|---|---|---|
| `id` | string | non-null | unique GitHub event ID |
| `type` | string | non-null | GitHub event type |
| `actor_login` | string | non-null | `actor.login` |
| `repo_name` | string | non-null | `repo.name` |
| `created_at` | timestamp | non-null | strict whole-second UTC event time |

The namespace is exactly `lakehouse.silver`. The output contains one row per valid source record,
preserves duplicate non-key event values, and has a unique `id`. Before writing, the application
materializes and validates schema, non-null/blank constraints, ID uniqueness, and a positive row
count. After replacement it reads the table back and verifies the exact schema, row multiset, key,
row count, and provenance properties.

## 6. Sessionization stage contract

### 6.1 Matching-generation precondition

Immediately before reading event rows, the sessionization application queries
`lakehouse.silver.gh_events` table properties and requires all five exact values from the shared
resolver payload. It filters the five `data_eng_lab.dataset*` keys from ordinary Iceberg metadata;
unrelated system properties are allowed. A missing key, an unexpected additional
`data_eng_lab.dataset*` identity key, or a mismatched value fails before reading the source table and
before any write to `gh_sessions`.

It then reads `lakehouse.silver.gh_events` and validates the exact flatten schema, non-null/blank
constraints, unique event IDs, positive row count, and strict timestamp values. It does not read the
landing objects. This makes the flat Iceberg table the independently testable stage boundary.

### 6.2 Deterministic session semantics

Events are partitioned by `actor_login` and ordered by `(created_at, id)` ascending. The ID
tie-breaker makes simultaneous events deterministic. For each actor:

- the first ordered event has `previous_created_at = null`, `new_session = 1`, and
  `session_id = 1`;
- each later event has `previous_created_at` equal to the immediately preceding ordered event time;
- a gap strictly greater than 1,800 seconds has `new_session = 1` and increments `session_id`;
- a gap equal to or less than 1,800 seconds has `new_session = 0` and keeps the current
  `session_id`.

`previous_created_at` is nullable only for the first ordered event of each actor. `new_session` is
an exact non-null Spark `integer` restricted to 0 or 1. `session_id` is an exact non-null Spark
`long`, begins at 1 for each actor, and has no gaps beyond the ordered new-session transitions.

### 6.3 Exact output

The sessionization stage deterministically replaces exactly
`lakehouse.silver.gh_sessions` with this ordered schema:

| Column | Spark type | Nullability |
|---|---|---|
| `id` | string | non-null |
| `type` | string | non-null |
| `actor_login` | string | non-null |
| `repo_name` | string | non-null |
| `created_at` | timestamp | non-null |
| `previous_created_at` | timestamp | nullable only on the first actor event |
| `new_session` | integer | non-null |
| `session_id` | long | non-null |

The output preserves every flat-event row exactly once and keeps `id` as the unique row key. Before
writing, the application validates the exact schema, row conservation, keys, ordering-derived
columns, and a positive row count. After replacement it reads back and verifies the exact schema,
row multiset, unique key, row count, session invariants, and provenance properties.

## 7. Provenance and downstream trust

Both tables carry the exact five-key convention already established by #107 and #108:

```text
data_eng_lab.dataset=gh_archive
data_eng_lab.dataset.scale=<tiny|small|medium>
data_eng_lab.dataset.plan_id=<plan_id>
data_eng_lab.dataset.publication_id=<publication_id>
data_eng_lab.dataset.manifest_sha256=<manifest_sha256>
```

After the second write, sessionization re-reads both tables' properties and requires exact equality
with the shared intended generation and with each other. Consumers must query both Iceberg
`$properties` metadata tables and fail closed before downstream SQL if any of the five keys is
absent or differs. The application README and executable documentation tests provide the concrete
SQL comparison contract. This issue does not implement #83 or any other downstream consumer.

## 8. Failure, recovery, and concurrency

Iceberg does not provide a cross-table atomic transaction. The least harmful order is flatten first,
then sessionization: the downstream stage never runs before a successful matching flatten, and an
old `gh_sessions` table remains available if sessionization fails. That failure can temporarily leave
`gh_events` and `gh_sessions` on different generations, so provenance-aware consumers must reject
the pair.

The supported recovery is a same-DagRun task retry or a new serialized DAG run for the same immutable
generation. Both tables use deterministic replacement, so reprocessing converges the logical rows
and five properties; new Iceberg snapshot IDs are expected. Tests inject a failure before the flatten
write, between the two stage writes, and during each read/write/readback boundary, then prove a
same-generation rerun converges.

`max_active_runs=1` serializes the supported production path. Concurrent direct invocations of either
JAR entrypoint are unsupported because they bypass the coupled table lock and can interleave
replacements. The DAG propagates resolver, flatten, sessionization, Spark terminal-state, and readback
failures without marking the run successful.

## 9. Jenkins and artifact publication

Jenkins runs Maven tests, builds the shaded JAR, and publishes only the reviewed artifact to:

```text
s3a://jars/gh-archive-pipeline/0.1.0/app.jar
```

The Airflow DAG uses that exact URI. Spark, Iceberg, AWS/S3A, Scala, and logging runtime dependencies
remain provided by Atlas according to the existing Maven app convention. No Maven repository,
Atlas source, Git submodule, Compose registry, or dependency lock modification is required.

## 10. Test strategy

### 10.1 Scala and local Spark tests

Tests cover:

- exact argument and immutable inventory validation for all three scales;
- malformed, duplicate, missing, extra, reordered, zero-size, wrong-schema, wrong-digest,
  wrong-UUID, wrong-path, flat-path, and cross-generation inputs;
- nested source schema, extra payload fields, malformed records, wrong types, missing paths,
  null/blank values, duplicate IDs, and empty input;
- exact UTC timestamp acceptance and rejection of fractional, offset, local, whitespace,
  normalized, impossible, and ambiguous forms;
- exact flatten schema, types, keys, row conservation, and deterministic rows;
- deterministic timestamp/ID ordering, simultaneous events, first event, 1,799-, 1,800-, and
  1,801-second gaps, multiple actors, null actors/timestamps, output types, session continuity, and
  repeatability;
- provenance check ordering, missing/mismatched properties, read/source/write/readback failures,
  no write after a precondition failure, and same-generation recovery after partial failure.

### 10.2 Airflow, Jenkins, and documentation tests

Offline tests import the DAG without network calls and exercise `_effective_scale`, resolver request
and response parsing, canonical XCom validation, both operator `execute` paths, exact shared payload,
task ordering, `max_active_runs=1`, `@daily`, cluster mode, `spark_default`, wait-for-completion,
REST-confirming ownership, JAR/class/argument selection, event logging, failure propagation, and
bounded diagnostics. Separate contracts cover the Maven POM, Jenkins publication path, scenario
matrix, READMEs, site, wiki, diagrams, and notebook trust-boundary warnings.

## 11. Genuine live acceptance

The opt-in `RUN_INFRA=1` harness is specific to this DAG, namespace, artifact, and tables. It follows
the hardened exclusive-ownership pattern used by the existing production pipelines:

1. Fail before mutation if any project container exists in running, stopped, exited, or created
   state. Start and later stop only the stack owned by this test. Preserve named volumes.
2. Require an existing verified `tiny` GitHub Archive publication. Resolver failure fails closed;
   the harness never refreshes or mutates the dataset pointer. Operators may provision explicitly
   before acceptance with the supported bounded dataset CLI and then run verify-only.
3. Capture the active pointer's exact body and ETag before stack work and retain the resolver payload.
4. Build and publish the exact reviewed JAR, proving the local and object-store SHA-256 values match.
5. Record the DAG's initial pause state and keep the `@daily` DAG paused throughout controlled
   acceptance. Use two unique test-owned manual DagRun identifiers and reject any unexpected active
   or acceptance-window run.
6. For each of the two DagRuns, require Airflow success and exactly two new distinct Spark drivers:
   one flatten driver followed by one sessionization driver. All four drivers across the acceptance
   window must be distinct and terminal `FINISHED` with `success=true`.
7. Independently read and validate the resolver-verified gzip source objects, including exact object
   size and SHA-256, strict JSON consumed fields, timestamps, unique IDs, and source row count.
8. Query both Iceberg tables and independently prove `source rows = gh_events rows = gh_sessions
   rows`, exact schemas, unique IDs, representative type/session measures, all five equal properties,
   and nonempty results.
9. After the second run, require identical logical row counts, measures, and deterministic row
   checksums while allowing newer Iceberg snapshot IDs.
10. Require the active pointer body and ETag to be byte-for-byte unchanged, exactly the two owned
    DagRuns with no unexpected queued/running run, and the initial pause state restored.
11. Stop only the test-owned stack, assert zero project containers in all states, and preserve every
    named volume. Cleanup diagnostics must not mask the primary failure.

The tracked live report records exact replayable commands, artifact hash, resolver identity, owned
DagRun IDs, four Spark driver IDs and terminal states, source/event/session counts, schemas,
properties, measures, checksums, pointer equality, pause behavior, and teardown state. Secrets and
tokens are redacted.

## 12. Educational notebook and documentation reconciliation

The table identifiers are frozen from the executable notebooks:
`lakehouse.silver.gh_events` and `lakehouse.silver.gh_sessions`. Stale prose is corrected
consistently: `id` is a string, production sessionization consumes `gh_events`, and the session table
uses the exact eight-column schema in this design.

Both Zeppelin and Jupyter sessionization notebooks are updated to read `gh_events`, use the
deterministic `(created_at, id)` ordering, and demonstrate the same 30-minute boundary. The paired
flatten notebooks retain the same five-column logical transform. Notebook parity is tested.

All four notebooks remain educational surfaces, not equivalent production write paths. They write
the same table names but do not provide the production resolver/XCom handshake, five provenance
properties, DAG serialization, REST terminal confirmation, or complete readback validation. Running
them against production tables can invalidate the coupled generation contract. Production writes
must use `gh_archive_flatten_sessionization`; notebooks belong in an isolated educational run.

The canonical scenario matrix moves both rows from approved to existing production only after code,
offline gates, and genuine live acceptance pass. Scenario READMEs, generated site, wiki, notebook
index, Spark-app catalog, diagrams, go-live guide, root README, and changelog then agree on the JAR,
DAG, entrypoints, schedule, table contracts, and notebook boundary.

## 13. Scope boundaries and protected state

This issue does not:

- implement #83, #91, or another scenario child;
- productionize a Structured Streaming or Redpanda path;
- change the dataset registry, dataset locks, active pointer, or publication history;
- modify `uv.lock`, Atlas source, the Atlas gitlink, or nested Atlas state;
- delete named volumes or unrelated tables; or
- push, open a PR, or merge before independent specification and quality reviews.

The unrelated protected plan
`docs/superpowers/plans/2026-07-21-atlas-submodule-modernization.md` remains byte-for-byte unchanged.
