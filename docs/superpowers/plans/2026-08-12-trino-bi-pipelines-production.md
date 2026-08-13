# Trino BI Pipelines Production Implementation Plan

> **For implementers:** Follow the repository lifecycle, strict test-driven development, and
> verification-before-completion. Stop before push until independent spec and quality reviews pass.

**Goal:** Replace the two stale Trino scenario placeholders with two real, read-only Atlas Airflow
DAGs that return bounded canonical BI artifacts and enforce the exact TPC-H provenance contract.

**Architecture:** A consumer-owned `airflow-dags/trino_bi` package uses Airflow's installed HTTP
provider to execute a closed registry of fixed SELECT statements through the bounded Trino
`/v1/statement` protocol. `tpch_bi_query` validates matching five-key provenance and table snapshots
before and after a meaningful star-schema aggregate. `nyc_taxi_trino_daily` validates one unchanged
Bronze snapshot around a meaningful daily aggregate. Successful tasks return canonical typed XCom
records; neither task writes Iceberg.

**Runtime:** Python 3.11, Airflow 3.3.0, `apache-airflow-providers-http`, `requests`, Trino 482,
Iceberg REST/MinIO, pytest/Ruff, repository docs tooling.

**Design:** `docs/superpowers/specs/2026-08-12-trino-bi-pipelines-production-design.md`

## 1. Protected preflight and lifecycle state

**Read only:**

- `uv.lock`
- `datasets/registry.yaml`
- `infra` gitlink and nested status
- protected untracked `docs/superpowers/plans/2026-07-21-atlas-submodule-modernization.md`
- GitHub issues #83, #91, #107, #109 and Project fields

1. Record exact HEAD, branch, worktree, protected hashes, Atlas gitlink, nested status, issue states,
   and zero-container state.
2. Confirm #83 is Open/Todo, #91 is Open/Todo, completed dependencies are Closed/Done, and no
   conflicting PR exists.
3. Move only #83 to Project `In Progress` after the design and this plan are committed.
4. Never stage the protected untracked plan or ignored Graphify/review/progress artifacts.

## 2. Establish RED for the fixed query and artifact contracts

**Create:**

- `tests/trino_bi/test_contracts.py`
- `airflow-dags/trino_bi/__init__.py`
- `airflow-dags/trino_bi/contracts.py`

1. Write failing tests for the exact registered query names and SQL text categories:
   TPC-H properties, snapshots, schemas, BI aggregate, and source reconciliation; NYC snapshot,
   schema, daily aggregate, and source count.
2. Test the conservative read-only validator with valid `SELECT`/`WITH` statements and rejection of
   semicolons, comments, multiple statements, interpolation markers, unbalanced quotes/parentheses,
   DDL, DML, transaction, session, permission, and procedure keywords. Prove every registry entry
   passes the validator and an injected drift entry fails before transport.
3. Test exact TPC-H five-key parsing: one row per key per table, nonblank values, equality,
   `dataset=tpch`, scale enum, lowercase hex plan/manifest, UUID4 publication. Cover missing,
   duplicate, extra identity, blank, malformed, and unequal rows.
4. Test exact TPC-H schemas, five ordered output segments, canonical decimal strings, positive and
   reconciled revenue/line/order totals, complete joins, pre/post snapshot and provenance equality,
   and every failure before an artifact is returned.
5. Test NYC required schema/types, nonempty source, ISO unique ordered dates, positive counts,
   finite canonical average strings, 4,000-row/256-KiB bounds, source-count equality, and unchanged
   snapshot.
6. Test artifact encoding: deterministic row ordering, date/decimal/float normalization, sorted
   compact UTF-8 JSON, no NaN/Infinity, exact checksum bytes, canonical reserialization, allowed
   fields only, and no endpoint/user/header/SQL/error/secret data.
7. Run and preserve named RED evidence:

   ```bash
   uv run pytest tests/trino_bi/test_contracts.py -vv
   ```

8. Implement the minimum pure contract/query/artifact code, rerun GREEN, Ruff the files, and commit:

   ```bash
   git add airflow-dags/trino_bi/__init__.py airflow-dags/trino_bi/contracts.py \
     tests/trino_bi/test_contracts.py
   git commit -m "feat: define bounded Trino BI contracts"
   ```

## 3. Establish RED for the bounded Trino HTTP protocol

**Create:**

- `airflow-dags/trino_bi/client.py`
- `tests/trino_bi/test_client.py`

1. Build fake Airflow connection, HTTP session, response, monotonic clock, and cancellation fixtures.
2. Write failing tests for the exact `trino_default` origin, no credentials, no host fallback, no
   import-time connection/network, and the required `X-Trino-User`, `Source`, `Catalog`, and fixed
   schema headers.
3. Test initial POST body equality to the fixed registry statement; reject redirects and unexpected
   status codes. Test valid single-page and multipage responses with absent/data-less interim pages.
4. Test query-result validation: exact stable query ID, exact declared columns/types, row widths,
   JSON object/data shapes, terminal state, and Trino error objects.
5. Test same-scheme/host/port/path `nextUri`, and reject user info, query leakage, fragments, host,
   port, scheme, path, repeated URI, and non-progressing pagination.
6. Test every hard bound independently: per-request bytes, total bytes, pages, requests, rows,
   columns, cells/strings, nesting, timeout, and whole-query deadline. Prove no unbounded body read or
   collection occurs.
7. Test response/session closure and best-effort same-origin DELETE on timeout, bound, protocol
   error, contract validation error, and explicit cancellation. Test cleanup failure preserves the
   primary error.
8. Test bounded redaction: errors contain only query name/id/status/category/counts and never contain
   endpoint query/user info, header values, SQL/body, response body, credentials, or secrets.
9. Run RED, implement the minimum client/hook, run GREEN and Ruff, then commit:

   ```bash
   uv run pytest tests/trino_bi/test_client.py -vv
   uv run ruff check airflow-dags/trino_bi tests/trino_bi
   git add airflow-dags/trino_bi/client.py tests/trino_bi/test_client.py
   git commit -m "feat: add bounded Trino HTTP runtime"
   ```

## 4. Establish RED for task attempt ordering and DAG contracts

**Create:**

- `airflow-dags/trino_bi/tasks.py`
- `airflow-dags/trino_bi/dag.py`
- `tests/trino_bi/test_tasks.py`
- `tests/trino_bi/test_dag.py`

1. Write task tests using a recording fake client. For TPC-H require exact call order: preflight
   properties, snapshots, schemas, source measures, BI query, postflight properties/snapshots, then
   canonical artifact. Inject each boundary failure and prove no later query/artifact runs.
2. Test all TPC-H mismatch/adversarial result cases and postflight source change. Prove BI SQL is
   never submitted when the five-key preflight fails.
3. For NYC require exact snapshot/schema/count/query/post-snapshot order. Inject every source,
   transport, result, reconciliation, and postflight failure and prove no accepted artifact.
4. Prove task return values are exactly the canonical XCom dictionaries and only appear after all
   checks. Prove retry/repartitioned response ordering converges to the same bytes/checksum.
5. Write DAG import tests with isolated fake Airflow modules and import-time network traps. Assert
   exact DAG IDs, task IDs, owners, cron schedules, UTC start, `catchup=False`, one retry/two-minute
   delay, `max_active_runs=1`, fixed `trino_default`, no `params`, no DagRun SQL/config, and no
   Trino provider/client import.
6. Run RED, implement tasks/DAG, run GREEN and Ruff, then commit:

   ```bash
   uv run pytest tests/trino_bi/test_tasks.py tests/trino_bi/test_dag.py -vv
   uv run ruff check airflow-dags/trino_bi tests/trino_bi
   git add airflow-dags/trino_bi/{tasks.py,dag.py} tests/trino_bi/{test_tasks.py,test_dag.py}
   git commit -m "feat: orchestrate read-only Trino BI tasks"
   ```

## 5. Establish RED for consumer mounting and execution-mode truth

**Modify:**

- `compose/data-eng-lab.yml`
- `scripts/scenario_execution.py`
- `scenarios/execution-modes.yaml`
- `tests/scripts/test_consumer_manifest.py`
- `tests/scenarios/test_execution_modes.py`
- `tests/test_atlas_usage_contract.py`
- `tests/test_dag_catalog_conf.py`

1. Add failing tests that the scheduler and DAG processor each receive exactly one read-only
   `../airflow-dags:/opt/airflow/dags/data_eng_lab_airflow_dags:ro` mount.
2. Add failing tests for exact `AIRFLOW_CONN_TRINO_DEFAULT` internal HTTP origin with no password,
   token, host fallback, or secret interpolation, on the task runtime only.
3. Generalize production-entrypoint validation to allow only `spark-apps/**/dag.py` and
   `airflow-dags/**/dag.py`, compare the full discovered inventory, permit the two Trino rows to share
   one entrypoint, and continue prohibiting scenario-local DAGs and unsafe paths.
4. Keep both rows approved during RED. After code/focused/live acceptance, promote them together to
   `existing production DAG`, clear `child_issue`, and set the shared exact entrypoint.
5. Run focused RED/GREEN:

   ```bash
   uv run pytest tests/scripts/test_consumer_manifest.py tests/scenarios/test_execution_modes.py \
     tests/test_atlas_usage_contract.py tests/test_dag_catalog_conf.py -vv
   ```

6. Commit the runtime integration before matrix promotion:

   ```bash
   git add compose/data-eng-lab.yml scripts/scenario_execution.py \
     tests/scripts/test_consumer_manifest.py tests/scenarios/test_execution_modes.py \
     tests/test_atlas_usage_contract.py tests/test_dag_catalog_conf.py
   git commit -m "feat: mount consumer Trino DAGs"
   ```

## 6. Build a genuine offline-tested live harness

**Create:**

- `tests/scenarios/test_trino_bi_pipelines_live.py`
- `tests/scenarios/test_trino_bi_pipelines_live_harness.py`

1. Reuse only the hardened patterns, not imports, from the #107/#109 live harnesses: all-state
   project-container ownership preflight, owned stack cleanup, pause restoration, bounded complete
   Airflow-v2 pagination, exact DagRun set differences, command redaction, and pointer-body/ETag
   capture.
2. Write offline RED tests proving:
   - any pre-existing running/stopped/created project container rejects before mutation;
   - only an owned stack is stopped and owned-failure cleanup preserves the primary error;
   - both DAGs stay paused, their initial pause states restore, and unexpected active/new runs fail;
   - pagination is complete and bounded, including an unexpected run on page 2;
   - TPC-H resolver/pointer failure never triggers download, refresh, or retry; NYC is never
     resolved or verified, and its optional pointer distinguishes explicit absence from ambiguous
     or malformed reads while requiring exact state equality;
   - exact two owned runs per DAG and no third are required;
   - Trino query inventory, XCom retrieval, no Spark driver delta, source metadata/pointer equality,
     and zero-container teardown are asserted from runtime state rather than report strings.
3. The opt-in live test must skip safely unless `RUN_INFRA=1`. It must require an existing verified
   tiny TPC-H publication and existing NYC Bronze table, fail closed without mutating them, and
   never resolve, verify, or publish NYC raw data.
4. Run the offline harness suite RED/GREEN and commit:

   ```bash
   uv run pytest tests/scenarios/test_trino_bi_pipelines_live_harness.py -vv
   uv run ruff check tests/scenarios/test_trino_bi_pipelines_live*.py
   git add tests/scenarios/test_trino_bi_pipelines_live.py \
     tests/scenarios/test_trino_bi_pipelines_live_harness.py
   git commit -m "test: add Trino BI live acceptance harness"
   ```

## 7. Canonical live acceptance

**Create after successful replay:**

- `docs/superpowers/reports/2026-08-12-trino-bi-pipelines-live-acceptance.md`

1. Verify zero project containers, protected hashes, the existing tiny TPC-H publication, and the
   exact optional NYC pointer state without invoking an NYC resolver/acquisition workflow.
2. Start the canonical stack without cold cleanup. Prove inside the actual Airflow task runtime:
   exact HttpHook/requests imports, no required Trino provider/client, exact connection origin, DAG
   parse health, and both DAGs paused.
3. Snapshot input table schemas/properties/snapshots, the mandatory TPC-H pointer body/ETag, the
   exact absent-or-present NYC pointer state, and the complete Spark driver inventory.
4. Run two unique controlled paused DagRuns per DAG via `airflow dags test --use-executor`, with
   whole-second logical dates and bounded output. Require exact run-set differences and terminal
   Airflow success.
5. Retrieve each task's XCom through the actual Airflow runtime/API. Assert exact canonical bytes,
   schema/types/rows/measures/query IDs/checksums, TPC-H five-key values/source reconciliation,
   NYC snapshot/source count, and byte/checksum equality across each rerun.
6. Query Trino runtime inventory and require every owned query terminal `FINISHED` with no error.
   Require no unexpected owned queries and no endpoint/query/body leakage in logs.
7. Re-read Iceberg snapshots/properties, pointers, and Spark drivers. Require exact equality and zero
   Spark driver delta.
8. Restore both initial pause states, stop only the owned stack with `scripts/stop-all.sh`, preserve
   volumes, and assert zero project containers in all states.
9. Record exact replayable commands, DagRun IDs, Trino query IDs, source identities, artifacts,
   checksums, timings, pause/ownership/cleanup evidence, and redacted credentials in the report.
10. Commit the evidence separately:

    ```bash
    git add docs/superpowers/reports/2026-08-12-trino-bi-pipelines-live-acceptance.md
    git commit -m "docs: record Trino BI live acceptance"
    ```

## 8. Reconcile notebooks, public documentation, matrix, and diagrams

**Modify:**

- both scenario Jupyter and Zeppelin notebooks
- both scenario READMEs
- `scenarios/execution-modes.yaml`
- generated `docs/scenarios/execution-modes.md`
- `docs/notebooks/index.md` and generated notebook/scenario projections
- `docs/diagrams/bi_query-tpch-trino-iceberg.html`
- `docs/diagrams/federated_query-nyc_taxi-trino-iceberg.html`
- `docs/diagrams/overview.html` where the production count/path appears
- `docs/go-live-results.md`
- `docs/CHANGELOG.md`
- `README.md`, `docs/index.md`, and manifest/navigation counts when required
- focused docs/notebook tests

1. First write RED assertions for exact production classification, shared entrypoint, two new DAG
   IDs/schedules, read-only XCom output, NYC snapshot-bound wording, TPC-H five-key preflight, no
   current #268 blocker, and accurate production counts.
2. Correct stale BI source/output names/types and the historical Bronze TPC-H SQL. Correct
   `federated` wording to cross-engine Iceberg analytics. Keep notebook output tables educational
   only and warn that notebook CTAS is not the production artifact/security/provenance path.
3. Preserve explicitly historical evidence as historical; remove only claims that #268 is a current
   blocker.
4. Promote both canonical matrix rows only because code, focused tests, and live acceptance passed.
5. Regenerate deterministic projections, diagram PNGs, site, and wiki using repository tools.
6. Run focused docs gates and commit:

   ```bash
   uv run pytest tests/scenarios/test_execution_modes.py tests/test_docs_content_contract.py \
     tests/scenarios/test_build_notebooks.py -vv
   make docs-check
   make docs-wiki
   git add <reviewed public documentation and generated projections>
   git commit -m "docs: publish production Trino BI workflows"
   ```

## 9. Full verification and review handoff

1. Run focused suites:

   ```bash
   uv run pytest tests/trino_bi tests/scenarios/test_trino_bi_pipelines_live_harness.py -vv
   uv run ruff check airflow-dags/trino_bi tests/trino_bi \
     tests/scenarios/test_trino_bi_pipelines_live*.py
   ```

2. Run repository-wide gates:

   ```bash
   make lint
   make test
   make verify
   make docs-check
   make docs-wiki
   make docs-diff
   make compose-validate
   mvn -q -B -f spark-apps/tpch-star-schema/pom.xml test
   mvn -q -B -f spark-apps/gh-archive-pipeline/pom.xml test
   ```

3. Run `git diff --check`, execution-mode rendering/check, all relevant compose/config probes, and
   confirm the complete diff is scoped to #83.
4. Recompute and compare protected plan, `uv.lock`, registry, Atlas gitlink/nested status, issue #91,
   and zero-container invariants.
5. Generate the ignored exact binary review package from base `c6810e5` to HEAD and record its
   SHA-256 in the ignored progress/report ledger.
6. Request independent spec-compliance and code-quality reviews. Fix any verified finding under new
   RED evidence and repeat relevant offline/live/full gates.
7. Stop before push or PR. Report exact commits, RED/GREEN counts, live IDs/query IDs/artifacts,
   gate outputs, issue/Project state, protected invariants, zero containers, and review-package path
   and SHA.
