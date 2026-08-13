# Trino BI Pipelines Production Design

**Issue:** #83

**Date:** 2026-08-12
**Status:** Approved for implementation

## 1. Decision

Productionize the two approved Trino scenarios as two independently runnable Atlas Airflow DAGs
backed by one consumer-owned, read-only Trino HTTP runtime:

1. `tpch_bi_query` reads `lakehouse.gold.dim_customer` and
   `lakehouse.gold.fct_orders`, rejects missing or mixed five-key provenance before any BI SQL,
   and returns a validated segment-revenue artifact.
2. `nyc_taxi_trino_daily` reads `lakehouse.bronze.nyc_taxi_trips`, binds the attempt to one
   unchanged Iceberg snapshot, and returns a validated daily-fare artifact.

Each successful task returns one canonical, typed, bounded JSON-compatible dictionary through
Airflow XCom. XCom is the durable metadata-database record for that DagRun and task; it is not an
Iceberg table, a dataset publication, or an indefinitely retained BI warehouse. Operators retrieve
it through the Airflow task-instance/XCom UI or API while their deployment's XCom retention policy
keeps the DagRun. The DAG logs repeat only bounded identifiers, counts, and checksums rather than
the complete artifact.

The implementation adds `airflow-dags/trino_bi/` and mounts `airflow-dags/` read-only into the
Airflow scheduler and DAG processor. It does not add a Spark or Maven application, change the
pinned Atlas source, install a Trino provider or Python client, change `uv.lock`, or modify a
producer. Issue #91 and unrelated scenario children remain out of scope.

## 2. Runtime evidence and alternatives

The pinned Atlas Airflow 3.3.0 image contains
`apache-airflow-providers-http`, `apache-airflow-providers-common-sql`, and `requests`, but it does
not contain `apache-airflow-providers-trino`, `TrinoOperator`, or the `trino` Python client. The
repository host-only `live` dependency group contains `trino`; that is not task-runtime evidence.
Atlas provides Trino 482 at the internal Compose endpoint `http://trino:8080` and an Iceberg REST
catalog named `lakehouse`.

### 2.1 Consumer-owned Trino HTTP hook and Python tasks — selected

The selected runtime uses Airflow's installed HTTP provider to obtain a session from
`trino_default`, then implements the documented Trino `/v1/statement` protocol with strict bounds.
The overlay supplies `AIRFLOW_CONN_TRINO_DEFAULT` with only the internal HTTP endpoint and no
credentials. There is no host endpoint fallback.

This is the smallest dependency-free task surface proven in the actual image. The query registry,
transport, artifact validation, and two DAG tasks are ordinary Python and can be tested without
import-time network access.

### 2.2 Consumer-derived image with the Trino client or provider — rejected

The official client would reduce protocol code, but it expands the Airflow image and dependency
lock boundary solely for two small bounded queries. It also weakens the issue's proof that the
selected API exists in the pinned runtime. Reconsider only if Trino authentication or protocol
features outgrow the bounded client.

### 2.3 Iceberg materialization with `CREATE OR REPLACE TABLE AS` — rejected

Materializing `bi_segment_revenue` and `nyc_taxi_daily_trino` would provide reusable tables, but it
would grant the orchestration task DDL/DML authority, introduce snapshot and partial-recovery state,
and require Atlas catalog changes to allow arbitrary five-key output properties. The approved
production contract is read-only and returns bounded Airflow artifacts. The notebook CTAS examples
remain educational surfaces and are not an equivalent production write path.

## 3. Repository and orchestration contract

`airflow-dags/trino_bi/` contains:

- a bounded HTTP hook/client;
- a fixed query registry and typed artifact validators;
- `dag.py` defining both production DAGs; and
- a README documenting retrieval, schedules, security, failure, and recovery.

The consumer overlay mounts the directory at
`/opt/airflow/dags/data_eng_lab_airflow_dags` for the scheduler and DAG processor. The execution-mode
validator accepts truthful production entrypoints under either `spark-apps/` or `airflow-dags/` and
still prohibits scenario-local DAGs. Both matrix rows share
`airflow-dags/trino_bi/dag.py` while remaining separate DAGs and query contracts.

Both DAGs have owner `data-eng-lab`, `catchup=False`, `max_active_runs=1`, one retry after two
minutes, explicit UTC start semantics, and no configurable SQL. `tpch_bi_query` runs at
`0 1 * * *`; `nyc_taxi_trino_daily` runs at `0 2 * * *`. These staggered schedules follow the
current daily producer windows without coupling producer success to downstream availability.
Readiness checks fail closed and retry. Manual triggers use the same fixed contracts and accept no
query, endpoint, identity, table, or credential override.

## 4. Trino HTTP protocol contract

The Airflow connection is exactly `trino_default`, resolves only to `http://trino:8080`, has no
password, and cannot be replaced by a host endpoint environment variable. Initial statement
requests use:

- `POST /v1/statement` with one fixed SQL statement as the body;
- `X-Trino-User: data_eng_lab_bi`;
- `X-Trino-Source: data-eng-lab-airflow`;
- `X-Trino-Catalog: lakehouse`; and
- `X-Trino-Schema` when the registered query has a fixed schema context.

The client rejects redirects. Every `nextUri` must retain the initial `http` scheme, `trino` host,
and port `8080`; its nonempty `/v1/statement/` path segments may contain only unreserved ASCII and
must not be dot segments. Percent-encoded bytes, Unicode, matrix parameters, backslashes, duplicate
separators, credentials, fragments, unexpected paths, origin changes, or malformed URIs fail
closed. It follows pages only until the terminal document has no `nextUri` and validates every HTTP
status, JSON object, query ID, column name/type declaration, row width, and data container.

Bounds are explicit and centralized: request timeout, whole-query deadline, maximum requests and
pages, maximum bytes per response and in total, maximum rows, maximum columns, maximum cell/string
size, and maximum JSON depth. Repeated or non-progressing `nextUri`, query-ID changes, missing
terminal state, Trino error objects, malformed JSON, non-finite numbers, and bound exhaustion fail
closed. Responses are closed in all paths. When a live `nextUri` is known, timeout, cancellation,
bound, validation, or downstream result failure causes a best-effort same-origin `DELETE`; cleanup
failure never replaces the primary exception.

Errors and logs are bounded and redacted. They never expose connection user info, headers, query
body, URI query strings, response bodies, credentials, or environment values. They may expose a
registered query name, Trino query ID, HTTP status class, validation category, and bounded counts.

### 4.1 Fixed read-only registry

Callers pass a closed query-name enum, not SQL. Every registry entry is a constant statement with
fixed catalog, schema, tables, aliases, order, and limits. Before execution, a conservative
read-only validator removes quoted literals/identifiers and rejects semicolons, comments,
multi-statements, unbalanced tokens, and any DDL/DML/control keyword including `CREATE`, `ALTER`,
`DROP`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `CALL`, `GRANT`, `REVOKE`, `SET`, `RESET`, `START`,
`COMMIT`, and `ROLLBACK`. A registered statement must begin with `SELECT` or `WITH` and contain no
external substitution marker. Tests prove that accidental registry drift cannot gain write access.

Atlas Trino itself currently has no authenticator and its Iceberg catalog is not configured
`READ_ONLY`. This design therefore provides application-enforced least authority, not an
infrastructure security claim. Network authentication and catalog authorization belong to a
separate Atlas hardening issue.

## 5. Canonical XCom artifact

No accepted artifact is returned or pushed until all preflight, query, reconciliation, and postflight
checks pass. The artifact contains only public table identity, snapshot/provenance values, exact
column descriptors, normalized rows, row count, Trino query IDs, and checksums. It contains no
endpoint, user, header, SQL, secret, token, or raw error.

Dates serialize as `YYYY-MM-DD`. Decimal values serialize as canonical base-10 strings with their
contract scale, never binary floats or exponent notation. Integer values remain JSON integers.
Floating averages must be finite and serialize using one documented canonical decimal-string
normalization. Rows are sorted by the registered output key. Canonical bytes use UTF-8,
lexicographically sorted object keys, compact JSON separators, no NaN/Infinity, and a trailing-byte
free representation. `result_sha256` is the lowercase SHA-256 of the exact canonical result payload
before the checksum field is added. Re-serialization must be byte-identical. The full XCom has a
hard byte limit.

## 6. TPC-H BI contract

### 6.1 Mandatory fail-closed preflight

The task queries these metadata tables before any BI SQL:

- `lakehouse.gold."dim_customer$properties"`
- `lakehouse.gold."fct_orders$properties"`

It requires exactly one nonblank value for each exact key in each table:

1. `data_eng_lab.dataset`
2. `data_eng_lab.dataset.scale`
3. `data_eng_lab.dataset.plan_id`
4. `data_eng_lab.dataset.publication_id`
5. `data_eng_lab.dataset.manifest_sha256`

Both maps must be equal. `dataset` must be `tpch`; scale must be `tiny`, `small`, or `medium`;
plan/manifest are lowercase 64-hex; publication is the canonical lowercase UUID4-style 32-hex
identifier. Missing, duplicate, blank, malformed, or unequal values fail before the BI query.

The task also captures the current snapshot ID of both tables and validates the exact #107 schemas:

- `dim_customer(c_custkey bigint, c_name varchar, c_nationkey integer,
  c_mktsegment varchar)`; and
- `fct_orders(o_orderkey bigint, o_custkey bigint, o_orderdate date,
  revenue decimal(25,2), line_count bigint)` using Trino 482's information-schema value spelling.

### 6.2 BI result and reconciliation

The fixed BI query joins on `fct_orders.o_custkey = dim_customer.c_custkey`, groups by market
segment, and returns in ascending segment order:

| Column | Trino type | Contract |
|---|---|---|
| `market_segment` | `varchar` | unique, nonblank |
| `total_revenue` | `decimal(38, 2)` | canonical positive decimal string |
| `line_count` | `bigint` | positive |
| `order_count` | `bigint` | positive and no greater than line count |

Exactly five segments are required. Independent fixed source queries require nonempty tables,
complete customer joins, and exact equality between BI totals and source fact revenue, line count,
and order count.

Immediately after reconciliation, the task rereads both five-key property maps and snapshot IDs.
Every value must equal the preflight value. A producer change, mixed generation, or metadata change
causes failure and no accepted XCom.

## 7. NYC Taxi daily contract

The NYC Bronze producer predates five-key Iceberg provenance. This task is therefore explicitly
snapshot-bound, not resolver-generation-bound. The live gate does not resolve, verify, publish, or
refresh an NYC raw publication. Instead, it reads the optional active-pointer control key directly:
explicit `NoSuchKey` is a captured absent state, while a present key requires bounded valid JSON
bytes and a nonblank ETag. That exact absent or present state must be unchanged afterward. Ambiguous
authorization, transport, or malformed-response failures fail closed and are never interpreted as
absence. This is negative-control evidence that the consumer performed no dataset mutation; it does
not establish Bronze table provenance.

Before aggregation, the task captures the exact current Iceberg snapshot ID, validates that the
source is nonempty, and requires the needed source columns and types, including `trip_date date` and
numeric `fare_amount`. The fixed query returns in ascending date order:

| Column | Trino type | Contract |
|---|---|---|
| `trip_date` | `date` | ISO date, unique, nonnull |
| `trip_count` | `bigint` | positive |
| `avg_fare` | `double` | finite canonical decimal string |

The result is limited to 4,000 rows and the XCom to 256 KiB. `sum(trip_count)` must equal an
independent source count. The task rereads the source snapshot after validation and fails without an
accepted XCom unless it is unchanged.

Extending `nyc-taxi-etl` to persist generation provenance is useful future hardening, but it would
expand #83 into a producer migration and is not claimed here.

## 8. Failure, concurrency, and recovery

The tasks perform no Iceberg writes, so there is no partial table state to roll back. A failed
attempt may have submitted read queries to Trino but publishes no accepted XCom. Best-effort query
cancellation limits abandoned work. Retrying the same attempt against unchanged sources produces
the same canonical result and checksum. If a source changes before or during a retry, the new
attempt either binds truthfully to its stable identity or fails the pre/post identity comparison.

`max_active_runs=1` serializes each supported DAG and prevents duplicate artifacts for overlapping
runs. The two read-only DAGs may overlap each other. Direct helper use outside Airflow is a test and
diagnostic surface, not the supported production scheduling boundary.

## 9. Testing and live acceptance

Offline tests cover:

- exact pinned-runtime import evidence and absence of an assumed Trino provider/client;
- connection origin and no host fallback;
- fixed query registry, read-only validation, headers, POST/GET/DELETE behavior, redirects,
  same-origin pagination, bounds, closure, cancellation, error precedence, and redaction;
- malformed/error/duplicate/non-progressing protocol documents, query IDs, columns, types, rows,
  values, and terminal states;
- TPC-H exact five-key preflight ordering, all missing/duplicate/blank/malformed/mismatch cases,
  schemas, measures, reconciliation, postflight identity, and no BI SQL after preflight failure;
- NYC schema, snapshot, row/date/fare/count validation and snapshot-change rejection;
- canonical decimal/date/float encoding, row order, exact bytes/checksum, XCom bounds, retrieval
  contract, and no artifact before complete success;
- two DAG IDs, owners, schedules, retries, serialization, isolated imports, no import-time network,
  matrix/overlay mounts, and no arbitrary DagRun configuration;
- notebook, README, diagram, site, wiki, go-live, and changelog truth.

The genuine `RUN_INFRA=1` gate requires an already prepared stack with the existing reviewed tiny
TPC-H outputs and existing NYC Bronze table. It never refreshes or mutates a dataset pointer. It:

1. fails closed if any project container already exists, starts an exclusively owned stack, and
   proves the exact task imports inside the pinned Airflow image;
2. keeps both daily DAGs paused, records and restores initial pause state, and rejects unexpected
   active or newly created DagRuns using complete bounded Airflow-v2 pagination;
3. snapshots the mandatory TPC-H pointer body/ETag, the exact optional NYC pointer state, all input
   Iceberg snapshot/property state, and the Spark driver inventory;
4. runs two controlled paused DagRuns for each DAG through `airflow dags test --use-executor`;
5. requires terminal Airflow success, exact XCom artifacts, terminal successful Trino query
   inventories, meaningful measures, source reconciliation, frozen five-key TPC-H provenance,
   frozen NYC snapshot identity, and byte/checksum equality across reruns;
6. proves zero Spark driver delta and zero mutation to Iceberg snapshots, properties, or raw
   pointer bodies/ETags; and
7. stops only its owned stack without cold cleanup, preserves volumes, restores pause state, and
   asserts zero project containers in every Docker state.

The completed #109 output tables are not inputs to either #83 scenario. #109 contributes the
hardened acceptance-harness patterns only; the design does not invent a GitHub Archive dependency.

Only after this live gate passes may both execution-mode rows change from `approved new production
DAG` to `existing production DAG`. The scenario READMEs, both notebook languages, diagrams,
generated site, wiki, go-live record, and changelog must then agree that production is a read-only
Airflow artifact path and the notebook CTAS path is educational only. References to Atlas #268 may
remain solely in explicitly historical records.
