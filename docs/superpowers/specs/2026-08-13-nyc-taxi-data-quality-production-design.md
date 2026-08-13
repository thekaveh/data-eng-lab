# NYC Taxi Data Quality Production Design

**Issue:** #91

**Date:** 2026-08-13
**Status:** Approved for implementation

## 1. Decision and scope

Productionize `data_quality-nyc_taxi-spark-iceberg` as one Maven-built Scala Spark application and
one operator-owned Atlas Airflow DAG. The application reads one stable Iceberg snapshot of
`lakehouse.bronze.nyc_taxi_trips`, materializes an exact null-safe Silver clean/quarantine
partition, and persists versioned run-level facts in
`lakehouse.gold.nyc_taxi_quality_facts`. A fixed, bounded, read-only Trino query registry is the
durable dashboard/query surface.

The production DAG is `nyc_taxi_data_quality`. It is scheduled `@daily`, serialized with
`max_active_runs=1`, and first waits for the same-logical-date `nyc_taxi_etl` task to succeed. The
quality application then captures and rechecks the exact Bronze Iceberg snapshot. Spark completion
is accepted only after Atlas's REST-confirming hook observes `FINISHED` with `success=true` and the
application has read back the Silver tables and durable facts.

This issue does **not** retrofit the upstream `nyc_taxi_etl` producer. That producer resolves one
immutable raw generation, but passes only URI arguments to Spark and writes no
`data_eng_lab.dataset*` Iceberg properties. The Bronze input is therefore snapshot-bound, not
resolver-generation-bound. The same-logical-date Airflow dependency is orchestration evidence; it
does not turn a snapshot ID into plan, publication, manifest, or scale provenance.

## 2. Alternatives considered

### 2.1 Snapshot-bound quality product — selected

This approach implements the complete #91 monitoring product without migrating a completed
producer contract. It binds every accepted run to one positive Bronze snapshot ID, its commit time,
the exact source schema, and the successful same-logical-date ETL task. It is honest about the
remaining provenance limitation and matches the boundary already established for NYC Taxi by #83.

### 2.2 Retrofit five-key provenance into `nyc_taxi_etl` — deferred

Passing the complete resolver identity into the ETL JAR and persisting the five #107/#108 keys on
Bronze would provide stronger generation lineage. It would also change #78's shipped producer,
require a Bronze migration/backfill and new producer live acceptance, and exceed #91's minimum
quality scope. It should be a separate upstream hardening change. Until then, no #91 table, fact,
dashboard, or document may claim five-key Bronze provenance.

### 2.3 Recompute raw-to-Bronze equivalence in every quality run — rejected

Re-reading the raw publication and replaying the upstream transform would duplicate ETL cost and
logic while still not proving which deployed ETL artifact wrote Bronze. The additional runtime and
drift surface are not justified.

## 3. Runtime and orchestration contract

The implementation adds `spark-apps/nyc-taxi-data-quality/` with a Spark 4.1.2 / Scala 2.13 Maven
application, ScalaTest coverage, Jenkins publication, production DAG, README, and fixed Trino SQL
queries. Jenkins publishes the reviewed JAR to
`s3a://jars/nyc-taxi-data-quality/0.1.0/app.jar`. The exact Java entrypoint is
`com.thekaveh.dataeng.quality.NycTaxiDataQuality`.

The DAG uses `spark_default`, cluster deploy mode,
`spark.standalone.submit.waitAppCompletion=true`, the Atlas `RestConfirmingSparkHook`, the existing
Iceberg REST/S3A configuration, and Spark event logs. It has `catchup=False`, one retry after two
minutes, and `max_active_runs=1`. Direct concurrent JAR invocations are unsupported; Airflow
serialization is the production concurrency boundary.

The exact task order is:

```text
wait_for_matching_nyc_taxi_etl -> submit_nyc_taxi_data_quality
```

`wait_for_matching_nyc_taxi_etl` is an `ExternalTaskSensor` for DAG `nyc_taxi_etl`, task
`submit_nyc_taxi_etl`, and the quality DagRun's exact logical date. It accepts only `success`, treats
failed/upstream-failed/skipped as failure, checks that the external task exists, uses `reschedule`
mode, pokes every 60 seconds, and times out after 3,600 seconds. It never silently selects a latest
or merely recent upstream run. Controlled manual acceptance creates ETL and quality runs with the
same canonical logical date and proves the sensor, rather than bypassing it.

The Spark task receives only fixed table names plus three bounded canonical values rendered from
Airflow context:

1. `--logical-date <yyyy-MM-dd'T'HH:mm:ss'Z'>`;
2. `--data-interval-end <yyyy-MM-dd'T'HH:mm:ss'Z'>`; and
3. `--upstream-dag-id nyc_taxi_etl`.

Timestamps are strict UTC whole seconds. Arguments reject duplicates, missing/extra options,
non-ASCII/control content, values over 128 bytes, fractional seconds, offsets, whitespace, invalid
calendar values, arbitrary tables, or arbitrary thresholds. No secrets, connection values, raw
SQL, resolver payload, or user-selected rule configuration enter arguments or facts.

## 4. Bronze trust boundary and exact schema

Immediately before reading data, the application reads the Bronze `main` reference and latest
snapshot metadata and requires exactly one positive current snapshot ID and a canonical UTC commit
timestamp. It validates the table's exact ordered schema below. The current catalog inspection on
2026-08-13 confirmed this 20-column contract and only ordinary Iceberg properties; no
`data_eng_lab.dataset*` keys exist.

| Column | Spark type | Nullable |
|---|---|---|
| `VendorID` | long | yes |
| `tpep_pickup_datetime` | timestamp_ntz | yes |
| `tpep_dropoff_datetime` | timestamp_ntz | yes |
| `passenger_count` | double | yes |
| `trip_distance` | double | yes |
| `RatecodeID` | double | yes |
| `store_and_fwd_flag` | string | yes |
| `PULocationID` | long | yes |
| `DOLocationID` | long | yes |
| `payment_type` | long | yes |
| `fare_amount` | double | yes |
| `extra` | double | yes |
| `mta_tax` | double | yes |
| `tip_amount` | double | yes |
| `tolls_amount` | double | yes |
| `improvement_surcharge` | double | yes |
| `total_amount` | double | yes |
| `congestion_surcharge` | double | yes |
| `airport_fee` | double | yes |
| `trip_date` | date | yes |

The application selects those exact case-preserving Iceberg field names in that order. Trino's
information-schema presentation lowercases unquoted identifiers, but the Iceberg metadata JSON and
Spark schema preserve the mixed-case names shown above. Extra, missing,
reordered, wrongly typed, or differently nullable fields fail before a Silver write. The canonical
schema document is compact UTF-8 JSON: one array in field order whose objects have the sorted keys
`name`, `nullable`, and `type`; no whitespace or terminal newline is present. Spark canonical type
names are used (`timestamp_ntz` for both trip timestamps). Its frozen SHA-256 is
`5a8d2916cc5967c0eeb8318136c1262156cd616105dad67a713f1cb1cc872fc5` and is recorded with each fact.

The two trip timestamps are source-derived local civil times. The NYC Parquet logical annotation
has no UTC adjustment, so the Spark 4.1 ETL preserves them as `TimestampNTZType`; neither the ETL
nor this quality application may silently reinterpret them as UTC instants. Operational fact
timestamps (`logical_date`, `data_interval_end`, and `source_snapshot_committed_at`) remain Spark
`TimestampType` UTC instants.

Freshness is tied to the matching daily ETL window, not raw-event wall-clock recency. The current
snapshot commit must be at or after `data_interval_end - 6 hours`. This permits normal scheduler and
cluster delay while rejecting the previous daily snapshot. A commit before that bound is `stale`.
The application rereads the current snapshot and source schema after both Silver readbacks and
before facts are accepted; any change fails the run.

## 5. Null-safe Silver partition

The governed rule version is `nyc_taxi_quality_v1`. Its exact row predicate is:

```text
fare_amount > 0 AND passenger_count BETWEEN 1 AND 6
```

Production evaluates validity as `coalesce(predicate, false)`. A row is written to exactly one of:

- `lakehouse.silver.nyc_taxi_clean` when validity is true; or
- `lakehouse.silver.nyc_taxi_quarantine` when validity is false.

This deliberately corrects the teaching notebooks' current three-valued filter gap. A row with a
null rule operand is quarantined, never dropped between branches. Every source row, including exact
duplicates, is preserved once across the two-table output multiset. The two Silver tables use the
exact 20-column Bronze schema above; no quality metadata column is added to trip rows.

Before writing, the application proves the exact schema, `clean_count + quarantine_count =
source_count`, and bidirectional multiplicity-aware equality between Bronze and the union of both
outputs. After replacement, the same schema, counts, distributed row checksums, duplicate
multiplicity, null-operand behavior, predicate membership, and union multiset are read back and
verified.

Both Silver tables carry exactly these production-owned properties:

```text
data_eng_lab.quality.binding=iceberg_snapshot
data_eng_lab.quality.source_table=lakehouse.bronze.nyc_taxi_trips
data_eng_lab.quality.source_snapshot_id=<positive snapshot id>
data_eng_lab.quality.rule_version=nyc_taxi_quality_v1
data_eng_lab.quality.run_id=<quality_run_id>
```

The application rereads the properties and requires exact equality between the two outputs and the
intended snapshot/run. These are quality lineage properties, not the five-key dataset-generation
contract.

## 6. Durable quality facts

The application creates or validates `lakehouse.gold.nyc_taxi_quality_facts` with this exact ordered
schema:

| Column | Spark type | Nullable |
|---|---|---|
| `quality_run_id` | string | no |
| `logical_date` | timestamp | no |
| `data_interval_end` | timestamp | no |
| `dataset_id` | string | no |
| `binding_type` | string | no |
| `upstream_dag_id` | string | no |
| `source_table` | string | no |
| `source_snapshot_id` | long | yes |
| `source_snapshot_committed_at` | timestamp | yes |
| `source_schema_sha256` | string | yes |
| `layer` | string | no |
| `rule_id` | string | no |
| `rule_version` | string | no |
| `owner` | string | no |
| `metric_name` | string | no |
| `metric_numerator` | long | yes |
| `metric_denominator` | long | yes |
| `metric_value` | decimal(38,9) | yes |
| `warn_threshold` | string | yes |
| `fail_threshold` | string | no |
| `severity` | string | no |
| `status` | string | no |
| `diagnostic_code` | string | no |

All strings use a strict printable-ASCII allowlist and fixed per-field bounds; diagnostic codes are
from a closed registry and never include exception text, paths, endpoints, SQL, headers, or secrets.
Decimals use scale 9 and Trino renders them as fixed nine-place canonical strings; counts remain
exact longs. Logical date and interval end render as UTC whole-second ISO values. The immutable
Iceberg source commit retains its exact millisecond precision; freshness uses Java
`Duration.between(commit, intervalEnd).getSeconds`, including floor semantics for negative
fractional durations when a matching ETL commits after its logical boundary.

For an accepted snapshot, `quality_run_id` is the lowercase SHA-256 of the exact UTF-8 bytes:

```text
nyc_taxi\n<logical-date-UTC>\n<source-snapshot-id>\nnyc_taxi_quality_v1
```

Missing-source diagnostics use the same formula with the closed token `missing` in place of a
snapshot and cannot form an accepted fact set. A same-logical-date retry against the same snapshot
therefore has the same key. Facts are merged on the exact composite key
`(quality_run_id, rule_id)`; a retry replaces those rows rather than appending duplicates. A new
Bronze snapshot creates a new run ID and preserves history.

### 6.1 Frozen signal registry

Status precedence is `missing > stale > fail > warn > pass`. Missing wins because no source can be
evaluated; stale wins next because a structurally valid but wrong-window snapshot must not be
published as current; fail, warn, and pass then express evaluated rule severity.

| Rule ID | Layer / owner | Metric and exact threshold | Severity / failure semantic |
|---|---|---|---|
| `bronze.source_available.v1` | Bronze / Data Engineering | source rows; `pass` when `> 0`, `fail` at `= 0`, `missing` when table/ref/snapshot is absent | error; no Silver mutation |
| `bronze.schema.v1` | Bronze / Data Engineering | exact 20-column match; `pass=1`, `fail=0` | error; no Silver mutation |
| `bronze.snapshot_freshness.v1` | Bronze / Data Engineering | snapshot age beyond the window; `stale` when commit `< data_interval_end - 21600s` | error; no Silver mutation |
| `bronze.invalid_ratio.v1` | Bronze / Data Quality Engineering | invalid/source as decimal; pass `<=0.010000000`, warn `>0.010000000` and `<=0.050000000`, fail `>0.050000000` | error at fail; warning may proceed |
| `silver.partition_conservation.v1` | Silver / Data Quality Engineering | clean + quarantine and union multiset equal Bronze; pass exact, otherwise fail | error; facts record failure when safe |
| `silver.clean_nonempty.v1` | Silver / Data Quality Engineering | clean rows; pass `>0`, fail `=0` | error |
| `silver.quarantine_ratio.v1` | Silver / Data Quality Engineering | quarantine/source; same 1% warn and 5% fail boundaries | warning/error mirrors observed output |
| `silver.output_readback.v1` | Silver / Data Platform Engineering | both schemas, checksums, predicate membership, properties and snapshot binding exact; pass=1, fail=0 | error |

The invalid-ratio bands are reviewed policy rather than runtime tuning. The preserved catalog
snapshot inspected during design contained 8,991,502 rows and 82,414 invalid rows (about 0.9166%),
which falls below the 1% pass boundary while leaving a meaningful warning band and a 5% hard stop.
DagRun configuration cannot change thresholds.

Each row also freezes its metric encoding. Counts and elapsed seconds use exact signed longs;
ratios are `numerator / denominator` rounded half-up to nine decimal places in `decimal(38,9)`.
Count-valued metrics encode the count itself at scale 9. Threshold strings are exact ASCII policy
expressions rather than floating-point values:

| Rule ID | `metric_name` | Numerator / denominator / value | `warn_threshold` / `fail_threshold` |
|---|---|---|---|
| `bronze.source_available.v1` | `source_row_count` | source rows / null / source rows | null / `rows=0` |
| `bronze.schema.v1` | `schema_match_ratio` | matching fields / 20 / ratio | null / `ratio<1.000000000` |
| `bronze.snapshot_freshness.v1` | `snapshot_age_seconds` | interval end minus commit / 21600 / seconds | null / `seconds>21600` |
| `bronze.invalid_ratio.v1` | `invalid_row_ratio` | invalid rows / source rows / ratio | `ratio>0.010000000` / `ratio>0.050000000` |
| `silver.partition_conservation.v1` | `partition_row_ratio` | clean plus quarantine rows / source rows / ratio | null / `ratio!=1.000000000` |
| `silver.clean_nonempty.v1` | `clean_row_count` | clean rows / source rows / clean rows | null / `rows=0` |
| `silver.quarantine_ratio.v1` | `quarantine_row_ratio` | quarantine rows / source rows / ratio | `ratio>0.010000000` / `ratio>0.050000000` |
| `silver.output_readback.v1` | `readback_check_ratio` | passed checks / 8 / ratio | null / `ratio<1.000000000` |

The eight readback checks are exactly: clean schema, quarantine schema, clean predicate membership,
quarantine predicate membership, combined count, combined multiset/checksum, clean properties, and
quarantine properties. They describe the two Silver outputs; Gold completion remains the separate
post-MERGE readback below. A ratio has a non-null positive denominator; a missing denominator is
never encoded as zero. Observed severity is `info` for pass, `warning` for warn, and `error` for
fail/missing/stale. Diagnostic codes are exactly `ok`, `threshold_warn`, `threshold_fail`,
`source_missing`, `source_stale`, `schema_mismatch`, `partition_mismatch`, `output_empty`, or
`readback_mismatch`, selected by the rule/status pair. Any other code fails validation.

Gold coverage is the durable facts table itself plus a mandatory post-MERGE acceptance check. It is
not represented by a self-referential fact row. After MERGE the application requires exactly the
eight keyed rows, exact field values and statuses, no duplicate key, exact Gold schema, and the
expected source snapshot. Failure of this readback fails the task. That explicit second phase avoids
claiming a Gold-completion fact before Gold has been verified.

## 7. Failure ordering, recovery, and idempotence

Iceberg has no cross-table transaction. The exact order is:

1. create or validate the Gold facts table;
2. capture and validate Bronze snapshot/schema/count and compute Bronze signals;
3. best-effort MERGE controlled diagnostics, then fail without Silver mutation for
   missing/stale/schema/invalid-ratio fatal states;
4. replace and validate clean;
5. replace and validate quarantine;
6. validate both Silver tables together and recheck Bronze snapshot/schema;
7. MERGE the exact eight facts; and
8. read back the complete Gold fact set before accepting success or warning.

Clean is written first because it is the downstream-consumable subset; a failure before quarantine
cannot falsely publish a completed quality run because Gold facts are last. There is still a
visible mixed-Silver window and direct readers must not treat table replacement alone as accepted.
Consumers select only a complete fact-set run and require both Silver property maps to match it.

The supported recovery is a serialized rerun with the same logical date and unchanged Bronze
snapshot. Deterministic replacement plus composite-key MERGE converges without duplicate facts.
Tests inject failures before each source read, each Silver write/readback, the Bronze postcheck, the
facts MERGE, and facts readback; no later write occurs after a primary failure. A failure between
Silver writes is recovered by the same-snapshot rerun. Ordinary diagnostic persistence and cleanup
errors never replace the primary error. Catalog-unavailable or first-run missing-table failures may
make fact persistence impossible; this is reported truthfully rather than silently converted into
an accepted run.

Warnings succeed only after all Silver and Gold checks. `missing`, `stale`, or `fail` makes the
Spark application fail after safe diagnostic persistence. Operators remediate the upstream ETL or
policy violation and rerun the same logical date; they do not edit facts or thresholds manually.

## 8. Fixed dashboard/query surface

Three checked-in fixed Trino queries read the durable Gold facts:

1. `latest` returns exactly the latest complete eight-row accepted fact set ordered by
   `(layer, rule_id)`;
2. `trend` returns at most 90 complete runs ordered by logical date and snapshot descending with
   source, invalid, clean, and quarantine measures; and
3. `operator_attention` returns at most 100 `warn`, `fail`, `missing`, or `stale` rows ordered by
   logical date, status precedence, layer, and rule ID.

Latest and trend admit a run only when it has exactly one row for each of the eight frozen rule
definitions, no duplicate or foreign row, exact rule metadata, and eight non-null equal bindings
to the expected dataset, binding type, upstream DAG, source table, positive source snapshot,
snapshot commit, logical date, data interval, and frozen schema fingerprint. Operator attention
instead joins the same governed rule registry directly so bounded missing, stale, warning, and
failure diagnostics remain visible even when only a safe partial fact set could be persisted. Each
query is one literal `SELECT`/`WITH` statement. Tests reject semicolons, comments,
multi-statements, DDL/DML/CALL/SET, interpolation, nondeterministic ordering, `SELECT *`, unbounded
results, wrong table names, or arbitrary DagRun SQL. Exact output columns/types and decimal/UTC
serialization are frozen. Operators retrieve the surface with the internal Trino CLI or UI using
the documented SQL files; the durable source of truth is the Iceberg fact table, not console output
or Airflow XCom. Grafana is intentionally not changed because the enabled Atlas integration is a
Prometheus infrastructure datasource, not a governed Trino quality datasource.

## 9. Educational notebook reconciliation

Both Jupyter and Zeppelin notebooks retain the same source and two Silver table names, but switch
quarantine to the explicit null-safe complement and prove total partition conservation. Their
projected columns/order match production. Both notebooks and every generated surface warn that
interactive `createOrReplace` calls bypass Airflow serialization, snapshot/run properties, durable
facts, thresholds, and readback acceptance; running them against production tables can invalidate
the dashboard contract. Production writes must use the DAG/application.

The scenario matrix moves from `approved new production DAG` to `existing production DAG` only
after the real live gate succeeds. README, site, wiki, notebook index, app index, runbook, diagram,
changelog, and matrix are generated/reconciled from the canonical sources.

## 10. Test and live acceptance

Strict RED-first tests cover:

- exact argument/timestamp/table bounds and deterministic run IDs;
- the exact Bronze/Silver/Gold schemas and schema hash;
- null fare/passenger values, predicate boundaries, duplicates, multiset conservation and stable
  distributed checksums;
- every signal, threshold edge, status and precedence for pass/warn/fail/missing/stale;
- same-key MERGE idempotence, new-snapshot history, duplicate rejection, and exact fact readback;
- source/snapshot/schema/checksum failures and injected failures at every write/readback boundary;
- first-write and between-write recovery without a false complete fact set;
- exact same-logical-date `ExternalTaskSensor`, timeout/reschedule/failure states, DAG serialization,
  Atlas operator ownership, terminal REST confirmation, no import-time network, and fixed runtime
  configuration;
- fixed query allowlist, row/type/byte bounds, deterministic order, canonical decimals/timestamps,
  and operator retrieval; and
- Jenkins, Maven, docs, diagram, matrix, Compose mount, and repository invariants.

The genuine `RUN_INFRA=1` harness requires zero pre-existing project containers and an already
verified tiny NYC Taxi publication. It never downloads, refreshes, rolls back, or mutates a dataset
pointer. It starts only an exclusively owned stack, records the exact optional NYC pointer
body/ETag or explicit absence, and restores zero containers with volumes preserved.

The canonical live sequence publishes the exact reviewed ETL and quality JARs, keeps both daily
DAGs paused, and creates controlled ETL and quality DagRuns at the same whole-second logical date.
It proves the sensor observes the real upstream success. It then reruns quality against the same
snapshot to prove identical run ID/no duplicate facts, and performs a second matching ETL+quality
pair at a second logical date to prove new-snapshot history. Complete paginated Airflow inventories
must contain only test-owned run IDs and no unexpected active run.

Acceptance requires every ETL and quality task to succeed, every distinct Spark driver to be
REST-terminal `FINISHED` with `success=true`, and no unowned driver delta. The harness independently
asserts the exact Bronze schema/snapshot/commit window/count/checksum; Silver schemas, null-safe
partition, duplicates, counts, predicate membership, checksums, properties and snapshot binding;
the exact facts schema, keys, eight rows per accepted run, statuses, thresholds, idempotent retry,
history and post-MERGE readback; and all three actual Trino query schemas/order/bounds/checksums.
The raw pointer is byte/ETag identical before and after. Cleanup preserves volumes and an all-state
Docker query returns zero project containers.
