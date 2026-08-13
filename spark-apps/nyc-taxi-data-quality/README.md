# NYC Taxi Data Quality Production Application

This Spark application evaluates one stable Iceberg snapshot of
`lakehouse.bronze.nyc_taxi_trips`, replaces the null-safe Silver partitions
`lakehouse.silver.nyc_taxi_clean` and `lakehouse.silver.nyc_taxi_quarantine`, and idempotently
MERGEs eight governed facts into `lakehouse.gold.nyc_taxi_quality_facts`. Jenkins publishes
`s3a://jars/nyc-taxi-data-quality/0.1.0/app.jar`.

## Trust boundary

The input binding is the exact Bronze Iceberg snapshot ID, commit timestamp, schema hash, row
count, and content checks. It is snapshot-bound evidence, not five-key generation provenance.
Adding the upstream dataset/scale/plan/publication/manifest properties to `nyc_taxi_etl` is deferred
producer hardening; this application does not invent those properties or claim that the Bronze
table can be traced to one resolver generation.

Airflow's `wait_for_matching_nyc_taxi_etl` `ExternalTaskSensor` waits for the successful
`nyc_taxi_etl.submit_nyc_taxi_etl` task at
the same logical date. The quality task then captures and rechecks the Bronze snapshot. Missing,
inaccessible, empty, stale, schema-changing, or concurrently changing Bronze state fails closed.

## Exact data contracts

Bronze and both Silver tables preserve these 20 nullable fields in order:

```text
VendorID long
tpep_pickup_datetime timestamp_ntz
tpep_dropoff_datetime timestamp_ntz
passenger_count double
trip_distance double
RatecodeID double
store_and_fwd_flag string
PULocationID long
DOLocationID long
payment_type long
fare_amount double
extra double
mta_tax double
tip_amount double
tolls_amount double
improvement_surcharge double
total_amount double
congestion_surcharge double
airport_fee double
trip_date date
```

The two source-derived trip timestamps are local civil timestamps and therefore use Spark
`TimestampNTZType`. Gold logical-date, interval-end, and source-commit fields remain UTC
`TimestampType` instants.

The schema fingerprint is SHA-256 over compact UTF-8 JSON in field order. Every field object has
the sorted keys `name`, `nullable`, and `type`, with Spark canonical type names and no whitespace or
terminal newline. The frozen 20-field digest is
`5a8d2916cc5967c0eeb8318136c1262156cd616105dad67a713f1cb1cc872fc5`.

Rows are clean only when finite `fare_amount > 0` and finite `passenger_count BETWEEN 1 AND 6`
both evaluate true. `trip_distance` is not a governed rule operand. Null, NaN, and infinite rule
operands are quarantined. Duplicates are preserved, and
the null-safe clean/quarantine multisets must exactly conserve the Bronze multiset.

The Gold schema is:

```text
quality_run_id string, logical_date timestamp, data_interval_end timestamp,
dataset_id string, binding_type string, upstream_dag_id string, source_table string,
source_snapshot_id long, source_snapshot_committed_at timestamp, source_schema_sha256 string,
layer string, rule_id string, rule_version string, owner string, metric_name string,
metric_numerator long, metric_denominator long, metric_value decimal(38,9),
warn_threshold string, fail_threshold string, severity string, status string,
diagnostic_code string
```

The deterministic `quality_run_id` binds the canonical whole-second logical date, Bronze snapshot,
dataset, and rule version `nyc_taxi_quality_v1`. The exact rule set includes
`bronze.source_available.v1`, `bronze.schema.v1`, `bronze.snapshot_freshness.v1`,
`bronze.invalid_ratio.v1`, `silver.partition_conservation.v1`, `silver.clean_nonempty.v1`,
`silver.quarantine_ratio.v1`, and `silver.output_readback.v1`. Invalid/quarantine ratios pass at or
below 1%, warn above 1% through 5%, and fail above 5%. Missing and stale outrank fail, which outranks
warn and pass.

Logical date and interval end are canonical whole-second UTC instants. The source snapshot commit
retains exact Iceberg millisecond precision; freshness uses Java `Duration.getSeconds` semantics,
including floor behavior when the matching ETL commits fractionally after its logical boundary.

## Failure and recovery

Silver replacements and the Gold MERGE are non-atomic across tables. The application validates
the complete split before writing, replaces clean then quarantine, rechecks Bronze, MERGEs facts on
`(quality_run_id, rule_id)`, and reads back the exact fact set. A failed attempt never reports
success. Rerun the same logical date after correcting the primary cause: the deterministic writes
converge and the Gold key prevents duplicate facts. If the first Silver table changed before a
later failure, this same-date rerun is the supported recovery.

The production DAG uses `max_active_runs=1`. Concurrent direct JAR execution is unsupported because
it bypasses this serialization boundary.

## Dashboard and operations

The durable dashboard source is the Gold Iceberg table. A run is complete only when it has exactly
one row for every frozen `(rule_id, rule_version)` pair with consistent dataset, binding, source
table, snapshot, and schema fingerprint. These fixed SELECT-only Trino queries are bounded and
deterministically ordered:

- `queries/latest.sql`: latest complete accepted eight-row fact set;
- `queries/trend.sql`: at most 90 complete accepted runs; and
- `queries/operator_attention.sql`: at most 100 warning/failure rows.

Run them from a Trino-capable operator environment, for example:

```bash
trino --file spark-apps/nyc-taxi-data-quality/queries/latest.sql
trino --file spark-apps/nyc-taxi-data-quality/queries/trend.sql
trino --file spark-apps/nyc-taxi-data-quality/queries/operator_attention.sql
```

Do not pass SQL or policy thresholds through DagRun configuration. The checked-in rule version and
query files are the reviewed interface.

## Build and publication

```bash
mvn -q -B -f spark-apps/nyc-taxi-data-quality/pom.xml test
mvn -q -B -f spark-apps/nyc-taxi-data-quality/pom.xml package
```

Jenkins tests, packages, and copies the exact artifact using injected MinIO credentials. Airflow
uses `spark_default`, cluster mode, and Atlas `RestConfirmingSparkHook`; accepted task success also
requires the standalone Spark driver to reach `FINISHED` with `success=true`.

The educational notebooks write the same Silver tables directly without Airflow serialization,
snapshot properties, governed facts, or post-write acceptance. Run them only in an isolated
learning environment; production writes must use `nyc_taxi_data_quality`.
