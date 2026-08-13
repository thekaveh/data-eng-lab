# NYC Taxi Data Quality Live Acceptance

Status: pending canonical `RUN_INFRA=1` replay.

The tracked executable source of truth is
`tests/scenarios/test_nyc_taxi_data_quality_live.py`. It requires an existing verified tiny NYC
Taxi publication and fails closed without refreshing or mutating the dataset pointer. It requires
exclusive ownership of a stopped project stack, keeps both daily DAGs paused during controlled
manual acceptance, restores their initial pause states, and stops only its owned stack without
removing volumes.

Prerequisite and acceptance commands:

```bash
uv run python scripts/download_datasets.py --scale tiny --only nyc_taxi --verify-only
uv run python scripts/resolve_dataset.py nyc_taxi --scale tiny
RUN_INFRA=1 uv run pytest tests/scenarios/test_nyc_taxi_data_quality_live.py -vv -s
```

If the verified publication is intentionally absent, an operator may provision it separately with
the supported bounded command below and must then run verify-only before acceptance. The harness
never performs this operation itself.

```bash
uv run python scripts/download_datasets.py --scale tiny --only nyc_taxi --refresh
uv run python scripts/download_datasets.py --scale tiny --only nyc_taxi --verify-only
```

## Prerequisite recovery evidence

The pointer was confirmed absent (`NoSuchKey`) before the authorized bounded refresh. The original
refresh failed closed on the Arrow Parquet metadata verifier; the separately reviewed recovery is
recorded in `2026-08-13-parquet-schema-normalization-blocker.md`. After both independent reviews
returned Critical 0 / Important 0 / Minor 0 and Ready Yes, the identical supported command
published and verify-only accepted:

- plan `66929ee59188f5a2deb8e29e8593fbe9bad1ad6dc1c4daadd7aeb45b51916189`;
- publication `16e280e900a84d1b9d617743472b8ada`;
- manifest SHA-256 `3b678261e704aeb6dee3ae981d699bf81db5696fa9da64abfbce6fe2bd7f6c12`;
- one 47,673,370-byte object with SHA-256
  `32df6f67578fa86c484a6b5ef23a5281992ff085521082340b0f9e5889e9a572`; and
- canonical `s3://landing/nyc_taxi/_generations/<plan>/<publication>/yellow_tripdata_2023-01.parquet`.

Registry and lock hashes were unchanged. No legacy key or volume was deleted, and the provisioning
stack stopped with zero project containers.

## First acceptance replay and corrected scale binding

The first replay failed before any Spark write or quality DagRun. The matching ETL task made two
resolver requests for `small` and received the resolver's redacted HTTP 500
`{"error":"dataset resolution failed"}`. Direct health and tiny resolution succeeded from the
resolver, Airflow scheduler, and Jupyter containers, while a direct small request reproduced the
same 500. Host and image hashes for publication, registry, resolver, S3, schema, verification, and
registry YAML matched exactly; the resolver image was the freshly rebuilt reviewed image.

Root cause: the acceptance harness omitted Airflow's `--conf` argument, so the production ETL
correctly fell back to the scheduler's `DATASET_SCALE=small` instead of the verified tiny
prerequisite. The harness now passes exact bounded canonical JSON
`--conf '{"dataset_scale":"tiny"}'` to every ETL and quality test invocation, proves the real ETL
`_effective_scale` path selects it over the environment, and rejects any created DagRun whose
stored conf is not exactly that mapping. Production DAG code is unchanged. RED was two command
contract failures; GREEN is eleven offline harness tests with one expected live skip.

Exact artifact, run, driver, snapshot, fact, query, pointer, and teardown evidence will be appended
only after the canonical replay succeeds.

## Second replay and Bronze timestamp contract diagnosis

After the tiny-conf correction, the matching ETL completed and committed Bronze snapshot
`8441725828099085709`. The first quality attempt and its retry both failed before a Silver write
at `QualityTransforms.assertExactSchema`: Spark 4.1 `DESCRIBE` reported
`tpep_pickup_datetime` and `tpep_dropoff_datetime` as `timestamp_ntz`, while the quality contract
incorrectly expected UTC `timestamp`. The source Parquet logical annotations are explicitly not
UTC-adjusted, and the ETL intentionally preserves those local civil timestamps. The exact worker
exception was `IllegalArgumentException: NYC Taxi source schema is invalid`.

The failed attempt created only the empty Gold facts table metadata (zero rows and zero snapshots).
Neither Silver table existed and no fact row was persisted; the Bronze snapshot stayed stable.
That empty metadata residue is safe because the application validates its exact schema and the
supported same-logical-date rerun converges through the ordinary MERGE path. The corrected contract
uses `TimestampNTZType` only for the two source-derived trip fields. Facts logical date, interval
end, and source snapshot commit fields remain UTC `TimestampType`. Paired producer-transform and
quality-consumer tests freeze the exact 20-column schema, reject the legacy timestamp type, and the
live harness checks the actual post-ETL Iceberg schema before starting quality. Diagnostic cleanup
left zero project containers and preserved all volumes.

The next canonical attempt correctly refused to proceed after its first matching ETL because the
persisted Airflow baseline still contained the earlier test-owned quality DagRun
`manual__2026-08-13T07:32:25.647385+00:00` in `running`. Read-only inspection proved exact tiny
conf, `triggered_by=test`, `triggering_user_name=dag_test`, a successful sensor, a stopped
`up_for_retry` Spark task ending at `2026-08-13T07:32:45.785977Z`, complete Spark application-end
event logs, and no active quality driver. The exact run alone was terminalized to `failed` through
Airflow API v2 at `2026-08-13T07:50:55.892792Z`; no production or foreign run was changed. The
harness now performs this bounded recovery only for the one run created by its own failed
`dags test`, only after every task is stopped, and verifies the exact PATCH response and readback.
Foreign or actively executing runs remain fail-closed. This attempt made no quality write and its
owned stack again stopped volume-preserving with zero containers.

## Third replay and Iceberg property-readback diagnosis

The next replay proved the timestamp correction in the real catalog and passed the first quality
schema/source evaluation. Both quality attempts then failed immediately after replacing the clean
table. The exact event-log exception was `TABLE_OR_VIEW_NOT_FOUND` for
`lakehouse.silver.nyc_taxi_clean.properties`: Spark Iceberg exposes table configuration through
`SHOW TBLPROPERTIES`, not a `.properties` metadata table. The repository's three established
production writers already use that command; the quality store's recording fake had returned a
generic key/value frame without asserting the SQL and therefore hid the dialect error.

The preserved partial state is clean-only and recoverable: Bronze snapshot
`3083283024212730022` committed at `2026-08-13T07:55:45.507Z`; clean contains 2,917,820 rows and
the exact five quality properties for run `0fad4d95b2bcd9927790bccb9f1926c3525163d96d3e244a2e9fa62ff5a58b75`;
quarantine is absent; Gold facts remains zero rows. The exact application JAR SHA-256 was
`45a1fb63616131507b86f445dff74ed27c870f754b4f4d5ce89a40c8d1267448`. The failure occurred before
the quarantine write and fact MERGE. The strict fix uses the fixed Silver allowlist and exact
`SHOW TBLPROPERTIES <identifier>` statement. A real local Spark syntax test proves its key/value
shape and proves the old `.properties` relation fails; the partial-state regression proves a
clean-only retry converges both Silver tables and one idempotent eight-fact set.

## Fourth replay and Gold fact binding diagnosis

The property-readback correction allowed both Silver tables to converge in the real stack. Both
attempts then failed before the Gold MERGE because Spark's case-class encoder named the fact frame
columns `qualityRunId`, `logicalDate`, and the other Scala camelCase member names, while production
selected the exact snake-case Gold contract beginning with `quality_run_id`. The event-log failure
was `UNRESOLVED_COLUMN.WITH_SUGGESTION`, explicitly suggesting `qualityRunId` for
`quality_run_id`. Bronze snapshot `3969634704401179188` stayed stable; clean contained 2,917,820
rows, quarantine contained 26,039 rows, both had matching intended quality properties, and facts
remained zero rows. This is a safely converged Silver/facts-empty recovery point.

The correction constructs explicit Spark Rows under the exact 23-field
`QualityContract.factsSchema` before creating the MERGE source view. Its executable regression
materializes all eight real `QualityFact` values and proves exact names/order/types/nullability,
scale-nine decimals, UTC timestamps, row count, and snake-case temp-view binding. The Gold MERGE
SQL and idempotent `(quality_run_id, rule_id)` key remain unchanged.
