# NYC Taxi Data Quality Live Acceptance

Status: accepted by canonical `RUN_INFRA=1` replay on 2026-08-13.

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

Exact artifact, run, driver, snapshot, fact, query, pointer, and teardown evidence appears in the
final acceptance section below.

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

## Fifth replay and Airflow same-date retry semantics

The fact-schema correction completed two real quality applications and produced one exact
eight-rule fact set. The acceptance harness then failed closed because it expected both same-date
test DagRuns to remain in the Airflow API inventory. Pinned Airflow 3.3.0 implements
`DAG.test()` through `get_or_create_dagrun`; that helper selects an existing run by
`(dag_id, logical_date)`, deletes and commits it, then creates the replacement test run. The first
run is therefore intentionally absent from the final API inventory even though its bounded task
logs and terminal Spark event log remain durable acceptance evidence.

Read-only recovery evidence bound both executions to the successful ETL task at logical date
`2026-08-13T08:25:20+00:00`. Drivers `driver-20260813082614-0001` and
`driver-20260813082806-0002` both reached `FINISHED` with successful applications
`app-20260813082617-0002` and `app-20260813082808-0003`. The two fact snapshots contained exactly
eight rows, eight distinct `(quality_run_id, rule_id)` keys, and one deterministic quality run ID;
clean 2,917,820 plus quarantine 26,039 conserved all 2,943,859 Bronze rows. The harness correction
now captures and validates the first run's exact conf, sensor log, Spark log, driver, and terminal
state before invoking the retry. It then requires the API transition to remove exactly that owned
run and add exactly one successful same-date replacement, with no unrelated or active run delta,
and preserves both bounded sensor/driver proofs. Unique-date executions retain the additive
inventory contract.

## Sixth replay and Trino timestamp dialect diagnosis

The corrected same-date inventory contract passed in the real stack, as did both ETL runs, all
three quality executions, Silver conservation/provenance, two complete governed fact sets, and the
clean-membership check. The first fixed dashboard query then failed read-only. Exact Trino stderr
reported that `with_timezone(timestamp(6) with time zone, varchar)` is invalid because the function
accepts an unzoned timestamp. `DESCRIBE lakehouse.gold.nyc_taxi_quality_facts` confirmed that
`logical_date`, `data_interval_end`, and `source_snapshot_committed_at` are already
`timestamp(6) with time zone`, matching the UTC-instant Iceberg contract.

Direct `format_datetime(logical_date, 'yyyy-MM-dd''T''HH:mm:ss''Z''')` returned the intended
whole-second UTC string, while removing only the redundant wrapper made all three reviewed queries
execute: latest returned eight rows, trend returned the complete accepted run history, and operator
attention returned its bounded empty result. The minimal correction applies that direct formatting
to all three immutable query files. The live Trino helper now also preserves only a bounded,
endpoint- and secret-redacted diagnostic tail without exposing the fixed SQL body. The accepted
partial state remained three deterministic run IDs with exactly eight rows and eight distinct rule
keys each; no data mutation was needed for this read-only query correction.

## Review-hardening replay diagnostics

Four acceptance-only mismatches were found and corrected under focused RED/GREEN tests before the
final replay. None changed the production application or governed query results:

1. the new snapshot-binding probe initially emitted a malformed Trino format literal and failed
   after the first ETL, before a quality run;
2. its next revision truncated the Iceberg commit to seconds and used Python truncation toward zero,
   while production correctly retained milliseconds and used Java duration floor semantics;
3. the typed protocol helper initially admitted only internal `trino:8080` next URIs, while the
   loopback request correctly yielded `127.0.0.1:20029`; and
4. Trino 482 renders decimals as `decimal(38, 9)` and infers the trend status expression as
   `varchar(4)`, more precisely than the original harness expectation.

Every failed replay restored pause state, used standard volume-preserving teardown, and ended with
zero project containers. Preserved Silver and Gold state converged through the normal serialized
rerun contract; the active dataset pointer was never refreshed or mutated.

## Final canonical acceptance

The final review-hardened harness at commit `770aa29` completed with `1 passed in 549.78s`. Both
production DAGs stayed paused
throughout controlled execution, then returned to their initial states: ETL unpaused and quality
paused. The owned stack stopped volume-preserving and an all-state Docker probe returned zero
project containers.

Immutable inputs and artifacts:

- resolver plan `66929ee59188f5a2deb8e29e8593fbe9bad1ad6dc1c4daadd7aeb45b51916189`;
- publication `16e280e900a84d1b9d617743472b8ada`;
- manifest SHA-256 `3b678261e704aeb6dee3ae981d699bf81db5696fa9da64abfbce6fe2bd7f6c12`;
- active pointer ETag `"204190c685574dfa91c330acb64dfb82"`, unchanged before/after;
- ETL JAR SHA-256 `cc50a4352de371151ddec2dcc66ab73a7e9af3c4b69d8b45fe7471bedef84b74`;
  and
- quality JAR SHA-256 `7fdf03eeeab3e1cd62385ba17ed4e7e5e3f065cfe142b1e5349e1580c122cf1f`.

Airflow and Spark execution evidence:

- ETL runs `manual__2026-08-13T10:47:31.955992+00:00` and
  `manual__2026-08-13T10:51:36.657505+00:00`;
- first quality run `manual__2026-08-13T10:48:19.698016+00:00`, replaced under pinned same-date
  test semantics by `manual__2026-08-13T10:50:10.059952+00:00`;
- final quality run `manual__2026-08-13T10:52:00.638217+00:00`; and
- five distinct REST-confirmed terminal drivers: `driver-20260813104737-0000`,
  `driver-20260813104823-0001`, `driver-20260813105013-0002`,
  `driver-20260813105140-0003`, and `driver-20260813105204-0004`.

The first accepted source snapshot was `815563093405415986`, committed at
`2026-08-13T10:47:47.218Z`; the same-date retry advanced clean snapshot
`1470972520855769750` to `6911075779583457757` and quarantine snapshot
`7480648615033564735` to `3653883011626346753` while preserving exact table multisets and the fact
set. Its exact deterministic fact ID was
`d0277840940b98d38c1724e3e0e4715847b2b55cd85678d3466b5f29f8e7da35`.
The second matching ETL produced Bronze snapshot `1189034078581805403`, committed at
`2026-08-13T10:51:51.024Z`. Its final Silver
properties matched exactly and bound both outputs to deterministic run
`bf2b3e0b4a185645a68077191281728f052972b6347f636365e0954739e2a959` and that Bronze snapshot.

The harness selected those two exact owned fact IDs rather than a lexical history tail. It compared
all 23 fields of all 16 rows, including exact UTC timestamps, millisecond source commits, Java
`Duration.getSeconds` freshness values `-22` and `-265`, nullable thresholds/denominators, exact
scale-nine decimals, schema fingerprint
`5a8d2916cc5967c0eeb8318136c1262156cd616105dad67a713f1cb1cc872fc5`, lineage, owners, rule
metadata, severities, statuses, and diagnostic codes. Both owned runs had exactly eight governed
keys and zero rejected statuses.

Final source/output evidence:

- Bronze 2,943,859 rows, checksum `b66a4c29486af278`;
- clean 2,917,820 rows, checksum `330b0a56eb827b24`;
- quarantine 26,039 rows, checksum `d4c2179946371cbd`; and
- facts 104 historical rows, checksum `29746ac58941798a`; the first owned acceptance point had 96
  rows/checksum `d82ef6f4e67f8748`, and same-date retry left that exact fact multiset unchanged.

The fixed dashboards executed read-only with exact bounded results: latest returned eight rows and
checksum `5f32f593f4fd6119effe8ab049aa062f24658ab7a3546931032454a7a3a7c16c`;
trend returned 13 rows and checksum
`bd125cc8f28ac501c58e9a064aac6b1cfc9e791d2555805bdb32faf29a7669dd`; operator attention
returned zero rows and canonical empty checksum
`4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945`.
Protocol metadata was present even for the empty result and matched the frozen names and types.
Trino 482 rendered decimal fields as `decimal(38, 9)` and inferred trend `overall_status` as
`varchar(4)`; every other textual output field was `varchar` and all count/snapshot fields were
`bigint`. Pagination accepted only the exact configured internal or loopback Trino origin and the
canonical statement path.

## Final diagnostic-contract replay

The post-review artifact at commit `3609452` completed the unchanged canonical harness with
`32 passed in 552.17s`. Both production DAGs stayed paused during controlled execution, then
returned to their initial states: ETL unpaused and quality paused. Standard teardown preserved all
volumes, and the final all-state Docker probe found zero project containers.

Immutable inputs and artifacts remained exact:

- resolver plan `66929ee59188f5a2deb8e29e8593fbe9bad1ad6dc1c4daadd7aeb45b51916189`;
- publication `16e280e900a84d1b9d617743472b8ada`;
- manifest SHA-256 `3b678261e704aeb6dee3ae981d699bf81db5696fa9da64abfbce6fe2bd7f6c12`;
- active pointer ETag `"204190c685574dfa91c330acb64dfb82"`, unchanged before/after;
- ETL JAR SHA-256 `cc50a4352de371151ddec2dcc66ab73a7e9af3c4b69d8b45fe7471bedef84b74`;
  and
- quality JAR SHA-256 `569320db7510d891c86f3a98b940effcd1da1f68c6f9eefff5c1318c4548f99b`.

Airflow and Spark evidence was exactly:

- ETL runs `manual__2026-08-13T11:27:34.166677+00:00` and
  `manual__2026-08-13T11:31:39.135521+00:00`;
- first quality run `manual__2026-08-13T11:28:22.049612+00:00`, replaced under pinned same-date
  semantics by `manual__2026-08-13T11:30:12.935869+00:00`;
- final quality run `manual__2026-08-13T11:32:03.345200+00:00`; and
- five distinct REST-confirmed terminal drivers: `driver-20260813112739-0000`,
  `driver-20260813112825-0001`, `driver-20260813113016-0002`,
  `driver-20260813113142-0003`, and `driver-20260813113207-0004`.

The first accepted source snapshot was `5989237067513271004`, committed at
`2026-08-13T11:27:51.181Z`, and produced deterministic quality run
`593843271477d30c9416aa8e6696a4e2975d845effd0812cfb10c71abcf2e682`. Its same-date retry
advanced clean snapshot `5051434589101096761` to `8030645671831296095` and quarantine snapshot
`5367696590882175467` to `5481628481191702516`, while preserving exact clean/quarantine
multisets and the 112-row fact table checksum `36f42e46e4c84123`. The second ETL committed source
snapshot `1555947243003706568` at `2026-08-13T11:31:52.794Z` and produced deterministic quality
run `47289684ef73bc7f82a7c121f6ad1c4ace6ec7b46a77afbb616b92c1c6231c8e`. Final Gold state was
120 rows with checksum `30d9523e9bc632fd`.

The harness selected those two exact owned run IDs, compared all 23 fields of all 16 facts, and
required exact lineage, rule metadata, nullable fields, UTC timestamps, scale-nine decimals,
statuses, severities, and closed diagnostic codes. Both runs had exactly eight governed keys and
zero rejected statuses. In particular, `silver.output_readback.v1` was exactly numerator 8,
denominator 8, value `1.000000000`, status `pass`, and diagnostic `ok` in each owned run.

Source and output contents remained deterministic:

- Bronze 2,943,859 rows, checksum `b66a4c29486af278`;
- clean 2,917,820 rows, checksum `330b0a56eb827b24`; and
- quarantine 26,039 rows, checksum `d4c2179946371cbd`.

The hardened fixed dashboards executed read-only with exact protocol metadata. Latest returned
eight rows/checksum `97fd81540b79ef8423d760acdc8520db0da3fd489d46cbdffc254152d4017294`;
trend returned ten complete accepted runs/checksum
`def147f29f83b53686e483535e8a5f7770dbfe27d95a2f367870c5468ade2a8f`; and operator attention
returned zero current warning/failure rows with the canonical empty checksum
`4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945`. Offline executable
adversarial tests separately prove that operator attention surfaces a governed partial
missing-source diagnostic while latest/trend reject null, wrong-fingerprint, wrong-lineage,
wrong-rule-metadata, missing, duplicate, and foreign fact sets.

## Final authenticated-dashboard replay

The final reviewed artifact at commits `76f98ea` and `a554b7e` passed the unchanged canonical
harness with `32 passed in 549.25s`. The first replay correctly failed closed after all five Spark
applications at the new dashboard authenticity gate: it exposed that Airflow manual test runs use
an equal logical date and data-interval end, and that Java `Duration.getSeconds` floors negative
fractional ages while Trino `date_diff('second', ...)` truncates them. The final SQL keeps all eight
interval values non-null and equal, recomputes freshness as the floor of the exact millisecond
difference, and does not invent an interval duration. The passing replay exercised that native
Trino expression. Both DAGs stayed paused during controlled execution and were restored to their
initial states. Volume-preserving teardown and the later read-only evidence extraction each ended
with zero all-state project containers.

Immutable identities remained unchanged: resolver plan
`66929ee59188f5a2deb8e29e8593fbe9bad1ad6dc1c4daadd7aeb45b51916189`, publication
`16e280e900a84d1b9d617743472b8ada`, manifest
`3b678261e704aeb6dee3ae981d699bf81db5696fa9da64abfbce6fe2bd7f6c12`, and pointer ETag
`"204190c685574dfa91c330acb64dfb82"` before and after. The ETL JAR SHA-256 was
`cc50a4352de371151ddec2dcc66ab73a7e9af3c4b69d8b45fe7471bedef84b74`; the final quality JAR
SHA-256 was `593f59d8deea026448c0e6514b33338c10c2d303857a7187298b92bb2597869d`.

The exact Airflow/Spark evidence was:

- ETL runs `manual__2026-08-13T12:14:00.523419+00:00` and
  `manual__2026-08-13T12:18:04.763337+00:00`;
- initial quality run `manual__2026-08-13T12:14:48.652746+00:00`, replaced at the same logical
  date by `manual__2026-08-13T12:16:38.694341+00:00`;
- final quality run `manual__2026-08-13T12:18:29.679926+00:00`; and
- five distinct REST-confirmed terminal drivers `driver-20260813121405-0000`,
  `driver-20260813121452-0001`, `driver-20260813121642-0002`,
  `driver-20260813121808-0003`, and `driver-20260813121833-0004`.

The first Bronze snapshot was `4468175290679957811`, committed at
`2026-08-13T12:14:16.963Z`, and bound deterministic fact run
`2579c618ee8db7073bb515d032185add960cbe6fa5d7a0c5dde4917939c9f8ad`. Same-date recovery
advanced clean snapshot `8326777679340570550` to `1936699521753809644` and quarantine snapshot
`1757991413199017851` to `440610164289841125`, while preserving exact clean/quarantine
multisets and the 144-row fact checksum `73adae5c6f0f3a88`. The second Bronze snapshot was
`1746075589603593049`, committed at `2026-08-13T12:18:18.617Z`, and bound deterministic fact run
`86f05d9b134e136169e1a7774dde7a2308759019104068164b141cd2b66b2958`. Final clean and
quarantine snapshots were `4617293870179976012` and `1499385381134625583`; final Gold state was
152 rows/checksum `485b337889d56874`.

Contents remained exact: Bronze 2,943,859 rows/checksum `b66a4c29486af278`, clean 2,917,820
rows/checksum `330b0a56eb827b24`, and quarantine 26,039 rows/checksum `d4c2179946371cbd`.
The harness compared all 23 fields of the exact two owned eight-row fact sets, including the
canonical run IDs, snapshot bindings, source commits, floor-exact freshness values, decimals,
thresholds, owners, severities, statuses, and the closed per-rule diagnostic mapping.

The final fixed dashboards executed natively and read-only. Latest returned eight rows/checksum
`6a71aa2ef475f63bdbf4e3444a82481a6d63842a7797bd362ef7d9f54b6171cc`; trend returned six
authentic historical runs/checksum
`04ec0dd4431b727b8ae23d1aac2720c3b6133d55f4aa9a8f9e72e0d8e1cc46dc`; and operator attention
returned zero rows with canonical empty checksum
`4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945`. The lower trend count is
intentional: structurally complete legacy rows whose signals or run identity do not satisfy the
approved contract remain preserved in Gold but are excluded rather than rewritten.
