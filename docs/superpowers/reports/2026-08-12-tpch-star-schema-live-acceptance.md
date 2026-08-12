# TPC-H Star-Schema Live Acceptance

- **Date:** 2026-08-12 UTC
- **Branch:** `codex/107-tpch-star-schema`
- **Reviewed application commit:** `1afeeb5` plus prior application commits
**JAR SHA-256:** `b15b5ca04415336aafba6d25c77b6bbff877eeea32679176f6a772073e33135d`

## Dataset recovery and verification

The preserved MinIO volume contained pre-#81 flat `tpch/*.parquet` objects. Default acquisition failed closed with `LockMismatch: tpch/tiny legacy size_bytes mismatch`; the legacy inventory included `customer.parquet` 125850 bytes, `lineitem.parquet` 1822439 bytes, `nation.parquet` 2330 bytes, `orders.parquet` 537458 bytes, `part.parquet` 69379 bytes, `partsupp.parquet` 428287 bytes, `region.parquet` 1077 bytes, and `supplier.parquet` 10470 bytes. No legacy object or volume was deleted.

The supported `uv run python scripts/download_datasets.py --scale tiny --only tpch --refresh` operation generated and published a new immutable locked generation, atomically created its active pointer, and did not modify the registry or root lock:

- plan: `9feb94629d99d3a813e920dc63d5df2fe87a00eac990a09f70256ea76193d8e5`
- publication: `1cd69dac4c4444e6b346c542318e7cd4`
- manifest: `cd11af757ca2ad9c9baa26d6058cde536eecc0a66ac01b5e73d5d3993df4539f`
- objects: eight, exact registry order

Resolver output returned all eight canonical immutable URIs. Both before execution and after the two runs, `--verify-only` returned `status=verified-existing` for the same publication and manifest.

## Application publication

Maven test and package succeeded. The reviewed 40416-byte JAR was copied through the repository's MinIO/Jenkins publication convention to `jars/tpch-star-schema/0.1.0/app.jar`; MinIO reported ETag `502571963b206252690f4e53c4ba2ae1`.

## Airflow and Spark evidence

Both manual runs used `{"dataset_scale":"tiny"}`:

| Airflow run | Airflow task/run | Spark driver | REST terminal state |
|---|---|---|---|
| `manual__2026-08-12T19:06:03.898675+00:00` | success | `driver-20260812190708-0000` | `FINISHED`, `success=true` |
| `manual__2026-08-12T19:08:17.304959+00:00` | success | `driver-20260812190835-0001` | `FINISHED`, `success=true` |

An automatic run was created when the initial daily DAG was temporarily unpaused during acceptance. Its first attempt preceded the verified publication and its retry overlapped a controlled manual execution, so it ended failed. Review correctly identified that the original DAG did not serialize the non-atomic two-table replacement. The production DAG now sets `max_active_runs=1`; concurrent direct JAR invocations remain unsupported. The follow-up executable live gate pauses before setup and triggers one controlled rerun only after the preceding run and table snapshot complete.

## Table and idempotence evidence

Both post-run snapshots were identical:

| Table | Rows | Deterministic checksum | Schema |
|---|---:|---|---|
| `lakehouse.gold.dim_customer` | 1500 | `8b024198f91d197b` | `c_custkey long`, `c_name string`, `c_nationkey int`, `c_mktsegment string` |
| `lakehouse.gold.fct_orders` | 15000 | `8ce8521bbc607f2e` | `o_orderkey long`, `o_custkey long`, `o_orderdate date`, `revenue decimal(25,2)`, `line_count long` |

Both tables had identical `data_eng_lab.dataset*` properties matching the selected scale, plan, publication, and manifest. The downstream Trino query returned all five market segments and nonzero revenue/line measures; for example, `BUILDING` returned revenue `537013021.20` and `14908` lines.

The repository stop script completed without `--cold`; all volumes and the verified active TPC-H pointer were preserved.

## Review-fix replay contract

The tracked opt-in gate is now the source of truth instead of assertions over this report. From a
clean checkout with Docker running, execute exactly:

```bash
RUN_INFRA=1 uv run pytest tests/scenarios/test_tpch_star_schema_live.py -vv -s
```

The gate performs these replayable operations internally; credentials are loaded from `infra/.env`
and are never printed or written into this report:

```bash
./scripts/start-all.sh
mvn -q -B -f spark-apps/tpch-star-schema/pom.xml package
uv run python scripts/resolve_dataset.py tpch --scale tiny
# authenticated Airflow /api/v2 requests use admin:<redacted>
# exact reviewed JAR publication uses MinIO access key/secret: <redacted>
# Spark terminal status is read in-network from spark-master:6066
# Trino reads both Iceberg $properties tables and measures
./scripts/stop-all.sh
```

The executable assertions cover exact local/published JAR SHA-256 equality; a resolver-verified
positive-size eight-object tiny publication; two explicit Airflow runs; distinct Spark drivers with
`FINISHED` and `success=true`; exact output schemas; nonempty rows, revenue, line count, and customer
join; equality of exactly the five `data_eng_lab.dataset*` provenance properties; deterministic
row/schema/checksum/provenance equality after rerun; serialized run timestamps; final DAG pause; and
volume-preserving teardown. A later subsection records the identifiers from the latest successful
replay without replacing those runtime assertions.

## Review-fix replay result

The first executable replay failed before DAG lookup because Airflow 3 `/api/v2` rejects Basic auth;
the supported `/auth/token` JWT exchange was added under a named RED/GREEN regression. The second
replay reached trigger creation and failed with HTTP 422 because Airflow 3 requires
`logical_date: null`; that request contract also received a named RED/GREEN regression. Both attempts
ran standard volume-preserving teardown, and neither failure reached Spark.

The final command above passed on 2026-08-12 in 208.05 seconds. Runtime identifiers were:

| Airflow run | Spark driver | Result |
|---|---|---|
| `issue107_first_ae61b0f99d3d4a189e2f79335403adfd` | `driver-20260812195542-0000` | Airflow success; Spark `FINISHED`, `success=true` |
| `issue107_rerun_50644f26735047d9950ff81019f170ea` | `driver-20260812195616-0001` | Airflow success; Spark `FINISHED`, `success=true` |

The first run ended at `2026-08-12T19:56:08.647176Z`; the rerun started at
`2026-08-12T19:56:13.754642Z`, proving the controlled execution did not overlap. The built and
published JAR SHA-256 was
`b15b5ca04415336aafba6d25c77b6bbff877eeea32679176f6a772073e33135d`. The gate verified both
latest table snapshots had exact schemas, nonempty rows and measures, equal deterministic checksums
between runs, and the same exact five properties bound to the resolver result. It paused the DAG and
completed `scripts/stop-all.sh`; zero `data-eng-lab` containers remained while the named MinIO volume
remained present.
