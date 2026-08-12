# TPC-H Star-Schema Production Design

**Issue:** [#107](https://github.com/thekaveh/data-eng-lab/issues/107)

**Status:** Approved for implementation from the reviewed #82 execution-mode matrix and the explicit #107 delivery authorization.

## 1. Purpose and boundaries

Productionize the paired `star_schema-tpch-spark-iceberg` notebooks without replacing them as the educational surface. The production path consists of one reviewed Scala/Spark application, one operator-owned Airflow DAG, and the repository-standard Jenkins build and publish pipeline. It produces exactly these tables:

- `lakehouse.gold.dim_customer`
- `lakehouse.gold.fct_orders`

Issue #83 remains responsible for the downstream Trino orchestration. This change does not add a Trino client, provider, DAG, or query task. It also does not change Atlas source, the dataset registry, the dataset lock, or dependency versions.

## 2. Source and provenance contract

Each run selects one explicit scale using this precedence:

1. Airflow `dag_run.conf.dataset_scale`;
2. `DATASET_SCALE` from the runtime environment;
3. `small`.

Only `tiny`, `small`, and `medium` are valid. Resolution happens inside operator execution, never while Airflow imports the DAG.

The resolver request is exactly `{"dataset":"tpch","expected_scale":"<scale>"}`. The response must have the exact reviewed top-level and object fields, bounded size and nesting, lowercase SHA-256 identifiers, a UUIDv4 publication identifier, and the exact registry order:

1. `customer.parquet`
2. `lineitem.parquet`
3. `nation.parquet`
4. `orders.parquet`
5. `part.parquet`
6. `partsupp.parquet`
7. `region.parquet`
8. `supplier.parquet`

Every URI must equal `s3://landing/tpch/_generations/<plan-id>/<publication-id>/<object-name>`. Duplicate, missing, extra, reordered, malformed, flat, or cross-generation objects fail before Spark submission. Airflow passes all eight canonical `s3://` URIs followed by exact `--dataset-scale`, `--plan-id`, `--publication-id`, and `--manifest-sha256` metadata arguments. The Scala boundary independently validates the same eight-name/order/generation contract, checks that the explicit plan and publication values equal the URI components, validates the scale and manifest digest, then changes only the leading `s3://` scheme to `s3a://`. There is no flat-path fallback.

The application reads `customer.parquet`, `orders.parquet`, and `lineitem.parquet`. Carrying all eight objects across the executable boundary proves that the selected input is one complete, resolver-verified TPC-H publication rather than an ad hoc three-file set.

## 3. Application architecture

The Maven application lives at `spark-apps/tpch-star-schema` and targets the repository's existing Java 17, Scala 2.13.14, and Spark 4.1.2 runtime.

The units are:

- `TpchSources`: the eight canonical object names and the immutable URI parser. It returns a typed name-to-`s3a` map only after validating the complete argument set.
- `StarSchemaTransforms`: pure DataFrame transformations plus pre-write source integrity checks.
- `TpchStarSchema`: the entrypoint, namespace creation, eager validation/materialization, ordered Iceberg replacement, and result reporting.

Spark and Iceberg runtime libraries remain `provided`; the application must not install or download dependencies at runtime.

## 4. Transform and schema contract

For valid TPC-H inputs, the transformations match both notebooks exactly.

`dim_customer` is a projection of `customer` in this exact order:

| Column | Spark type | Meaning |
|---|---|---|
| `c_custkey` | `long` | Customer key |
| `c_name` | `string` | Customer name |
| `c_nationkey` | `integer` | Nation key |
| `c_mktsegment` | `string` | Market segment |

`fct_orders` is the inner join of orders to lineitems on `o_orderkey = l_orderkey`, grouped in this exact order:

| Column | Spark type | Meaning |
|---|---|---|
| `o_orderkey` | `long` | Order key |
| `o_custkey` | `long` | Customer foreign key |
| `o_orderdate` | `date` | Order date |
| `revenue` | `decimal(25,2)` | `sum(l_extendedprice)` |
| `line_count` | `long` | `count(*)` |

The production path fails closed before writes when required columns are missing or have unexpected types; a customer, order, lineitem, or composite lineitem key is null or duplicated; an order references a missing customer; or a lineitem references a missing order. These checks do not alter results for valid locked TPC-H data. They make the documented primary/foreign-key meaning enforceable and prevent Spark's null join behavior from silently dropping invalid locked input.

Output order is not a table contract. Tests compare keyed rows or sorted query results. Decimal revenue is not converted to floating point.

## 5. Replacement, failure, and recovery contract

The application creates `lakehouse.gold` if necessary. It computes both output DataFrames, validates them, and materializes both before the first write, so source/read/transform failures cannot create a partial replacement. Each table replacement atomically carries these Iceberg properties with that table's new state:

- `data_eng_lab.dataset=tpch`
- `data_eng_lab.dataset.scale=<scale>`
- `data_eng_lab.dataset.plan_id=<plan-id>`
- `data_eng_lab.dataset.publication_id=<publication-id>`
- `data_eng_lab.dataset.manifest_sha256=<manifest-sha256>`

Iceberg/Spark does not provide one atomic commit spanning two independent tables. The application therefore replaces `dim_customer` first and `fct_orders` second. Publishing a new fact against an old dimension can produce missing customer joins; publishing the dimension first is less harmful because every new fact is withheld until its matching dimension exists, while old fact rows normally join the stable TPC-H customer keys. A process or catalog failure during the second replacement can still leave a new dimension with the previous fact table. The task must fail, Airflow must report failure, and the operational recovery is to rerun the same immutable generation; both replacements are deterministic and converge to the same row set and provenance.

After both replacements, the application reads the five properties back from both Iceberg tables and requires exact equality with the intended run metadata and with each other. A between-write test injects failure after the dimension replacement, observes the mixed state, reruns the same generation, and proves convergence. Airflow cannot report success while either table is missing or the properties differ.

`createOrReplace()` provides deterministic table-state replacement, not snapshot-ID stability. A rerun can create new Iceberg snapshots while preserving schemas, row counts, keyed result rows, aggregate measures, deterministic checksums, and provenance values. Spark verifies with `SHOW TBLPROPERTIES`; downstream #83 can reject a mixed generation by comparing the five rows exposed through Trino's Iceberg `lakehouse.gold."dim_customer$properties"` and `lakehouse.gold."fct_orders$properties"` metadata tables before running BI SQL.

## 6. Airflow and Jenkins contract

The DAG ID is `tpch_star_schema`; its one task is `submit_tpch_star_schema`. It is owned by `data-eng-lab`, does not catch up, retries once after two minutes, and runs daily with explicit UTC start semantics. Manual runs may override `dataset_scale`.

The task uses:

- `conn_id="spark_default"`;
- `deploy_mode="cluster"`;
- `spark://spark-master:7077`;
- `spark.standalone.submit.waitAppCompletion=true`;
- `RestConfirmingSparkHook` against `spark-master:6066` through Atlas's adapter;
- `s3a://jars/tpch-star-schema/0.1.0/app.jar`;
- `com.thekaveh.dataeng.tpch.TpchStarSchema`;
- the repository-standard S3A, Iceberg REST catalog, credential, and Spark event-log settings.

Airflow success requires normal Spark submission success followed by the Atlas terminal confirmation `driverState=FINISHED` and `success=true`. Importing the DAG performs no DNS, HTTP, S3, or resolver access.

Jenkins runs Maven tests, packages the reviewed JAR, and publishes it to the exact application URI using injected MinIO endpoint and Iceberg credentials. The existing wildcard `spark-apps` Airflow mount and `make build-apps` discovery already include the new directory, so Compose requires no path change.

## 7. Testing and acceptance

Offline tests cover:

- exact URI count, name order, canonical syntax, UUIDv4, duplicate rejection, one-generation enforcement, and explicit provenance cross-checks;
- exact source map and `s3` to `s3a` conversion boundary;
- scale precedence, exact resolver request/response validation, and no import-time network access;
- dimension projection, fact join/grouping, decimal revenue, line counts, exact schemas, null/key/integrity failures, and failure propagation;
- atomic per-table provenance properties, read-back equality, deterministic replacement call order, and the documented between-write failure/rerun convergence contract;
- operator ownership, Atlas hook wrapping, Spark configuration, JAR/class/arguments, retry, and daily schedule;
- Maven/Jenkins conventions and execution-mode/doc projections.

Live acceptance uses `tiny`: build and publish the reviewed JAR, ensure a verified TPC-H publication, trigger Airflow with `{"dataset_scale":"tiny"}`, require Airflow success and Spark `FINISHED`/`success=true`, query both nonempty tables and a meaningful segment revenue join, compare both `$properties` metadata tables, rerun the same generation, and compare schemas, row counts, keyed aggregates, deterministic checksums, and provenance. Teardown preserves volumes.

Only after live acceptance may the canonical execution-mode row change from `approved new production DAG` to `existing production DAG`, with `spark-apps/tpch-star-schema/dag.py` and the daily schedule as the entrypoint contract. At that point the scenario README, Spark-app README, notebook index, diagrams, generated site, wiki, go-live notes, and changelog must agree.

## 8. Rejected alternatives

- **Pass only three files:** smaller arguments, but it no longer proves consumption of one complete reviewed TPC-H publication and cannot detect missing/extra resolver objects. Rejected.
- **Resolve inside Scala:** makes the Spark driver a network client and duplicates the operator's scale/orchestration responsibility. Rejected.
- **Use flat landing prefixes or globbing:** bypasses generation immutability. Rejected.
- **Claim cross-table atomicity:** unsupported by the current independent Iceberg table API. Rejected in favor of eager validation, safe ordering, explicit residual risk, and deterministic rerun recovery.
