# MovieLens Feature-Pipeline Production Design

**Issue:** [#108](https://github.com/thekaveh/data-eng-lab/issues/108)

**Status:** Approved for implementation from the reviewed #82 execution-mode matrix and the explicit #108 delivery authorization.

## 1. Purpose and boundaries

Productionize the paired `feature_engineering-movielens-spark-iceberg` notebooks without replacing them as the educational surface. The production path consists of one reviewed Scala/Spark application, one operator-owned Airflow DAG, and the repository-standard Jenkins build and publication pipeline. It produces exactly these two notebook-faithful tables:

- `lakehouse.gold.ml_user_features`
- `lakehouse.gold.ml_movie_features`

The application intentionally does not create the three Silver tables, rating-deviation features, genre features, or interaction features described by stale scenario documentation. Those products are absent from both executable notebooks and are outside #108. This change also does not alter the dataset registry or lock, Atlas source, dependency versions, or another child issue.

## 2. Scale, artifact, and immutable source contract

Each run selects one explicit scale using this precedence:

1. Airflow `dag_run.conf.dataset_scale`;
2. `DATASET_SCALE` from the runtime environment;
3. `small`.

Only `tiny`, `small`, and `medium` are valid. Resolution happens inside operator execution, never while Airflow imports the DAG. The request is exactly `{"dataset":"movielens","expected_scale":"<scale>"}`.

The current reviewed dataset registry maps `tiny` and `small` to artifact `latest_small`, and `medium` to artifact `release_25m`. The resolver must preserve each selected artifact's declared `outputs` order. That order legitimately varies by scale because the registry freezes the reviewed archive-member publication order rather than inventing a consumer-side alphabetical order.

For `tiny` and `small`, the exact object-name and schema-ID sequence is:

| Position | Object name | Registry schema ID |
|---:|---|---|
| 1 | `links.csv` | `movielens_latest_small_links` |
| 2 | `tags.csv` | `movielens_latest_small_tags` |
| 3 | `ratings.csv` | `movielens_latest_small_ratings` |
| 4 | `README.txt` | `movielens_latest_small_readme` |
| 5 | `movies.csv` | `movielens_latest_small_movies` |

For `medium`, the exact sequence is:

| Position | Object name | Registry schema ID |
|---:|---|---|
| 1 | `tags.csv` | `movielens_25m_tags` |
| 2 | `links.csv` | `movielens_25m_links` |
| 3 | `README.txt` | `movielens_25m_readme` |
| 4 | `ratings.csv` | `movielens_25m_ratings` |
| 5 | `genome-tags.csv` | `movielens_25m_genome_tags` |
| 6 | `genome-scores.csv` | `movielens_25m_genome_scores` |
| 7 | `movies.csv` | `movielens_25m_movies` |

The resolver response must have the exact reviewed top-level and object fields, bounded bytes and JSON nesting, lowercase SHA-256 values, a UUIDv4 publication ID, integral `size_bytes > 0`, and the exact name/schema-ID order for the requested scale. Every URI must equal `s3://landing/movielens/_generations/<plan-id>/<publication-id>/<object-name>`. Duplicate, missing, extra, reordered, malformed, flat, or cross-generation objects fail before Spark submission.

Airflow passes the complete resolver-ordered canonical `s3://` URI sequence followed by exact `--dataset-scale`, `--plan-id`, `--publication-id`, and `--manifest-sha256` arguments. The Scala boundary independently validates the scale-specific object names and one-generation URI contract, cross-checks the explicit metadata, and converts only the leading `s3://` scheme to `s3a://`. The application reads only the verified `ratings.csv`; carrying every artifact object across the executable boundary proves that the rating input belongs to one complete reviewed MovieLens publication. There is no flat-path fallback, globbing, or runtime download.

## 3. Application architecture

The Maven application lives at `spark-apps/movielens-feature-pipeline` and targets the repository's Java 17, Scala 2.13.14, and Spark 4.1.2 runtime.

The units are:

- `MovieLensSources`: freezes both registry object sequences and parses one immutable canonical generation into a typed source map.
- `FeatureTransforms`: defines the explicit ratings schema, input validation, and pure notebook-equivalent user/movie aggregates.
- `MovieLensFeaturePipeline`: coordinates reads, eager output validation, ordered Iceberg replacement, provenance readback, and result reporting.

Spark and Iceberg runtime libraries remain `provided`. The application must not resolve the registry, access the network, install packages, or download dependencies at runtime.

## 4. Input, aggregation, and output contract

The production CSV reader uses `header=true`, UTF-8, comma delimiter, fail-fast parsing, and this exact schema and order:

| Column | Spark type | Null allowed |
|---|---|---|
| `userId` | `long` | no |
| `movieId` | `long` | no |
| `rating` | `double` | no |
| `timestamp` | `long` | no |

Missing or additional source columns, wrong types, null fields, an empty source, or a non-finite rating fail before either write. Duplicate rating rows and repeated `(userId, movieId, timestamp)` tuples are intentionally valid separate rating events: the pipeline does not deduplicate them, and every row contributes to `count(*)` and to the corresponding mean. This preserves exact notebook semantics.

`ml_user_features` groups by `userId` and has this exact schema and order:

| Column | Spark type | Definition |
|---|---|---|
| `userId` | `long` | non-null row key |
| `avg_rating` | `double` | `avg(rating)` across all user rows |
| `num_ratings` | `long` | `count(*)` across all user rows |

`ml_movie_features` groups by `movieId` and has this exact schema and order:

| Column | Spark type | Definition |
|---|---|---|
| `movieId` | `long` | non-null row key |
| `movie_avg` | `double` | `avg(rating)` across all movie rows |
| `popularity` | `long` | `count(*)` across all movie rows |

Both outputs must be nonempty, have exactly one row per key, contain no nulls, and contain only finite averages and positive counts. The sums of `num_ratings` and `popularity` must both equal the source rating-row count. Logical output is deterministic independent of source row order; physical table row order is not a contract.

## 5. Replacement, provenance, failure, and recovery

The application creates `lakehouse.gold` when necessary. It reads and validates the source, computes both outputs, and materializes and validates both before the first write so source, parse, transform, schema, and row-key failures cannot create a partial replacement.

Each replacement carries the exact five-key convention established by #107 and required by #83:

- `data_eng_lab.dataset=movielens`
- `data_eng_lab.dataset.scale=<scale>`
- `data_eng_lab.dataset.plan_id=<plan-id>`
- `data_eng_lab.dataset.publication_id=<publication-id>`
- `data_eng_lab.dataset.manifest_sha256=<manifest-sha256>`

The application replaces `ml_user_features` first and `ml_movie_features` second, matching notebook order. Iceberg does not provide an atomic commit across two independent tables, so a process or catalog failure during the second replacement can leave a new user table beside the previous movie table. Airflow reports failure. The supported recovery is a deterministic rerun of the same immutable generation; a test injects failure between writes and proves convergence after rerun. A first-write failure performs no second write.

After both replacements, the application reads back each table's exact schema, non-null unique row key, rating-count invariant, and five provenance properties. It requires both schemas and row-key contracts to match the intended output, each property's value to match the run, and the two property maps to be equal. Airflow may report success only after these checks pass.

`createOrReplace()` provides logical result replacement, not snapshot-ID stability. An unchanged rerun may create new Iceberg snapshots while preserving schemas, keyed rows, counts, aggregate values, deterministic checksums, and provenance. Any consumer that joins the two tables must first compare the five keys exposed through Trino's Iceberg metadata tables:

```sql
WITH user_props AS (
  SELECT key, value FROM lakehouse.gold."ml_user_features$properties"
  WHERE key IN (
    'data_eng_lab.dataset', 'data_eng_lab.dataset.scale',
    'data_eng_lab.dataset.plan_id', 'data_eng_lab.dataset.publication_id',
    'data_eng_lab.dataset.manifest_sha256'
  )
), movie_props AS (
  SELECT key, value FROM lakehouse.gold."ml_movie_features$properties"
  WHERE key IN (
    'data_eng_lab.dataset', 'data_eng_lab.dataset.scale',
    'data_eng_lab.dataset.plan_id', 'data_eng_lab.dataset.publication_id',
    'data_eng_lab.dataset.manifest_sha256'
  )
)
SELECT key, user_props.value AS user_value, movie_props.value AS movie_value
FROM user_props FULL OUTER JOIN movie_props USING (key)
WHERE user_props.value IS DISTINCT FROM movie_props.value;
```

The query must return zero rows and each metadata table must contain all five keys before downstream SQL runs; absence or mismatch fails closed.

Airflow enforces `max_active_runs=1`, which is the supported production serialization boundary for the two-table replacement. Direct concurrent execution of the JAR is unsupported.

## 6. Airflow and Jenkins contract

The production DAG ID is `movielens_feature_pipeline` and its task is `submit_movielens_feature_pipeline`. It is owned by `data-eng-lab`, uses `catchup=False`, retries once after two minutes, and runs `@daily` with explicit UTC start semantics after live acceptance. Manual runs may override `dataset_scale`.

The task uses:

- `conn_id="spark_default"`;
- `deploy_mode="cluster"`;
- `spark://spark-master:7077`;
- `spark.standalone.submit.waitAppCompletion=true`;
- Atlas `RestConfirmingSparkHook` with terminal confirmation at `spark-master:6066`;
- `s3a://jars/movielens-feature-pipeline/0.1.0/app.jar`;
- `com.thekaveh.dataeng.movielens.MovieLensFeaturePipeline`;
- repository-standard S3A, Iceberg REST catalog, credential, and event-log settings.

Airflow success requires normal submission success followed by `driverState=FINISHED` and `success=true`. DAG import performs no DNS, HTTP, S3, or resolver access.

Jenkins runs Maven tests before packaging, verifies the exact reviewed JAR, and publishes it to the fixed application URI using injected MinIO/Iceberg credentials. Existing wildcard Spark-app discovery and mounts are reused unless an executable test proves a missing consumer path.

## 7. Testing and live acceptance

Offline coverage includes:

- both exact registry order/schema-ID contracts, scale precedence, bounded resolver parsing, positive sizes, canonical URIs, one generation, metadata equality, and `s3` to `s3a` conversion;
- exact source schema/read options, null/wrong/missing/extra/non-finite failures, and deliberate duplicate-event counting;
- exact output schemas, keyed aggregate rows, finite means, positive counts, rating-count equality, and source-order independence;
- eager validation before writes, user-then-movie ordering, first/second-write failure propagation, between-write failure and same-generation convergence;
- post-write schema, row-key, result, and five-property readback equality;
- DAG ownership, Atlas confirmation, runtime-only resolution, no import network, fixed JAR/class/config, `@daily`, `catchup=False`, and `max_active_runs=1`;
- Jenkins publication and synchronized execution-mode/documentation contracts.

The genuine `RUN_INFRA=1` live harness is MovieLens-specific even where it reuses hardened helper patterns from #107. It fails before mutation if any project container already exists, leaves the DAG paused throughout controlled acceptance, restores its initial pause state, and never refreshes or mutates the MovieLens pointer after an ambiguous resolver failure. A verified tiny MovieLens publication is an explicit prerequisite; intentional provisioning uses the supported bounded dataset command separately, followed by verify-only.

The harness builds/publishes the exact reviewed artifact, verifies the resolver inventory, executes two unique `airflow dags test --use-executor` runs with whole-second logical dates, uses complete bounded Airflow-v2 pagination and exact run-set differences, requires exactly one new Spark driver per run with terminal `FINISHED`/`success=true`, and rejects unexpected active or additional runs. Trino verifies both exact schemas, nonzero rows, finite/meaningful averages, equal source-count sums, all five properties, and deterministic keyed checksums across reruns. Teardown stops only its owned stack, preserves volumes, leaves the production pointer unchanged, and ends with zero project containers.

Only after live acceptance may the canonical #82 matrix row change from `approved new production DAG` to `existing production DAG`, with `spark-apps/movielens-feature-pipeline/dag.py` and `@daily` as the entrypoint contract. The scenario README, generated site, wiki, notebook index, Spark-app docs, diagram, go-live notes, and changelog must then agree.

## 8. Notebook trust boundary and rejected alternatives

The notebooks remain educational parity surfaces, not equivalent production writers. They infer CSV schema and directly replace the same two tables without the production application's complete input validation, five-property provenance, readback, serialization, or recovery checks. Running either notebook can therefore invalidate the downstream generation contract. Production writes must use the reviewed DAG/application path.

Rejected alternatives:

- **Three Silver feature tables:** based on stale prose rather than notebook behavior and expands #108. Rejected.
- **Join movies/genres or derive rating deviations:** useful possible future features, but changes established feature semantics. Rejected.
- **Pass only `ratings.csv`:** cannot prove that the source belongs to a complete resolver-reviewed artifact. Rejected.
- **Sort resolver objects alphabetically:** conflicts with the registry's scale-specific declared output order. Rejected.
- **Deduplicate rating rows:** silently changes `count(*)` and average semantics. Rejected.
- **Resolve or download in Scala:** makes the driver a network client and weakens the operator boundary. Rejected.
- **Claim cross-table atomicity or concurrent direct-JAR safety:** unsupported. Rejected in favor of eager validation, serialized Airflow runs, explicit residual risk, and deterministic rerun recovery.
