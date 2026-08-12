# 7.5. movielens-feature-pipeline

The production Scala/Spark application deterministically replaces `lakehouse.gold.ml_user_features` and `lakehouse.gold.ml_movie_features` from one resolver-verified immutable MovieLens publication. Jenkins publishes `s3a://jars/movielens-feature-pipeline/0.1.0/app.jar`; Airflow runs `movielens_feature_pipeline` `@daily` or with an explicit manual `dataset_scale`.

## 1. Input contract

Tiny and small publications carry the five reviewed `latest_small` objects; medium carries the seven reviewed `release_25m` objects. Their registry order differs because each artifact freezes its publisher-declared output order, rather than an invented alphabetical order. Airflow passes every canonical URI in the exact scale-specific registry order plus scale, plan, publication, and manifest arguments. The application rejects flat, malformed, duplicate, missing, extra, reordered, or cross-generation inputs.

Only `ratings.csv` is read after the complete publication crosses the boundary. Its schema is exactly `userId long`, `movieId long`, `rating double`, `timestamp long`; fields are required and ratings finite. Duplicate rating rows intentionally count separately in averages and `count(*)`.

## 2. Output contract

`lakehouse.gold.ml_user_features` has `userId long`, `avg_rating double`, `num_ratings long`. `lakehouse.gold.ml_movie_features` has `movieId long`, `movie_avg double`, `popularity long`. Keys are unique and non-null; averages are finite; counts are positive; both count sums equal the input row count.

Both tables carry equal Iceberg properties for `data_eng_lab.dataset`, `data_eng_lab.dataset.scale`, `data_eng_lab.dataset.plan_id`, `data_eng_lab.dataset.publication_id`, and `data_eng_lab.dataset.manifest_sha256`. The application reads back ordered names/types, keyed rows, measures, and all five properties before success. Consumers must query both `$properties` tables and fail closed on an absent or unequal key; the concrete SQL is in the application [README](../../spark-apps/movielens-feature-pipeline/README.md#output-contract).

## 3. Failure and recovery

Iceberg cannot atomically commit two tables. Both frames are validated first, then the user table is replaced before the movie table to preserve notebook order. Any failure fails Airflow; a deterministic same-generation rerun converges both logical results and provenance. The DAG sets `max_active_runs=1`, so supported runs cannot interleave. Concurrent direct JAR invocations are unsupported.

## 4. Notebook trust boundary

The paired notebooks are educational parity surfaces, not a supported production write path. They infer schema and directly replace the same tables without complete-publication validation, production provenance, serialization, or readback. Running them can invalidate downstream provenance. Use `movielens_feature_pipeline` from `spark-apps/movielens-feature-pipeline` for production writes.

## 5. Build and run

```bash
mvn -q -B -f spark-apps/movielens-feature-pipeline/pom.xml test
mvn -q -B -f spark-apps/movielens-feature-pipeline/pom.xml package
```

Jenkins publishes the reviewed artifact. Airflow submits through `spark_default` in cluster mode with Atlas's `RestConfirmingSparkHook`; success requires Spark `FINISHED` and `success=true`.

## 6. Live evidence

On 2026-08-12, two serialized tiny-scale manual runs succeeded from the same immutable publication. Both reproduced `610` user rows, `9,724` movie rows, a rating-count sum of `100,836`, checksums `377574ef54523af2` and `4c87d628b90fe38e`, and equal five-key provenance. The first corrected run also converged the preserved partial-write state from the preceding failed acceptance attempt.

## 7. See also

- [Scenario](../scenarios/feature_engineering-movielens-spark-iceberg.md)
- [Notebook walkthrough](../notebooks/feature_engineering-movielens-spark-iceberg.md)
- [Execution-mode matrix](../scenarios/execution-modes.md)
- [Datasets](../datasets.md)
