# MovieLens Feature-Pipeline Production Application

This Scala/Spark application deterministically replaces `lakehouse.gold.ml_user_features` and
`lakehouse.gold.ml_movie_features` from one resolver-verified immutable MovieLens publication.
Jenkins publishes `s3a://jars/movielens-feature-pipeline/0.1.0/app.jar`; Airflow runs
`movielens_feature_pipeline` `@daily` or on demand with
`{"dataset_scale":"tiny|small|medium"}`.

## Input contract

Tiny and small use registry artifact `latest_small` in this exact declared order:

1. `links.csv` — `movielens_latest_small_links`
2. `tags.csv` — `movielens_latest_small_tags`
3. `ratings.csv` — `movielens_latest_small_ratings`
4. `README.txt` — `movielens_latest_small_readme`
5. `movies.csv` — `movielens_latest_small_movies`

Medium uses `release_25m` in its distinct exact declared order:

1. `tags.csv` — `movielens_25m_tags`
2. `links.csv` — `movielens_25m_links`
3. `README.txt` — `movielens_25m_readme`
4. `ratings.csv` — `movielens_25m_ratings`
5. `genome-tags.csv` — `movielens_25m_genome_tags`
6. `genome-scores.csv` — `movielens_25m_genome_scores`
7. `movies.csv` — `movielens_25m_movies`

Registry order is reviewed artifact output order, not alphabetical order. Airflow passes every
canonical `s3://landing/movielens/_generations/<plan>/<publication>/...` URI, then
`--dataset-scale`, `--plan-id`, `--publication-id`, and `--manifest-sha256`. The application rejects
flat, malformed, duplicate, missing, extra, reordered, or cross-generation inputs and converts only
the verified URI scheme to `s3a://`. It reads `ratings.csv` only after the complete publication has
crossed that boundary.

The ratings CSV schema is exactly `userId long`, `movieId long`, `rating double`, and `timestamp
long`; every field is required and rating must be finite. Duplicate rating rows are intentional
separate events. They are not deduplicated: each row contributes independently to `avg` and
`count(*)`.

## Output contract

`ml_user_features` has `userId long`, `avg_rating double`, and `num_ratings long`.
`ml_movie_features` has `movieId long`, `movie_avg double`, and `popularity long`.
Both tables are nonempty with one non-null unique key, finite averages, and positive counts. The sum
of `num_ratings` and the sum of `popularity` both equal the input rating-row count.

Each table carries equal Iceberg properties:

- `data_eng_lab.dataset=movielens`
- `data_eng_lab.dataset.scale`
- `data_eng_lab.dataset.plan_id`
- `data_eng_lab.dataset.publication_id`
- `data_eng_lab.dataset.manifest_sha256`

After replacement, the application reads back and compares each table's schema, keyed rows, count
invariant, and all five properties. A consumer must fail closed before downstream SQL unless each
property exists and the following query returns zero rows:

```sql
WITH expected(property_key) AS (
  VALUES
    'data_eng_lab.dataset',
    'data_eng_lab.dataset.scale',
    'data_eng_lab.dataset.plan_id',
    'data_eng_lab.dataset.publication_id',
    'data_eng_lab.dataset.manifest_sha256'
),
user_features AS (
  SELECT key AS property_key, value AS property_value
  FROM lakehouse.gold."ml_user_features$properties"
  WHERE key IN (SELECT property_key FROM expected)
),
movie_features AS (
  SELECT key AS property_key, value AS property_value
  FROM lakehouse.gold."ml_movie_features$properties"
  WHERE key IN (SELECT property_key FROM expected)
),
bound AS (
  SELECT e.property_key,
         u.property_value AS user_value,
         m.property_value AS movie_value
  FROM expected e
  FULL OUTER JOIN user_features u ON e.property_key = u.property_key
  FULL OUTER JOIN movie_features m ON e.property_key = m.property_key
)
SELECT property_key, user_value, movie_value
FROM bound
WHERE property_key IS NULL
   OR user_value IS NULL
   OR movie_value IS NULL
   OR user_value <> movie_value;
```

## Failure, recovery, and concurrency

Both outputs are materialized and validated before writing. Iceberg cannot atomically commit two
tables, so the user table is replaced first and the movie table second, matching notebook order. A
failure between them leaves a partial generation and makes the task fail. Rerun the same immutable generation:
deterministic replacements converge both logical results and provenance. New Iceberg
snapshot IDs on rerun are expected.

The DAG uses `max_active_runs=1`; this Airflow boundary serializes the supported non-atomic
production write path. Concurrent direct application invocations are unsupported.

## Notebook trust boundary

The paired Zeppelin and Jupyter notebooks are educational parity surfaces, not a production write path.
They infer schema and directly replace the same tables without complete source validation,
provenance, serialization, or readback. Running them can invalidate downstream provenance. Use
`movielens_feature_pipeline` for production writes and use notebooks only in an isolated educational
environment.

## Build and publish

```bash
mvn -q -B -f spark-apps/movielens-feature-pipeline/pom.xml test
mvn -q -B -f spark-apps/movielens-feature-pipeline/pom.xml package
```

Jenkins tests, packages, and copies the exact JAR using injected MinIO credentials. Airflow uses
`spark_default`, cluster mode, and Atlas `RestConfirmingSparkHook`; success requires Spark
`FINISHED` with `success=true`. Spark, S3A, and Iceberg runtime dependencies are supplied by Atlas.
