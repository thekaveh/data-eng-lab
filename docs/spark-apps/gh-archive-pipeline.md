# 7.6. gh-archive-pipeline

The `gh-archive-pipeline` Maven application contains two independently runnable Scala entrypoints joined by one serialized production Airflow DAG:

1. `GhArchiveFlatten` reads the exact resolver-verified immutable GH Archive object set, validates its strict JSON contract, preserves identical duplicate records, rejects conflicting records that reuse an event ID, and replaces `lakehouse.silver.gh_events`.
2. `GhArchiveSessionization` verifies all five `gh_events` provenance properties against the same canonical resolver payload before reading any rows, then replaces `lakehouse.silver.gh_sessions`.

Both tables carry `data_eng_lab.dataset`, `data_eng_lab.scale`, `data_eng_lab.plan_id`, `data_eng_lab.publication_id`, and `data_eng_lab.manifest_sha256`. Session boundaries are per actor in `(created_at, id)` order: the first event and any gap greater than 1,800 seconds start a new session. `previous_created_at` is null only on each actor's first event, `new_session` is an integer, and `session_id` is a long.

## 1. Production execution

Jenkins publishes the reviewed shaded JAR to `s3a://jars/gh-archive-pipeline/0.1.0/app.jar`. Airflow DAG `gh_archive_flatten_sessionization` resolves one immutable generation, passes one bounded canonical payload to both stages, and submits each entrypoint through Atlas's REST-confirming operator. The DAG runs `@daily` with `max_active_runs=1`; direct concurrent JAR execution is unsupported because the two table replacements are not cross-table atomic.

```bash
airflow dags trigger gh_archive_flatten_sessionization \
  --conf '{"dataset_scale":"tiny"}'
```

On any partial failure, repair the cause and rerun the same generation through the serialized DAG. Consumers must compare the five properties on both tables and fail closed if they are absent or differ.

The paired educational notebooks are not an equivalent production write path: they directly replace the same tables without resolver validation, provenance, or serialization and can invalidate provenance-aware downstream reads.
