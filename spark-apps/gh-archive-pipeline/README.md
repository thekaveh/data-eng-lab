# GitHub Archive Flatten and Sessionization Production Pipeline

This bounded batch application replaces `lakehouse.silver.gh_events` and then
`lakehouse.silver.gh_sessions` from one resolver-verified immutable GH Archive generation. Jenkins
publishes `s3a://jars/gh-archive-pipeline/0.1.0/app.jar`. Airflow runs
`gh_archive_flatten_sessionization` `@daily` or with `{"dataset_scale":"tiny|small|medium"}`.

## Entrypoints and input

The separate Spark stages are `com.thekaveh.dataeng.gharchive.GhArchiveFlatten` and
`com.thekaveh.dataeng.gharchive.GhArchiveSessionization`. One Airflow resolver task validates the
exact chronological registry inventory and returns one bounded canonical XCom payload. Both tasks
consume that same payload; neither resolves independently. Canonical `s3://` generation URIs are
converted to `s3a://` only inside the flatten application. There is no flat landing fallback,
directory discovery, refresh, `readStream`, or Redpanda dependency.

The required JSON paths `id`, `type`, `actor.login`, `repo.name`, and `created_at` are strings.
Unrelated payload fields are allowed. Required values are non-null and nonblank, IDs are unique, and
timestamps must be exact whole-second UTC `yyyy-MM-dd'T'HH:mm:ss'Z'` values.

## Outputs

`gh_events` is exactly `id string, type string, actor_login string, repo_name string, created_at
timestamp`. `gh_sessions` retains those columns and adds `previous_created_at timestamp`,
`new_session integer`, and `session_id long`. Sessions partition by actor and order by
`(created_at, id)`. The first event starts session 1; a gap greater than 1,800 seconds starts another;
exactly 1,800 seconds stays in the current session. Source, events, and sessions conserve rows.

Both tables carry exactly:

- `data_eng_lab.dataset=gh_archive`
- `data_eng_lab.dataset.scale`
- `data_eng_lab.dataset.plan_id`
- `data_eng_lab.dataset.publication_id`
- `data_eng_lab.dataset.manifest_sha256`

Consumers must fail closed unless this query returns no rows:

```sql
WITH expected(key) AS (VALUES
  'data_eng_lab.dataset', 'data_eng_lab.dataset.scale', 'data_eng_lab.dataset.plan_id',
  'data_eng_lab.dataset.publication_id', 'data_eng_lab.dataset.manifest_sha256'),
events AS (
  SELECT key, value FROM lakehouse.silver."gh_events$properties"
  WHERE key IN (SELECT key FROM expected)),
sessions AS (
  SELECT key, value FROM lakehouse.silver."gh_sessions$properties"
  WHERE key IN (SELECT key FROM expected))
SELECT e.key, v.value AS events_value, s.value AS sessions_value
FROM expected e
LEFT JOIN events v ON e.key = v.key
LEFT JOIN sessions s ON e.key = s.key
WHERE v.value IS NULL OR s.value IS NULL OR v.value <> s.value;
```

## Failure and recovery

Cross-table replacement is not atomic. Flatten runs first; a session failure can leave mixed
generations, which the properties query rejects. Rerun the same immutable generation to converge
both deterministic tables. `max_active_runs=1` serializes the supported production path. Concurrent direct
JAR invocations are unsupported.

The paired Zeppelin and Jupyter notebooks are educational parity surfaces, not a production write path.
They directly replace the same table names without the canonical XCom handshake, complete
provenance, serialization, REST terminal confirmation, or readback validation.

## Build

```bash
mvn -q -B -f spark-apps/gh-archive-pipeline/pom.xml test
mvn -q -B -f spark-apps/gh-archive-pipeline/pom.xml package
```
