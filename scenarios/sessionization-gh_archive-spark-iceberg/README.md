# sessionization-gh_archive-spark-iceberg

Detects user sessions from GitHub Archive events using window functions and gap-based sessionization with a 30-minute inactivity threshold.

## 1. Purpose

Sessionization is a foundational pattern in event-driven analytics, used to understand user behavior patterns, engagement, and activity flows. This scenario showcases advanced window function techniques: partitioning events by `actor_login`, ordering by timestamp, detecting inactivity gaps exceeding 30 minutes using the `LAG` window function, and assigning session IDs via a cumulative sum over gap indicators.

## 2. Data Model

### 2.1 Input Source

Source: `lakehouse.silver.gh_events` from the matching production flatten stage. The educational
notebooks read this typed table directly.

| Column | Type | Notes |
|---|---|---|
| `id` | string | Required source identifier; not a primary key |
| `actor_login` | string | Partition key for session detection |
| `created_at` | timestamp | Used for ordering and gap detection |
| `type` | string | Event type |
| `repo_name` | string | Repository name |

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.silver.gh_sessions` | Silver | `id`, `type`, `actor_login`, `repo_name`, `created_at`, `previous_created_at`, `new_session`, `session_id` |

## 3. Architecture

![Architecture](../../docs/diagrams/img/sessionization-gh_archive-spark-iceberg.png)

Production reads the five provenance properties before the event rows and fails closed on any
generation mismatch. Direct concurrent JAR execution is unsupported; Airflow serialization is the
production boundary.

## 4. Notebooks

- **Zeppelin (Scala):** `zeppelin/notebook.zpln` — Sections: Overview, Read Events, Compute LAG, Detect Gaps (> 30 min), Assign Session IDs, Write to Iceberg, Verify
- **Jupyter (PySpark):** `jupyter/notebook.ipynb` — Same sections; same sessionization logic using `lag()`, `when()`, and `sum().over()` window operations in PySpark

Both languages implement identical sessionization logic with gap detection, session assignment, and verification.

## 5. Orchestration

Classification: **existing production DAG**. `spark-apps/gh-archive-pipeline/dag.py` schedules
`gh_archive_flatten_sessionization` `@daily`. It runs sessionization only after a successful matching
flatten task and serializes runs with `max_active_runs=1`.

## 6. Usage

1. Ensure the `silver` Iceberg namespace exists: `scripts/register_iceberg.py`
2. Run the production flatten stage through `gh_archive_flatten_sessionization`, or prepare an
   isolated educational `gh_events` table.
3. Open either notebook only for the educational path.
4. Verify:
      ```bash
   spark-sql -e "SELECT actor_login, COUNT(DISTINCT session_id) AS num_sessions FROM lakehouse.silver.gh_sessions GROUP BY actor_login LIMIT 10"
      ```

## 7. Dependencies

- **Dataset:** GitHub Archive events (via `lakehouse.silver.gh_events`)
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other:** None

## 8. Known Issues & Caveats

The 30-minute gap threshold is fixed at 1,800 seconds. Both notebooks directly replace
`lakehouse.silver.gh_sessions`; they do not write production provenance or participate in Airflow
serialization and can invalidate downstream generation checks. Production writes must use
`gh_archive_flatten_sessionization`. The notebook consumes `gh_events` and therefore requires the
typed flatten table to exist.

## See Also

- [Related: json_flatten-gh_archive-spark-iceberg](../json_flatten-gh_archive-spark-iceberg/README.md) — Produces the typed flattened-events input
- [Related: schema_evolution-gh_archive-spark-iceberg](../schema_evolution-gh_archive-spark-iceberg/README.md) — Another GitHub Archive processing scenario
- [Datasets](../../docs/datasets.md)
- [Lakehouse Architecture](../../docs/lakehouse.md)
