# sessionization-gh_archive-spark-iceberg

Detects user sessions from GitHub Archive events using window functions and gap-based sessionization with a 30-minute inactivity threshold.

## 1. Overview

This scenario demonstrates user session detection using Spark SQL window functions. It partitions events by `actor_login`, orders them by timestamp, uses the `LAG` window function to detect inactivity gaps exceeding 30 minutes, and assigns session IDs using a cumulative sum over gap indicators. Each output row includes the actor login and its assigned session ID.

## 2. Why This Exists

Sessionization is a foundational pattern in event-driven analytics, used to understand user behavior patterns, engagement, and activity flows. This scenario showcases advanced window function techniques that are widely applicable across clickstream analytics, user behavior tracking, and platform engagement analysis.

## 3. Architecture

```
lakehouse.silver.gh_events  →  Spark (batch)  →  lakehouse.silver.gh_sessions
```

Key components:
- **Source:** GitHub Archive events from `lakehouse.silver.gh_events`
- **Processing:** Spark (batch)
- **Sink:** `lakehouse.silver.gh_sessions`
- **Orchestration:** `sessionization_gh_archive` Airflow DAG

## 4. Data Schema

### 4.1 Input

Source: `lakehouse.silver.gh_events`

| Column | Type | Notes |
|--------|------|-------|
| `actor_login` | string | Used for partitioning |
| `created_at` | timestamp | Used for ordering and gap detection |
| `type` | string | Event type |
| `repo_name` | string | Repository name |

### 4.2 Output

- **Table:** `lakehouse.silver.gh_sessions`
- **Layer:** Silver
- **Key columns:** `actor_login`, `session_id`, `created_at`, `type`

## 5. Notebooks

- **Zeppelin (Scala):** Sections Overview → Verify; computes LAG over events partitioned by actor, detects gaps > 30 min, assigns session IDs via cumulative sum, and writes results.
- **Jupyter (PySpark):** Sections Overview → Verify; same sessionization logic using `lag()`, `when()`, and `sum().over()` window operations in PySpark.
- Both languages implement identical sessionization logic with gap detection, session ID assignment, and verification sections.

## 6. How to Run

1. Ensure the `silver` Iceberg namespace exists by running `scripts/register_iceberg.py`.
2. Populate the GitHub Archive landing zone and run the prerequisite JSON flatten scenario (`json_flatten-gh_archive-spark-iceberg`), or `make datasets` followed by the upstream DAG.
3. Open either notebook on the Atlas stack and execute all sections, or trigger the Airflow DAG:
   ```bash
   airflow dags trigger sessionization_gh_archive
   ```
4. Verify output:
   ```bash
   spark-sql -e "SELECT actor_login, COUNT(DISTINCT session_id) AS num_sessions FROM lakehouse.silver.gh_sessions GROUP BY actor_login LIMIT 10"
   ```

## 7. Dependencies

- **Dataset:** GitHub Archive events (via `lakehouse.silver.gh_events`)
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other libraries:** None

## 8. Known Issues & Caveats

- Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4.
- The `silver` namespace must exist in the Iceberg REST catalog; run `scripts/register_iceberg.py` before executing standalone.
- The 30-minute gap threshold is currently hardcoded as 1800 seconds and not externally configurable.
- Requires upstream data in `lakehouse.silver.gh_events`; ensure the JSON flatten scenario has run first.

## 9. See Also

- [json_flatten-gh_archive-spark-iceberg](../scenarios/json_flatten-gh_archive-spark-iceberg/README.md)
- [Datasets](../docs/datasets.md)
- [Lakehouse](../docs/lakehouse.md)
