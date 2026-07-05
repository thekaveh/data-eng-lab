# json_flatten-gh_archive-spark-iceberg

Reads GitHub Archive nested JSON events, extracts and flattens nested fields, casts timestamps, and writes to a flat Iceberg silver table.

## 1. Overview

This scenario demonstrates nested JSON field extraction and type casting with Apache Spark. It reads compressed JSON event files from GitHub Archive, extracts deeply nested fields such as `actor.login` and `repo.name`, casts `created_at` to a proper timestamp type, and writes the result as a flat Iceberg table for downstream consumption.

## 2. Why This Exists

Handling semi-structured nested data is a common ETL pattern in data engineering. This scenario shows how to convert messy JSON into well-typed columns using Spark's built-in `get_json_object` and `col` dot-navigation, a foundational skill for lakehouse pipelines.

## 3. Architecture

```
s3a://landing/gh_archive/*.json.gz  →  Spark (batch)  →  lakehouse.silver.gh_events
```

Key components:
- **Source:** GitHub Archive compressed JSON
- **Processing:** Spark (batch)
- **Sink:** `lakehouse.silver.gh_events`
- **Orchestration:** `json_flatten_gh_archive` Airflow DAG

## 4. Data Schema

### 4.1 Input

Source: `s3a://landing/gh_archive/*.json.gz`

| Column | Type | Source |
|--------|------|--------|
| `id` | long | JSON: `id` |
| `type` | string | JSON: `type` |
| `actor_login` | string | JSON: `actor.login` |
| `repo_name` | string | JSON: `repo.name` |
| `created_at` | timestamp | JSON: `created_at` (cast) |

### 4.2 Output

- **Table:** `lakehouse.silver.gh_events`
- **Layer:** Silver
- **Key columns:** `id`, `type`, `actor_login`, `repo_name`, `created_at`

## 5. Notebooks

- **Zeppelin (Scala):** Sections Overview → Verify; reads JSON from S3, extracts nested fields with dot notation, casts created_at, writes to Iceberg.
- **Jupyter (PySpark):** Sections Overview → Verify; same logic implemented in PySpark using `col("actor.login")` syntax and `toTimestamp`.
- Both languages implement identical JSON flatten logic with source read, field extraction, type casting, and sink write sections.

## 6. How to Run

1. Ensure the `silver` Iceberg namespace exists by running `scripts/register_iceberg.py`.
2. Populate the landing zone: run `make datasets` to download GitHub Archive data to S3.
3. Open either notebook on the Atlas stack and execute all sections, or trigger the Airflow DAG:
   ```bash
   airflow dags trigger json_flatten_gh_archive
   ```
4. Verify output:
   ```bash
   # Via Zeppelin notebook Verify section or via spark-sql:
   spark-sql -e "SELECT COUNT(*) FROM lakehouse.silver.gh_events"
   ```

## 7. Dependencies

- **Dataset:** GitHub Archive compressed JSON from `s3a://landing/gh_archive/`
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other libraries:** None

## 8. Known Issues & Caveats

- Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4.
- The `silver` namespace must exist in the Iceberg REST catalog; run `scripts/register_iceberg.py` before executing standalone.
- `make datasets` is required to populate the GitHub Archive landing zone before the notebook can read data.

## 9. See Also

- [schema_evolution-gh_archive-spark-iceberg](../scenarios/schema_evolution-gh_archive-spark-iceberg/README.md)
- [sessionization-gh_archive-spark-iceberg](../scenarios/sessionization-gh_archive-spark-iceberg/README.md)
- [streaming_ingest-gh_archive-spark-iceberg](../scenarios/streaming_ingest-gh_archive-spark-iceberg/README.md)
- [Datasets](../docs/datasets.md)
- [Lakehouse](../docs/lakehouse.md)
