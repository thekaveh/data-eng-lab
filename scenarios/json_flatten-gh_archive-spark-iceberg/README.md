<!-- AUTO-GENERATED — do not edit; run scripts/build_docs.py -->
# json_flatten-gh_archive-spark-iceberg

Reads GitHub Archive nested JSON events, extracts and flattens nested fields, casts timestamps, and writes to a flat Iceberg silver table.

## 1. Purpose

Handling semi-structured nested data is a common ETL pattern in data engineering. This scenario demonstrates converting messy JSON into well-typed columns using Spark's built-in `get_json_object` and `col` dot-notation for extracting deeply nested fields (like `actor.login` and `repo.name`), casting `created_at` to a proper timestamp, and writing the result as a flat Iceberg table for downstream consumption.

## 2. Data Model

### 2.1 Input Source

Source: `s3a://landing/gh_archive/*.json.gz` (compressed JSON files from GitHub Archive, downloaded via `make datasets`).

| Column | Type | Source |
|---|---|---|
| `id` | long | JSON: `id` |
| `type` | string | JSON: `type` |
| `actor_login` | string | JSON: `actor.login` |
| `repo_name` | string | JSON: `repo.name` |
| `created_at` | timestamp | JSON: `created_at` (cast from string) |

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.silver.gh_events` | Silver | `id`, `type`, `actor_login`, `repo_name`, `created_at` |

## 3. Architecture

![Architecture](architectures/json_flatten-gh_archive-spark-iceberg.svg)

Data flows from compressed JSON files in S3 through Spark batch processing. Nested fields are extracted using dot notation (`col("actor.login")`), timestamps are cast to proper types, and the flattened result is written to an Iceberg silver table.

## 4. Notebooks

- **Zeppelin (Scala):** `zeppelin/notebook.zpln` — Sections: Overview, Read JSON from S3, Extract Nested Fields, Cast Timestamps, Write to Iceberg, Verify
- **Jupyter (PySpark):** `jupyter/notebook.ipynb` — Same sections; same JSON flatten logic using `col("actor.login")` syntax and `toTimestamp`

Both languages implement identical JSON flatten logic with source read, field extraction, type casting, and sink write.

## 5. Orchestration

Airflow DAG: `json_flatten_gh_archive` — a scheduled batch DAG.

## 6. Usage

1. Ensure the `silver` Iceberg namespace exists: `scripts/register_iceberg.py`
2. Populate the landing zone: `make datasets`
3. Open either notebook on the Atlas stack, or trigger the Airflow DAG:
      ```bash
   airflow dags trigger json_flatten_gh_archive
      ```
4. Verify output:
      ```bash
   spark-sql -e "SELECT COUNT(*) FROM lakehouse.silver.gh_events"
      ```

## 7. Dependencies

- **Dataset:** GitHub Archive compressed JSON from `s3a://landing/gh_archive/`
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other:** None

## 8. Known Issues & Caveats

Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4. The `silver` namespace must exist; run `scripts/register_iceberg.py` first. `make datasets` is required to populate the GitHub Archive landing zone before the notebook can read data.

## See Also

- [Related: schema_evolution-gh_archive-spark-iceberg](../schema_evolution-gh_archive-spark-iceberg/README.md) — Another GitHub Archive processing scenario
- [Related: sessionization-gh_archive-spark-iceberg](../sessionization-gh_archive-spark-iceberg/README.md) — Consumes flattened events from this scenario
- [Related: streaming_ingest-gh_archive-spark-iceberg](../streaming_ingest-gh_archive-spark-iceberg/README.md) — Streaming version of JSON ingest
- [Datasets](../../README.md#datasets)
- [Lakehouse Architecture](../../README.md#lakehouse-architecture)
