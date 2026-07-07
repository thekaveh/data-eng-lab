# schema_evolution-gh_archive-spark-iceberg

Handles schema evolution in GitHub Archive events using Iceberg's schema evolution to accommodate evolving JSON schema with new fields over time.

## 1. Purpose

This scenario demonstrates Iceberg's schema evolution capabilities — a critical feature for lakehouse pipelines where source data schema changes over time. It simulates schema changes by injecting new fields into a subset of JSON data, allowing the pipeline to gracefully accept evolving schema while preserving historical records written with the original schema.

## 2. Data Model

### 2.1 Input Source

Source: Compressed JSON files from GitHub Archive landing zone (`s3a://landing/gh_archive/*.json.gz`), with simulated schema evolution via injected fields.

| Column | Type | Notes |
|---|---|---|
| All base JSON fields | varied | Standard GitHub Archive fields |
| Evolved fields | varied | New fields injected into subset of data to simulate schema changes |

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.silver.github_archive_events` | Silver | All source fields; schema evolves to include new injected fields |

## 3. Architecture

![Architecture](architectures/schema_evolution-gh_archive-spark-iceberg.html)

GitHub Archive JSON events flow from the landing zone through Spark batch processing with Iceberg's schema evolution enabled. As new fields appear in the JSON data, Iceberg automatically extends the table schema to include them, preserving historical records that were written with the original schema. No manual ALTER TABLE is required.

## 4. Notebooks

- **Zeppelin (Scala):** `zeppelin/notebook.zpln` — Sections: Overview, Read JSON Base, Inject Evolved Schema, Write with Schema Evolution, Verify Schema Evolution
- **Jupyter (PySpark):** `jupyter/notebook.ipynb` — Same sections; same schema evolution logic using PySpark

Both languages implement identical schema evolution logic with base data, evolved data injection, Iceberg schema evolution, and verification.

## 5. Orchestration

Airflow DAG: `schema_evolution_gh_archive` — a scheduled batch DAG.

## 6. Usage

1. Ensure the `silver` Iceberg namespace exists: `scripts/register_iceberg.py`
2. Populate the landing zone: `make datasets`
3. Open either notebook on the Atlas stack, or trigger the Airflow DAG:
     ```bash
     airflow dags trigger schema_evolution_gh_archive
     ```
4. Verify:
     ```bash
     spark-sql -e "DESCRIBE lakehouse.silver.github_archive_events"
     spark-sql -e "SELECT * FROM lakehouse.silver.github_archive_events LIMIT 10"
     ```

## 7. Dependencies

- **Dataset:** GitHub Archive compressed JSON from `s3a://landing/gh_archive/`
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other:** Iceberg schema evolution must be enabled

## 8. Known Issues & Caveats

Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4. The `silver` namespace must exist; run `scripts/register_iceberg.py` first. Schema evolution relies on Iceberg's native capabilities — ensure Iceberg configuration supports auto-schema evolution.

## See Also

- [Related: json_flatten-gh_archive-spark-iceberg](./json_flatten-gh_archive-spark-iceberg.md) — JSON field extraction (upstream)
- [Related: sessionization-gh_archive-spark-iceberg](./sessionization-gh_archive-spark-iceberg.md) — Consumes flattened events
- [Related: streaming_ingest-gh_archive-spark-iceberg](./streaming_ingest-gh_archive-spark-iceberg.md) — Streaming version of JSON ingest
- [Datasets](../datasets.md)
- [Lakehouse Architecture](../lakehouse.md)
