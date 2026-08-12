# schema_evolution-gh_archive-spark-iceberg

Demonstrates explicit Iceberg schema evolution on a small scenario-owned table shaped like GitHub Archive events.

## 1. Purpose

This scenario creates a three-column Iceberg table, inserts one representative event, adds `repo_name`, renames `type` to `event_type`, inserts an evolved row, and queries both records. The explicit `ALTER TABLE` sequence keeps the schema change visible and deterministic.

## 2. Data Model

### 2.1 Input Source

Source: two scenario-owned literal rows shaped like GitHub Archive events; no downloaded dataset is read.

| Column | Type | Notes |
|---|---|---|
| `id`, `type`, `actor_login` | string | Initial table fields |
| `repo_name` | string | Added field; `type` is renamed to `event_type` |

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.silver.gh_events_se` | Silver | `id`, `event_type`, `actor_login`, `repo_name` |

## 3. Architecture

The notebook uses explicit Iceberg `ALTER TABLE ADD COLUMN` and `RENAME COLUMN` statements between its two inserts, then selects the final four-column projection.

## 4. Notebooks

- **Zeppelin (Scala):** `zeppelin/notebook.zpln` — Sections: Overview, Read JSON Base, Inject Evolved Schema, Write with Schema Evolution, Verify Schema Evolution
- **Jupyter (PySpark):** `jupyter/notebook.ipynb` — Same sections; same schema evolution logic using PySpark

Both languages implement identical schema evolution logic with base data, evolved data injection, Iceberg schema evolution, and verification.

## 5. Orchestration

Airflow DAG: `schema_evolution_gh_archive` — a scheduled batch DAG.

## 6. Usage

1. Ensure the `silver` Iceberg namespace exists: `scripts/register_iceberg.py`
2. Open either notebook on the Atlas stack, or trigger the Airflow DAG:
     ```bash
     airflow dags trigger schema_evolution_gh_archive
     ```
3. Verify:
     ```bash
     spark-sql -e "DESCRIBE lakehouse.silver.gh_events_se"
     spark-sql -e "SELECT * FROM lakehouse.silver.gh_events_se ORDER BY id"
     ```

## 7. Dependencies

- **Dataset:** none; the notebook inserts two scenario-owned rows
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other:** Iceberg schema evolution must be enabled

## 8. Known Issues & Caveats

Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4. The `silver` namespace must exist; run `scripts/register_iceberg.py` first.

## See Also

- [Related: json_flatten-gh_archive-spark-iceberg](../json_flatten-gh_archive-spark-iceberg/README.md) — JSON field extraction (upstream)
- [Related: sessionization-gh_archive-spark-iceberg](../sessionization-gh_archive-spark-iceberg/README.md) — Consumes flattened events
- [Related: streaming_ingest-gh_archive-spark-iceberg](../streaming_ingest-gh_archive-spark-iceberg/README.md) — Streaming version of JSON ingest
- [Datasets](../../docs/datasets.md)
- [Lakehouse Architecture](../../docs/lakehouse.md)
