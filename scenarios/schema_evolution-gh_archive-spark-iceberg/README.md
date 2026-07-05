# schema_evolution-gh_archive-spark-iceberg

Demonstrates Iceberg schema evolution: adding columns with NULL propagation for existing rows and renaming columns without rewriting data.

## 1. Overview

This scenario demonstrates Apache Iceberg's schema evolution capabilities. It creates an initial table, inserts a row, adds a new column (where existing rows automatically show NULL), renames an existing column (`type` to `event_type`), inserts a new row, and verifies the evolved schema is consistent across old and new data.

## 2. Why This Exists

Schema evolution without data rewriting is one of Iceberg's core differentiators. This scenario shows that adding or renaming columns is a metadata-only operation in Iceberg, a critical capability for evolving data models over time without costly full-table rewrites.

## 3. Architecture

```
Inline data  →  Spark (batch)  →  lakehouse.silver.gh_events_evolved
```

Key components:
- **Source:** Inline sample data (no external dataset)
- **Processing:** Spark (batch)
- **Sink:** `lakehouse.silver.gh_events_evolved`
- **Orchestration:** `schema_evolution_gh_archive` Airflow DAG

## 4. Data Schema

### 4.1 Input

Source: Inline sample data embedded in the notebook

| Column | Type | Notes |
|--------|------|-------|
| `id` | long | Event identifier |
| `type` | string | Original column name (later renamed) |
| `actor_login` | string | GitHub actor username |

### 4.2 Output

- **Table:** `lakehouse.silver.gh_events_evolved`
- **Layer:** Silver
- **Evolved key columns:** `id`, `event_type` (renamed from `type`), `actor_login`, `repo_name` (new, NULL for old rows)

## 5. Notebooks

- **Zeppelin (Scala):** Sections Overview → Verify; creates table with initial schema, inserts a row, evolves schema (add column + rename column), inserts a new row, and queries to demonstrate NULL propagation.
- **Jupyter (PySpark):** Sections Overview → Verify; same evolution logic implemented in PySpark using Iceberg's `updateTableSchema` and `alterTable.rename`.
- Both languages implement identical schema evolution logic with table creation, schema evolution, data insertions, and verification sections.

## 6. How to Run

1. Ensure the `silver` Iceberg namespace exists by running `scripts/register_iceberg.py`.
2. Open either notebook on the Atlas stack and execute all sections, or trigger the Airflow DAG:
   ```bash
   airflow dags trigger schema_evolution_gh_archive
   ```
3. Verify output by checking that old rows show NULL for the newly added `repo_name` column.

## 7. Dependencies

- **Dataset:** None (inline sample data)
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other libraries:** None

## 8. Known Issues & Caveats

- Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4.
- The `silver` namespace must exist in the Iceberg REST catalog; run `scripts/register_iceberg.py` before executing standalone.
- Iceberg schema evolution is a metadata-only operation — no data rewriting occurs.

## 9. See Also

- [json_flatten-gh_archive-spark-iceberg](../scenarios/json_flatten-gh_archive-spark-iceberg/README.md)
- [sessionization-gh_archive-spark-iceberg](../scenarios/sessionization-gh_archive-spark-iceberg/README.md)
- [Datasets](../docs/datasets.md)
- [Lakehouse](../docs/lakehouse.md)
