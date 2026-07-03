# incremental_upsert-online_retail-spark-iceberg

Implement CDC upserts via `MERGE INTO` on the `lakehouse.silver.online_retail` table. Demonstrates idempotent
change-set application: the same batch can be merged multiple times without duplicating data.
Scala (Zeppelin) and PySpark (Jupyter) notebooks implement the same logic.

## 1. Scenario summary
Incremental upsert: batch 1 seeds the table (2 rows), batch 2 (1 update + 1 insert) is merged idempotently.
Reads from Iceberg `silver.online_retail`, writes to the same table (MERGE IN-PLACE).

## 2. Why this exists
Teaches CDC / change-data-capture: how to upsert a batch of deltas without re-writing the entire table,
and how to ensure the operation is idempotent (re-applying the same batch yields the same result).

## 3. What's in the notebooks
`zeppelin/notebook.zpln` (Scala) and `jupyter/notebook.ipynb` (PySpark), sections Overview->Verify; a `dag.py`.

## 4. How to run
Open either notebook on the Atlas stack, or trigger the `incremental_upsert_online_retail` Airflow DAG.

## 5. Data & dependencies
Requires `lakehouse.silver.online_retail` (can be seeded inline or via the registered `online_retail` dataset
via `make datasets`). Spark + Iceberg `lakehouse` catalog (Atlas A1-A4).

## 6. Known issues & caveats
Notebook execution + Scala/PySpark parity are live-gated on Atlas A1-A4. The inline seed can be replaced by
the registered `online_retail` dataset at scale. This scenario writes to `lakehouse.silver.*`, which requires
that namespace to exist in the Iceberg REST catalog. Run `scripts/register_iceberg.py` (creates `bronze`,
`silver`, and `gold`) before executing this scenario standalone. The notebook's seed INSERT is not guarded;
re-running the full notebook accumulates seed rows — drop the target table first for a clean demo.
