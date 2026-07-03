# scd2-online_retail-spark-iceberg

Implement Slowly Changing Dimension Type 2 (SCD2) on a customer dimension derived from online_retail.
SCD2 tracks historical changes: when a customer's segment changes, the old row is closed (effective_to set,
is_current=false) and a new row is opened with the new segment.
Scala (Zeppelin) and PySpark (Jupyter) notebooks implement the same logic.

## 1. Scenario summary
SCD2 dimension: initial load seeds 1 row (C1, standard, active), then a segment change (premium) closes
the old row and opens a new one. Effective_from/to timestamps + is_current flag track the change.
Reads from Iceberg `gold.dim_customer_scd2`, writes to the same table (row-level UPDATE + INSERT).

## 2. Why this exists
Teaches dimensional data modeling: how to maintain a slowly changing dimension that preserves historical
context (when did the change occur?) while allowing efficient current-state lookups (is_current=true).

## 3. What's in the notebooks
`zeppelin/notebook.zpln` (Scala) and `jupyter/notebook.ipynb` (PySpark), sections Overview->Verify; a `dag.py`.

## 4. How to run
Open either notebook on the Atlas stack, or trigger the `scd2_online_retail` Airflow DAG.

## 5. Data & dependencies
Requires `lakehouse.gold.dim_customer_scd2` (can be seeded inline or via dimension load from registered
`online_retail` dataset). Spark + Iceberg `lakehouse` catalog with row-level UPDATE support (Atlas A1-A4).

## 6. Known issues & caveats
Notebook execution + Scala/PySpark parity are live-gated on Atlas A1-A4. This scenario writes to
`lakehouse.gold.*`, which requires that namespace to exist in the Iceberg REST catalog. Run
`scripts/register_iceberg.py` (creates `bronze`, `silver`, and `gold`) before executing this scenario
standalone. Iceberg row-level UPDATE is an SQL extension; ensure `iceberg.sql.extensions` is enabled.
The notebook's seed INSERT is not guarded; re-running the full notebook accumulates seed rows — drop the
target table first for a clean demo.
