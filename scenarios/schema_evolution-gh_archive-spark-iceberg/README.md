# schema_evolution-gh_archive-spark-iceberg

Demonstrate Iceberg schema evolution: add a new column, rename an existing column, and verify that
existing rows show `NULL` for the new column while new rows populate all columns.
Scala (Zeppelin) and PySpark (Jupyter) notebooks implement the same logic.

## 1. Scenario summary
Schema evolution: create table with initial schema (id, type, actor_login), insert a row, add
`repo_name` column, rename `type` to `event_type`, insert a new row, then query to show old row has
NULL for the new columns under the evolved schema.

## 2. Why this exists
Demonstrates Iceberg's schema evolution capabilities: columns can be added and renamed without
rewriting existing data, and old rows transparently show NULL for new columns.

## 3. What's in the notebooks
`zeppelin/notebook.zpln` (Scala) and `jupyter/notebook.ipynb` (PySpark), sections Overview->Verify; a `dag.py`.

## 4. How to run
Open either notebook on the Atlas stack, or trigger the `schema_evolution_gh_archive` Airflow DAG.

## 5. Data & dependencies
Spark + Iceberg `lakehouse` catalog (Atlas A1-A4); requires `lakehouse.silver` namespace to exist.

## 6. Known issues & caveats
Notebook execution + Scala/PySpark parity are live-gated on Atlas A1-A4. Run `scripts/register_iceberg.py`
(creates `bronze`, `silver`, and `gold` namespaces) before executing this scenario standalone.
