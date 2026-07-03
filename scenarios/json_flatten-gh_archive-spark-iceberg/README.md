# json_flatten-gh_archive-spark-iceberg

Extract and flatten nested JSON from GitHub Archive: `s3a://landing/gh_archive/*.json.gz` →
`lakehouse.silver.gh_events`. Demonstrates nested JSON field extraction and type casting.
Scala (Zeppelin) and PySpark (Jupyter) notebooks implement the same logic.

## 1. Scenario summary
JSON flatten transform: read GitHub Archive JSON events from S3 → extract nested fields (actor.login,
repo.name) → cast created_at to timestamp → write to Iceberg `lakehouse.silver.gh_events`.

## 2. Why this exists
Demonstrates handling nested JSON data structures and converting them to typed columns, a common
ETL pattern for semi-structured data.

## 3. What's in the notebooks
`zeppelin/notebook.zpln` (Scala) and `jupyter/notebook.ipynb` (PySpark), sections Overview->Verify;
a `dag.py`.

## 4. How to run
Open either notebook on the Atlas stack (after running `make datasets` for GitHub Archive), or
trigger the `json_flatten_gh_archive` Airflow DAG.

## 5. Data & dependencies
Requires GitHub Archive dataset in S3 (`s3a://landing/gh_archive/*.json.gz`); Spark + Iceberg
`lakehouse` catalog (Atlas A1-A4).

## 6. Known issues & caveats
Notebook execution + Scala/PySpark parity are live-gated on Atlas A1-A4. This scenario writes to
`lakehouse.silver.*`, which requires the `silver` namespace to exist in the Iceberg REST catalog.
Run `scripts/register_iceberg.py` (creates `bronze`, `silver`, and `gold`) before executing this
scenario standalone. Use `make datasets` to populate the GitHub Archive landing zone.
