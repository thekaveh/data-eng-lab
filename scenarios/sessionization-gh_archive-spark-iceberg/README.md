# sessionization-gh_archive-spark-iceberg

Detect user sessions in GitHub archive data using window functions and gap-based sessionization
(30-minute inactivity threshold). Scala (Zeppelin) and PySpark (Jupyter) notebooks implement the
same sessionization logic.

## 1. Scenario summary
Gap-based sessionization: partition events by actor_login, order by timestamp, detect gaps > 30 min,
and assign session IDs using window functions. Output includes actor_login and session_id per event.

## 2. Why this exists
Demonstrates window functions (lag, sum over partition) for detecting user sessions based on
inactivity gaps. A common pattern in event-driven analytics for understanding user behavior.

## 3. What's in the notebooks
`zeppelin/notebook.zpln` (Scala) and `jupyter/notebook.ipynb` (PySpark), sections Overview->Verify;
a `dag.py` for Airflow integration.

## 4. How to run
Open either notebook on the Atlas stack (after ensuring `s3a://landing/gh_archive` data exists),
or trigger the `sessionization_gh_archive` Airflow DAG.

## 5. Data & dependencies
Requires `s3a://landing/gh_archive` (GitHub archive data in Parquet/JSON format);
Spark + Iceberg `lakehouse` catalog + S3 access (Atlas A1-A4).

## 6. Known issues & caveats
Notebook execution + Scala/PySpark parity are live-gated on Atlas A1-A4. This scenario writes to
`lakehouse.silver.gh_sessions`, which requires the `silver` namespace to exist in the Iceberg REST
catalog. Run `scripts/register_iceberg.py` (creates `bronze`, `silver`, and `gold`) before executing
this scenario standalone. The 30-min gap is configurable (currently hardcoded as 1800 seconds).
