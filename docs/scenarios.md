# Scenarios

Each scenario lives in a flat folder `scenarios/<pattern>-<dataset>-<engine>-<format>/` and is
**self-contained**: a 6-section `README.md`, a Scala Spark `zeppelin/notebook.zpln`, a PySpark
`jupyter/notebook.ipynb`, and an optional Airflow `dag.py`. Structure is enforced by the repo verifier.

## Add a scenario
```bash
make new-scenario NAME=medallion-nyc_taxi-spark-iceberg
uv run python scripts/verify_repo.py --root .   # validates structure
```
Then fill the notebook sections (`1. Overview` … `6. Verify`) with the scenario's Scala/PySpark logic.
The **Scala and PySpark notebooks must produce equivalent output** — Phase 2b captures a snapshot of
each and compares them with `tests/scenarios/parity.py`.

## Status
The scenario framework (scaffolder + verifier + parity harness) is in place; the curated scenario
notebooks are authored in Phase 2b (once the Atlas lakehouse — A1–A4 — is live).
