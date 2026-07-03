# Lakehouse

`data-eng-lab` uses an **Apache Iceberg** lakehouse on MinIO, cataloged by the Atlas **Iceberg REST
catalog** (`lakehouse`), in a **medallion** layout: `landing` (raw) → `bronze` → `silver` → `gold`.

## Namespaces
`scripts/register_iceberg.py` creates the `bronze`/`silver`/`gold` namespaces (idempotent). It runs
automatically during `make up`, or standalone: `uv run python scripts/register_iceberg.py`.

## Integration matrix (preflight Layer 2)
`make preflight` runs Layer 1 (service existence/init) then Layer 2 (real service↔service round-trips:
Spark↔MinIO↔Iceberg, Jupyter↔PyIceberg, Airflow↔MinIO/Spark, Zeppelin↔Spark). Layer 2 executes probes
inside the containers via `docker exec` (Spark Connect is not host-reachable) and prints a pass/fail
matrix. Edges depending on undelivered Atlas items are `skipped` by manifest until enabled.

## Bronze smoke
`scripts/bronze_smoke.py` loads a landing dataset into `lakehouse.bronze.*` via Spark Connect — the
end-to-end proof that the lakehouse path works.
