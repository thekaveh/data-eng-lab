# Phase 15 — Scala/PySpark Parity Harness + Go-Live Runbook Fixes

## Branch

`plan/phase-15-live-parity`

---

## Task 1: Real Parity Harness

### New file: `tests/scenarios/live_exec.py`

Three public functions, all heavy deps imported inside each function (offline-safe):

| Function | Description |
|---|---|
| `snapshot_table(table)` | Loads Iceberg table via pyiceberg REST catalog; returns `{"schema": [...], "row_count": int, "checksum": str}`. Schema = sorted `"name:type"` strings; checksum = SHA-256[:16] of all rows sorted by all columns (deterministic, insertion-order-independent). |
| `run_zeppelin_note(zpln_path, port_env)` | Imports `.zpln` via `POST /api/notebook/import`, runs via `POST /api/notebook/job/{id}`, polls `GET /api/notebook/job/{id}` every 5 s (10-minute timeout), deletes on exit. Raises on any ERROR paragraph. Port resolved from `ZEPPELIN_PORT` env / `infra/.env`. |
| `run_jupyter_note(ipynb_path, project)` | Uses `docker cp` to copy notebook into `{PROJECT_NAME}-jupyterhub:/tmp/`, then `docker exec … sh -c "papermill … || nbconvert …"`. Scenarios dir is NOT mounted into jupyterhub per `compose/data-eng-lab.yml` — `docker cp` is the correct approach. 10-minute timeout. |

Also: `drop_table(table)` — drops via pyiceberg, no-op if absent.

### Updated: `tests/scenarios/test_scenario_execution_live.py`

Replaced the vacuous placeholder with a real four-step executor:
1. `run_zeppelin_note(zpln_path)` → `snapshot_table("lakehouse.bronze.nyc_taxi_trips")` → `scala_snap`
2. `drop_table("lakehouse.bronze.nyc_taxi_trips")` (clean slate)
3. `run_jupyter_note(ipynb_path)` → `snapshot_table(...)` → `pyspark_snap`
4. `assert parity.tables_equivalent(scala_snap, pyspark_snap)` — schema + row_count + checksum

`pytestmark = pytest.mark.infra` + `skipif(RUN_INFRA != "1")` retained. No `assert True`.

---

## Task 2: Go-Live Runbook Fixes (`docs/go-live.md`)

### Preflight sections (§2.3, §2.4)

| Before | After |
|---|---|
| `tests/integration/test_preflight.py::test_l1_iceberg_rest` (etc., 6 lines) | `tests/infra/test_preflight_live.py::test_layer1_all_pass_against_live_stack` |
| `tests/integration/test_preflight.py::test_l2_iceberg_write` (etc., 3 lines) | `tests/infra/test_layer2_live.py::test_layer2_matrix_all_pass` |

### Validate Live section (§3)

- §3.1 full-suite command changed to `RUN_INFRA=1 uv run pytest tests/infra/ tests/scenarios/ -m infra -q`
- Added individual per-module run commands including the new parity test
- Added §3.4 "Scala/PySpark parity test" (Jenkins moved to §3.5, Airflow to §3.6, Trino/Redpanda to §3.7)
- Updated cross-reference §3.4 → §3.5 in Airflow prerequisites
- Updated success criteria bullet to include parity test assertion

---

## Verify Outputs (offline)

```
uv run pytest -m "not infra" -q          → 101 passed, 6 deselected in 2.50s
uv run pytest tests/scenarios/test_scenario_execution_live.py -q
                                         → 1 skipped (RUN_INFRA unset)
uv run ruff check .                      → All checks passed!
uv run python scripts/verify_repo.py --root .
                                         → 0 finding(s), 0 error(s)
```

---

## Design Notes

- **scenarios not mounted in jupyterhub**: `compose/data-eng-lab.yml` only mounts `datasets` and `probes` into jupyterhub. `docker cp` is used instead of assuming a volume mount.
- **Zeppelin REST API**: `POST /api/notebook/import` accepts the raw `.zpln` JSON; `POST /api/notebook/job/{id}` starts all paragraphs; `GET /api/notebook/job/{id}` returns per-paragraph statuses (`FINISHED` / `READY` = done, `ERROR` = fail).
- **Snapshot determinism**: Rows are sorted by all columns before hashing so row-insertion order doesn't affect the checksum.
- **Container naming**: Follows `{PROJECT_NAME}-{service}` (mirrors `layer2.py`'s `default_exec`).
- **Port resolution**: `_env_val()` mirrors `lakehouse/catalog.py` and `manifest.py` — env var first, then `infra/.env` last-wins, then default.
