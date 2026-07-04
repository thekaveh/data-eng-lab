import importlib.util
import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.infra


def _parity():
    spec = importlib.util.spec_from_file_location("parity", ROOT / "tests" / "scenarios" / "parity.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def _live_exec():
    spec = importlib.util.spec_from_file_location(
        "live_exec", ROOT / "tests" / "scenarios" / "live_exec.py"
    )
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


@pytest.mark.skipif(os.environ.get("RUN_INFRA") != "1",
                    reason="needs a live enhanced-Atlas stack (A1-A4)")
def test_batch_ingest_scala_pyspark_parity():
    """Execute both notebooks, snapshot lakehouse.bronze.nyc_taxi_trips, assert parity.

    Protocol
    --------
    1. Run the Scala notebook in Zeppelin via its REST API (import / run-all / poll /
       delete).  Snapshot ``lakehouse.bronze.nyc_taxi_trips`` → ``scala_snap``.
    2. Drop the table via pyiceberg so the PySpark run starts from a clean slate.
    3. Run the PySpark notebook inside the jupyterhub container (docker cp + papermill).
       Snapshot the table again → ``pyspark_snap``.
    4. Assert ``parity.tables_equivalent(scala_snap, pyspark_snap)``.

    Both notebooks write to the same Iceberg table, so the Scala snapshot is captured
    before the table is reset, ensuring both snapshots represent a full single-engine
    write (same schema, same row count, same deterministic checksum).

    Live requirements
    -----------------
    - RUN_INFRA=1
    - ICEBERG_REST_PORT, MINIO_PORT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD in env or
      infra/.env (mirroring lakehouse/catalog.py conventions)
    - ZEPPELIN_PORT in env or infra/.env
    - PROJECT_NAME in env or infra/.env (defaults to "data-eng-lab")
    - jupyterhub container running with papermill or jupyter-nbconvert available
    - docker CLI available on the host
    """
    parity = _parity()
    le = _live_exec()

    scenario = ROOT / "scenarios" / "batch_ingest-nyc_taxi-spark-iceberg"
    zpln_path = scenario / "zeppelin" / "notebook.zpln"
    ipynb_path = scenario / "jupyter" / "notebook.ipynb"

    # Step 1: Run Scala notebook via Zeppelin REST API
    le.run_zeppelin_note(str(zpln_path))
    scala_snap = le.snapshot_table("lakehouse.bronze.nyc_taxi_trips")

    # Step 2: Drop/reset the table so PySpark writes from scratch
    le.drop_table("lakehouse.bronze.nyc_taxi_trips")

    # Step 3: Run PySpark notebook inside the jupyterhub container
    le.run_jupyter_note(str(ipynb_path))
    pyspark_snap = le.snapshot_table("lakehouse.bronze.nyc_taxi_trips")

    # Step 4: Assert schema + row_count + checksum parity
    ok, detail = parity.tables_equivalent(scala_snap, pyspark_snap)
    assert ok, detail
