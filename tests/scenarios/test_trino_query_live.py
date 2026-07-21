import importlib.util
import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.infra


def _live_exec():
    spec = importlib.util.spec_from_file_location(
        "live_exec", ROOT / "tests" / "scenarios" / "live_exec.py"
    )
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


@pytest.mark.skipif(os.environ.get("RUN_INFRA") != "1",
                    reason="needs live Atlas Trino (issue #268)")
def test_trino_counts_bronze():
    from trino.dbapi import connect  # gated import
    # TRINO_PORT: env var > infra/.env (BASE_PORT: auto means no fixed default).
    port = _live_exec()._env_val("TRINO_PORT")
    if not port:
        pytest.skip("TRINO_PORT unresolved — is the stack up?")
    cur = connect(host=os.environ.get("TRINO_HOST", "localhost"),
                  port=int(port),
                  user="atlas", catalog="lakehouse").cursor()
    cur.execute("SELECT count(*) FROM lakehouse.bronze.nyc_taxi_trips")
    assert cur.fetchone()[0] >= 0
