import importlib.util
import os
from pathlib import Path
from urllib.parse import urlparse

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
    try:
        endpoint = _live_exec()._http_endpoint("TRINO_HOST_ENDPOINT", "TRINO_PORT")
    except RuntimeError as exc:
        pytest.skip(str(exc))
    parsed = urlparse(endpoint)
    if not parsed.hostname or not parsed.port:
        pytest.skip("TRINO_HOST_ENDPOINT unresolved — is the stack up?")
    cur = connect(host=parsed.hostname, port=parsed.port, user="atlas", catalog="lakehouse").cursor()
    cur.execute("SELECT count(*) FROM lakehouse.bronze.nyc_taxi_trips")
    assert cur.fetchone()[0] >= 0
