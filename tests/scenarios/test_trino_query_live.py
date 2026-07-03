import os

import pytest

pytestmark = pytest.mark.infra


@pytest.mark.skipif(os.environ.get("RUN_INFRA") != "1",
                    reason="needs live Atlas Trino (issue #268)")
def test_trino_counts_bronze():
    from trino.dbapi import connect  # gated import
    cur = connect(host=os.environ.get("TRINO_HOST", "localhost"),
                  port=int(os.environ.get("TRINO_PORT", "8080")),
                  user="data-eng", catalog="lakehouse").cursor()
    cur.execute("SELECT count(*) FROM lakehouse.bronze.nyc_taxi_trips")
    assert cur.fetchone()[0] >= 0
