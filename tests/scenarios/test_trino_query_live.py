import os

import pytest

pytestmark = pytest.mark.infra


@pytest.mark.skipif(os.environ.get("RUN_INFRA") != "1",
                    reason="needs live Atlas Trino (issue #268)")
def test_trino_counts_bronze():
    from trino.dbapi import connect  # gated import
    # TRINO_PORT is the host-published port, default 63029; source infra/.env before RUN_INFRA=1.
    cur = connect(host=os.environ.get("TRINO_HOST", "localhost"),
                  port=int(os.environ.get("TRINO_PORT", "63029")),
                  user="atlas", catalog="lakehouse").cursor()
    cur.execute("SELECT count(*) FROM lakehouse.bronze.nyc_taxi_trips")
    assert cur.fetchone()[0] >= 0
