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
                    reason="needs live Atlas Redpanda + Spark kafka (issue #269)")
def test_events_topic_reachable():
    """Broker-reachability smoke test; full readStream→writeStream→bronze.events round-trip
    exercised by scenario notebooks.
    """
    from kafka import KafkaAdminClient  # gated import
    bootstrap = os.environ.get("REDPANDA_BOOTSTRAP")
    if not bootstrap:
        # REDPANDA_KAFKA_PORT: env var > infra/.env (BASE_PORT: auto means no fixed default).
        port = _live_exec()._env_val("REDPANDA_KAFKA_PORT")
        if not port:
            pytest.skip("REDPANDA_KAFKA_PORT unresolved — is the stack up?")
        bootstrap = f"localhost:{port}"
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    topics = admin.list_topics()
    assert isinstance(topics, list)  # broker reachable + metadata fetched (topic auto-created on first produce)
