import os

import pytest

pytestmark = pytest.mark.infra


@pytest.mark.skipif(os.environ.get("RUN_INFRA") != "1",
                    reason="needs live Atlas Redpanda + Spark kafka (issue #269)")
def test_events_topic_reachable():
    """Broker-reachability smoke test; full readStream→writeStream→bronze.events round-trip
    exercised by scenario notebooks.
    """
    from kafka import KafkaAdminClient  # gated import
    bootstrap = os.environ.get("REDPANDA_BOOTSTRAP") or f"localhost:{os.environ.get('REDPANDA_KAFKA_PORT', '63010')}"
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    topics = admin.list_topics()
    assert isinstance(topics, list)  # broker reachable + metadata fetched (topic auto-created on first produce)
