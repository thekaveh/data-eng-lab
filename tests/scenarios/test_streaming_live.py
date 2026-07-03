import os

import pytest

pytestmark = pytest.mark.infra


@pytest.mark.skipif(os.environ.get("RUN_INFRA") != "1",
                    reason="needs live Atlas Redpanda + Spark kafka (issue #269)")
def test_events_topic_reachable():
    from kafka import KafkaAdminClient  # gated import
    admin = KafkaAdminClient(bootstrap_servers=os.environ.get("REDPANDA_BOOTSTRAP", "localhost:9092"))
    topics = admin.list_topics()
    assert isinstance(topics, list)  # broker reachable + metadata fetched (topic auto-created on first produce)
