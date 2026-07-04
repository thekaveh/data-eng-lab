"""Produce synthetic events to the Redpanda `events` topic (for the streaming scenario).
Live-gated: requires Atlas Redpanda (issue #269). Run: python producer.py [count]."""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

from kafka import KafkaProducer  # kafka-python

# Host-published Redpanda: localhost:$REDPANDA_KAFKA_PORT (default 63010). In-cluster use redpanda:9092.
BOOTSTRAP = os.environ.get("REDPANDA_BOOTSTRAP") or f"localhost:{os.environ.get('REDPANDA_KAFKA_PORT', '63010')}"
TOPIC = "events"


def main(count: int = 100) -> None:
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP,
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    fmt = "%Y-%m-%dT%H:%M:%S"
    for i in range(count):
        ts = datetime.now(timezone.utc).strftime(fmt)
        producer.send(TOPIC, {"user_id": f"u{i % 10}", "event": "click", "ts": ts})
    producer.flush()
    print(f"produced {count} events to {TOPIC}")


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 100)
