"""Produce synthetic events to the Redpanda `events` topic (for the streaming scenario).
Live-gated: requires Atlas Redpanda (issue #269). Run: python producer.py [count]."""
from __future__ import annotations

import json
import sys
import time

from kafka import KafkaProducer  # kafka-python

BOOTSTRAP = "localhost:9092"  # host-published Redpanda; in-cluster use redpanda:9092
TOPIC = "events"


def main(count: int = 100) -> None:
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP,
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    for i in range(count):
        producer.send(TOPIC, {"user_id": f"u{i % 10}", "event": "click", "ts": time.time()})
    producer.flush()
    print(f"produced {count} events to {TOPIC}")


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 100)
