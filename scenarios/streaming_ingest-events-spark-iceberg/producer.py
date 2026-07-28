"""Produce synthetic events to the Redpanda `events` topic (for the streaming scenario).
Live-gated: requires Atlas Redpanda (issue #269). Run: python producer.py [count]."""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer  # kafka-python

ROOT = Path(__file__).resolve().parents[2]
INFRA_ENV = ROOT / "infra" / ".env"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lakehouse.atlas_endpoints import resolve_http_endpoint  # noqa: E402


def _resolve_bootstrap() -> str:
    explicit = os.environ.get("REDPANDA_BOOTSTRAP", "").strip()
    if explicit:
        return explicit
    endpoint = resolve_http_endpoint(
        "REDPANDA_HOST_ENDPOINT", "REDPANDA_KAFKA_PORT", env_file=INFRA_ENV
    )
    return endpoint.removeprefix("http://").removeprefix("https://")


TOPIC = "events"


def main(count: int = 100) -> None:
    producer = KafkaProducer(bootstrap_servers=_resolve_bootstrap(),
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    fmt = "%Y-%m-%dT%H:%M:%S"
    for i in range(count):
        ts = datetime.now(timezone.utc).strftime(fmt)
        producer.send(TOPIC, {"user_id": f"u{i % 10}", "event": "click", "ts": ts})
    producer.flush()
    print(f"produced {count} events to {TOPIC}")


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 100)
