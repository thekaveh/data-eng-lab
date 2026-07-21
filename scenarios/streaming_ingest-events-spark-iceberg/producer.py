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


def _envval(key: str, env_file: Path) -> str:
    """Last-wins parse of a KEY=value line from a .env file (copied from lakehouse/catalog.py
    to keep this scenario script standalone — no imports from tests/ or lakehouse/).
    """
    if not env_file.exists():
        return ""
    val = ""
    for line in env_file.read_text(encoding="utf-8").splitlines():
        if line.startswith(f"{key}="):
            val = line.split("=", 1)[1].strip()  # last wins
    return val


def _resolve_bootstrap() -> str:
    bootstrap = os.environ.get("REDPANDA_BOOTSTRAP")
    if bootstrap:
        return bootstrap
    # REDPANDA_KAFKA_PORT: env var > infra/.env (BASE_PORT: auto means no fixed default).
    port = os.environ.get("REDPANDA_KAFKA_PORT") or _envval("REDPANDA_KAFKA_PORT", INFRA_ENV)
    if not port:
        raise RuntimeError(
            "REDPANDA_KAFKA_PORT not set in env or infra/.env — is the live Redpanda stack up?"
        )
    return f"localhost:{port}"  # in-cluster use redpanda:9092 instead


BOOTSTRAP = _resolve_bootstrap()
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
