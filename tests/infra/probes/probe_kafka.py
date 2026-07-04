"""Probe: Spark container can reach the Redpanda broker (redpanda:9092) + Kafka jar present.

Importable-by-package: call ``probe(exec_fn)`` to get ``(ok: bool, message: str)``.
As a script (``__main__``), runs inside a Spark container to perform checks directly.

Checks:
  1. TCP connectivity to redpanda:9092 (in-network from spark container)
  2. spark-sql-kafka-0-10_2.13 jar baked into /opt/spark/jars
"""
from __future__ import annotations

import sys

_BROKER_HOST = "redpanda"
_BROKER_PORT = 9092
_JAR_GLOB = "/opt/spark/jars/spark-sql-kafka-0-10_2.13-*.jar"


def probe(exec_fn):
    """Layer-2 probe: exec into spark-master to check Redpanda reachability + Kafka jar.

    Returns (ok: bool, message: str).
    """
    rc, out = exec_fn("spark-master", ["python", "/opt/probes/probe_kafka.py"])
    tail = out.strip().splitlines()[-1] if out.strip() else f"rc={rc}"
    return rc == 0, tail


if __name__ == "__main__":
    # When run as a script inside a Spark container.
    import glob
    import socket

    try:
        _sock = socket.create_connection((_BROKER_HOST, _BROKER_PORT), timeout=10)
        _sock.close()
        _tcp_ok, _tcp_msg = True, f"TCP {_BROKER_HOST}:{_BROKER_PORT} reachable"
    except OSError as _exc:
        _tcp_ok, _tcp_msg = False, f"TCP {_BROKER_HOST}:{_BROKER_PORT} unreachable: {_exc}"

    _jars = glob.glob(_JAR_GLOB)
    _jar_ok = bool(_jars)
    _jar_msg = (
        f"kafka jar: {_jars[0].split('/')[-1]}"
        if _jars
        else f"kafka jar missing (pattern: {_JAR_GLOB})"
    )

    if _tcp_ok and _jar_ok:
        print(f"spark->redpanda OK: {_tcp_msg}; {_jar_msg}")
        sys.exit(0)
    else:
        _fails = "; ".join(m for _ok, m in [(_tcp_ok, _tcp_msg), (_jar_ok, _jar_msg)] if not _ok)
        print(f"spark->redpanda FAIL: {_fails}", file=sys.stderr)
        sys.exit(1)
