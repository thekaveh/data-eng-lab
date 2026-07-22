import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _load(mod):
    spec = importlib.util.spec_from_file_location(mod, ROOT / "tests" / "infra" / f"{mod}.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_iceberg_edges_gated_off_when_source_disabled(monkeypatch):
    monkeypatch.setenv("ICEBERG_REST_SOURCE", "disabled")
    layer2 = _load("layer2")
    names = {e.name: e.enabled for e in layer2.EDGES}
    assert names["spark->minio+iceberg"] is False
    assert names["airflow->minio+spark"] is True  # base edge always on


def test_iceberg_edges_enabled_when_source_container(monkeypatch):
    monkeypatch.setenv("ICEBERG_REST_SOURCE", "container")
    layer2 = _load("layer2")
    assert {e.name for e in layer2.EDGES if e.enabled} >= {"spark->minio+iceberg", "jupyter->pyiceberg"}


# ---------------------------------------------------------------------------
# Phase 13 — Trino (A7) + Redpanda (A9) edge tests
# ---------------------------------------------------------------------------


def test_trino_edge_present_in_edges():
    """trino->lakehouse edge is declared in EDGES with correct name."""
    layer2 = _load("layer2")
    names = [e.name for e in layer2.EDGES]
    assert "trino->lakehouse" in names


def test_redpanda_edge_present_in_edges():
    """spark->redpanda edge is declared in EDGES with correct name."""
    layer2 = _load("layer2")
    names = [e.name for e in layer2.EDGES]
    assert "spark->redpanda" in names


def test_trino_edge_gated_off_when_source_disabled(monkeypatch):
    """trino->lakehouse is disabled only when TRINO_SOURCE is genuinely 'disabled'."""
    monkeypatch.setenv("TRINO_SOURCE", "disabled")
    layer2 = _load("layer2")
    names = {e.name: e.enabled for e in layer2.EDGES}
    assert names["trino->lakehouse"] is False


def test_trino_edge_enabled_when_source_container(monkeypatch):
    """trino->lakehouse is enabled when TRINO_SOURCE=container."""
    monkeypatch.setenv("TRINO_SOURCE", "container")
    layer2 = _load("layer2")
    names = {e.name: e.enabled for e in layer2.EDGES}
    assert names["trino->lakehouse"] is True


def test_redpanda_edge_gated_off_when_source_disabled(monkeypatch):
    """spark->redpanda is disabled only when REDPANDA_SOURCE is genuinely 'disabled'."""
    monkeypatch.setenv("REDPANDA_SOURCE", "disabled")
    layer2 = _load("layer2")
    names = {e.name: e.enabled for e in layer2.EDGES}
    assert names["spark->redpanda"] is False


def test_redpanda_edge_enabled_when_source_container(monkeypatch):
    """spark->redpanda is enabled when REDPANDA_SOURCE=container."""
    monkeypatch.setenv("REDPANDA_SOURCE", "container")
    layer2 = _load("layer2")
    names = {e.name: e.enabled for e in layer2.EDGES}
    assert names["spark->redpanda"] is True


def test_trino_edge_probe_uses_exec_fn():
    """trino->lakehouse probe correctly threads exec_fn and returns (bool, str)."""
    layer2 = _load("layer2")
    edge = next(e for e in layer2.EDGES if e.name == "trino->lakehouse")
    calls = []

    def fake_exec(container, argv):
        calls.append((container, argv))
        return 0, "Schema\nlakehouse_bronze\nlakehouse_silver\n"

    ok, msg = edge.probe(fake_exec)
    assert ok is True
    assert calls, "exec_fn was never called"
    container, argv = calls[0]
    assert container == "trino"
    assert "SHOW SCHEMAS FROM lakehouse" in " ".join(argv)


def test_redpanda_edge_probe_uses_exec_fn():
    """spark->redpanda probe correctly threads exec_fn and returns (bool, str)."""
    layer2 = _load("layer2")
    edge = next(e for e in layer2.EDGES if e.name == "spark->redpanda")
    calls = []

    def fake_exec(container, argv):
        calls.append((container, argv))
        return 0, "spark->redpanda OK: TCP redpanda:9092 reachable; kafka jar: spark-sql-kafka-0-10_2.13-4.1.2.jar"

    ok, msg = edge.probe(fake_exec)
    assert ok is True
    assert calls, "exec_fn was never called"
    container, argv = calls[0]
    assert container == "spark-master"
    assert argv[0] == "python3"
    assert argv[1] == "-c"
    assert "redpanda" in argv[2] and "9092" in argv[2]


def test_trino_edge_probe_fails_on_nonzero_rc():
    """trino->lakehouse probe returns fail when exec_fn returns non-zero."""
    layer2 = _load("layer2")
    edge = next(e for e in layer2.EDGES if e.name == "trino->lakehouse")

    def fake_exec(container, argv):
        return 1, "Error: connection refused"

    ok, msg = edge.probe(fake_exec)
    assert ok is False
    assert "1" in msg or "error" in msg.lower() or "exit" in msg.lower()


def test_redpanda_edge_probe_fails_on_nonzero_rc():
    """spark->redpanda probe returns fail when exec_fn returns non-zero."""
    layer2 = _load("layer2")
    edge = next(e for e in layer2.EDGES if e.name == "spark->redpanda")

    def fake_exec(container, argv):
        return 1, "spark->redpanda FAIL: TCP redpanda:9092 unreachable"

    ok, msg = edge.probe(fake_exec)
    assert ok is False
