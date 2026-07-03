import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _load(mod):
    spec = importlib.util.spec_from_file_location(mod, ROOT / "tests" / "infra" / f"{mod}.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_iceberg_edges_gated_off_when_flag_unset(monkeypatch):
    monkeypatch.delenv("ICEBERG_REST_ENABLED", raising=False)
    layer2 = _load("layer2")
    names = {e.name: e.enabled for e in layer2.EDGES}
    assert names["spark->minio+iceberg"] is False
    assert names["airflow->minio+spark"] is True  # base edge always on


def test_iceberg_edges_enabled_when_flag_set(monkeypatch):
    monkeypatch.setenv("ICEBERG_REST_ENABLED", "true")
    layer2 = _load("layer2")
    assert {e.name for e in layer2.EDGES if e.enabled} >= {"spark->minio+iceberg", "jupyter->pyiceberg"}
