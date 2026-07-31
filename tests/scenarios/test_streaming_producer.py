import builtins
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PRODUCER = ROOT / "scenarios" / "streaming_ingest-events-spark-iceberg" / "producer.py"


def _load():
    spec = importlib.util.spec_from_file_location("event_producer", PRODUCER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_bootstrap_uses_explicit_override(monkeypatch):
    monkeypatch.setenv("REDPANDA_BOOTSTRAP", "broker.example.test:19092")
    assert _load()._resolve_bootstrap() == "broker.example.test:19092"


def test_configuration_import_does_not_require_kafka(monkeypatch):
    """Offline configuration checks must not import the live producer dependency."""
    original_import = builtins.__import__

    def import_without_kafka(name, *args, **kwargs):
        if name == "kafka":
            raise ModuleNotFoundError("No module named 'kafka'")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_without_kafka)
    assert _load().TOPIC == "events"
