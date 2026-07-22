"""The committed Atlas consumer manifest is the single source of truth for how
data-eng-lab consumes Atlas (replaces the legacy _user/ symlink + .env upserts —
see docs/atlas-enablement.md)."""
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "atlas.consumer.yml"

SOURCES = ["SPARK", "ZEPPELIN", "AIRFLOW", "MINIO", "JUPYTERHUB",
           "ICEBERG_REST", "JENKINS", "TRINO", "REDPANDA"]


def _load() -> dict:
    return yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))


def test_manifest_exists_and_parses():
    assert MANIFEST.is_file(), "atlas.consumer.yml missing at repo root"
    assert isinstance(_load(), dict)


def test_manifest_identity_and_brand():
    data = _load()
    assert data["name"] == "data-eng-lab"
    assert data["project_name"] == "data-eng-lab"
    assert data["profile"] == "dev"
    assert data["brand"]["name"] == "data-eng-lab"


def test_manifest_env_values():
    env = _load()["env"]["values"]
    assert env["BASE_PORT"] == "auto"
    assert env["ICEBERG_REST_ENABLED"] == "true"
    assert env["ICEBERG_REST_URI"] == "http://iceberg-rest:8181"
    for svc in SOURCES:
        assert env[f"{svc}_SOURCE"] == "container", f"{svc}_SOURCE missing/wrong"


def test_manifest_uses_host_ollama():
    """This dev machine uses the host ollama (localhost:11434); the containerized
    CPU ollama is slow and pulls redundant models into disk (issue #58)."""
    env = _load()["env"]["values"]
    assert env["LLM_PROVIDER_SOURCE"] == "ollama-localhost"


def test_manifest_overlay_paths_exist():
    for rel in _load()["compose_overlays"]:
        assert (ROOT / rel).is_file(), f"overlay path {rel} does not exist"


def test_manifest_declares_test_bucket():
    buckets = _load()["storage"]["buckets"]
    assert any(b.get("bucket") == "lakehouse-test" for b in buckets)
