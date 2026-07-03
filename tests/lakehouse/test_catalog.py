from pathlib import Path

import pytest

from lakehouse import catalog


def _write_env(tmp_path: Path, **kv) -> Path:
    infra = tmp_path / "infra"
    infra.mkdir()
    (infra / ".env").write_text("".join(f"{k}={v}\n" for k, v in kv.items()))
    return infra


def test_catalog_config_from_env(tmp_path: Path):
    infra = _write_env(tmp_path, ICEBERG_REST_PORT="64110", MINIO_PORT="64093",
                       MINIO_ROOT_USER="minioadmin", MINIO_ROOT_PASSWORD="secret")
    cfg = catalog._catalog_config(infra)
    assert cfg["uri"] == "http://localhost:64110"
    assert cfg["s3.endpoint"] == "http://localhost:64093"
    assert cfg["s3.access-key-id"] == "minioadmin"
    assert cfg["s3.path-style-access"] == "true"


def test_catalog_config_missing_port_raises(tmp_path: Path):
    infra = _write_env(tmp_path, MINIO_PORT="64093")
    with pytest.raises(RuntimeError):
        catalog._catalog_config(infra)


class _FakeCatalog:
    def __init__(self, existing):
        self._ns = [(n,) for n in existing]
        self.created: list[str] = []

    def list_namespaces(self):
        return list(self._ns)

    def create_namespace(self, name):
        self._ns.append((name,))
        self.created.append(name)


def test_ensure_namespaces_creates_missing():
    fake = _FakeCatalog(["bronze"])
    created = catalog.ensure_namespaces(fake, ["bronze", "silver", "gold"])
    assert created == ["silver", "gold"]
    assert fake.created == ["silver", "gold"]


def test_ensure_namespaces_idempotent():
    fake = _FakeCatalog(["bronze", "silver", "gold"])
    assert catalog.ensure_namespaces(fake, ["bronze", "silver", "gold"]) == []
