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
    assert cfg["warehouse"] == "s3a://lakehouse/"
    assert cfg["s3.secret-access-key"] == "secret"


def test_catalog_config_sets_default_region(tmp_path: Path, monkeypatch):
    """pyiceberg's pyarrow FileIO needs an explicit s3.region against MinIO, or the
    region-probe HeadObject can 400 (issue #51). Defaults to us-east-1 (MinIO's
    default), matching every other S3 client in the repo."""
    monkeypatch.delenv("MINIO_REGION", raising=False)
    infra = _write_env(tmp_path, ICEBERG_REST_PORT="64110", MINIO_PORT="64093",
                       MINIO_ROOT_USER="minioadmin", MINIO_ROOT_PASSWORD="secret")
    cfg = catalog._catalog_config(infra)
    assert cfg["s3.region"] == "us-east-1"


def test_catalog_config_region_from_env_file(tmp_path: Path, monkeypatch):
    monkeypatch.delenv("MINIO_REGION", raising=False)
    infra = _write_env(tmp_path, ICEBERG_REST_PORT="64110", MINIO_PORT="64093",
                       MINIO_ROOT_USER="minioadmin", MINIO_ROOT_PASSWORD="secret",
                       MINIO_REGION="eu-west-2")
    cfg = catalog._catalog_config(infra)
    assert cfg["s3.region"] == "eu-west-2"


def test_catalog_config_region_env_var_overrides(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("MINIO_REGION", "ap-south-1")
    infra = _write_env(tmp_path, ICEBERG_REST_PORT="64110", MINIO_PORT="64093",
                       MINIO_ROOT_USER="minioadmin", MINIO_ROOT_PASSWORD="secret",
                       MINIO_REGION="eu-west-2")
    cfg = catalog._catalog_config(infra)
    assert cfg["s3.region"] == "ap-south-1"


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
