"""Iceberg REST catalog helpers for the medallion lakehouse (host-side access)."""
from __future__ import annotations

from pathlib import Path


def _envval(key: str, env_file: Path) -> str:
    if not env_file.exists():
        return ""
    val = ""
    for line in env_file.read_text(encoding="utf-8").splitlines():
        if line.startswith(f"{key}="):
            val = line.split("=", 1)[1].strip()  # last wins
    return val


def _catalog_config(infra_dir: Path) -> dict:
    env_file = Path(infra_dir) / ".env"
    port = _envval("ICEBERG_REST_PORT", env_file)
    minio_port = _envval("MINIO_PORT", env_file)
    user = _envval("MINIO_ROOT_USER", env_file)
    password = _envval("MINIO_ROOT_PASSWORD", env_file)
    if not (port and minio_port and user and password):
        raise RuntimeError(
            f"Iceberg REST / MinIO settings missing in {env_file} — start the enhanced stack first."
        )
    return {
        "uri": f"http://localhost:{port}",
        "warehouse": "s3a://lakehouse/",
        "s3.endpoint": f"http://localhost:{minio_port}",
        "s3.access-key-id": user,
        "s3.secret-access-key": password,
        "s3.path-style-access": "true",
    }


def rest_catalog_from_env(infra_dir: Path):
    from pyiceberg.catalog.rest import RestCatalog

    cfg = _catalog_config(infra_dir)
    return RestCatalog(name="lakehouse", **cfg)


def ensure_namespaces(cat, names: list[str]) -> list[str]:
    """Create any of `names` not already present. Idempotent; returns those created."""
    existing = {ns[0] if isinstance(ns, tuple) else ns for ns in cat.list_namespaces()}
    created: list[str] = []
    for name in names:
        if name not in existing:
            cat.create_namespace(name)
            created.append(name)
    return created
