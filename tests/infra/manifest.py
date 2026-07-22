"""Declarative expected-service manifest for the data-eng track (Phase 0: Layer 1)."""
from __future__ import annotations

import os
import shutil
import subprocess
from collections import namedtuple
from pathlib import Path

ServiceSpec = namedtuple("ServiceSpec", "name enabled init_check")


def _resolve_project() -> str:
    """PROJECT_NAME precedence: env var > infra/.env > default (mirrors the shell harness)."""
    env_val = os.environ.get("PROJECT_NAME")
    if env_val:
        return env_val
    env_file = Path(__file__).resolve().parents[2] / "infra" / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            if line.startswith("PROJECT_NAME="):
                val = line.split("=", 1)[1].strip()
                if val:
                    return val
    return "data-eng-lab"


PROJECT = _resolve_project()

_INFRA_ENV = Path(__file__).resolve().parents[2] / "infra" / ".env"


def _truthy(var: str) -> bool:
    return os.environ.get(var, "").lower() in ("1", "true", "yes", "on")


def _env_source(key: str, env_file: Path = _INFRA_ENV) -> str:
    """Resolve an Atlas ``*_SOURCE`` value: env var > infra/.env (last wins) > ''."""
    val = os.environ.get(key)
    if val is not None:
        return val.strip()
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            if line.startswith(f"{key}="):
                val = line.split("=", 1)[1].strip()  # last KEY= line wins
    return (val or "").strip()


def _source_enabled(key: str, env_file: Path = _INFRA_ENV) -> bool:
    """Is the service whose Atlas source is ``key`` expected to be running?

    Enabled unless the source is explicitly ``disabled`` — the consumer manifest
    (``atlas.consumer.yml``) sets ``*_SOURCE=container`` and Atlas materializes it
    into ``infra/.env``. An absent source defaults to enabled so a live service is
    never silently skipped (a genuinely-down one then surfaces as fail/blocked).
    """
    return _env_source(key, env_file).lower() != "disabled"


def _container_status(service: str) -> str:
    if not shutil.which("docker"):
        return ""
    try:
        out = subprocess.run(
            ["docker", "ps",
             "--filter", f"label=com.docker.compose.project={PROJECT}",
             "--filter", f"label=com.docker.compose.service={service}",
             "--format", "{{.Status}}"],
            capture_output=True, text=True, timeout=5,
        )
    except subprocess.TimeoutExpired:
        return "<docker-timeout>"
    if out.returncode != 0:
        return "<docker-error>"
    return out.stdout.strip().splitlines()[0] if out.stdout.strip() else ""


def daemon_ok() -> bool:
    """True only if the Docker daemon is reachable (not merely the binary present)."""
    if not shutil.which("docker"):
        return False
    try:
        out = subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=5)
    except subprocess.TimeoutExpired:
        return False
    return out.returncode == 0


def _up(service: str):
    status = _container_status(service)
    ok = ("healthy" in status) or status.startswith("Up")
    return ok, status or "not running"


def _minio_ready():
    ok, detail = _up("minio")
    # Deeper bucket check lands in Phase 1 (needs mc/boto3 against the live endpoint).
    return ok, detail


# Base data-eng services always expected; optional services gated on their Atlas
# `*_SOURCE` (set to `container` by atlas.consumer.yml) — enabled unless `disabled`.
EXPECTED_SERVICES = [
    ServiceSpec("minio", True, _minio_ready),
    ServiceSpec("spark-master", True, lambda: _up("spark-master")),
    ServiceSpec("spark-connect", True, lambda: _up("spark-connect")),
    ServiceSpec("zeppelin", True, lambda: _up("zeppelin")),
    ServiceSpec("jupyterhub", True, lambda: _up("jupyterhub")),
    ServiceSpec("airflow-webserver", True, lambda: _up("airflow-webserver")),
    ServiceSpec("airflow-scheduler", True, lambda: _up("airflow-scheduler")),
    ServiceSpec("weaviate", True, lambda: _up("weaviate")),
    ServiceSpec("neo4j-graph-db", True, lambda: _up("neo4j-graph-db")),
    ServiceSpec("iceberg-rest", _source_enabled("ICEBERG_REST_SOURCE"), lambda: _up("iceberg-rest")),
    ServiceSpec("jenkins", _source_enabled("JENKINS_SOURCE"), lambda: _up("jenkins")),
    ServiceSpec("redpanda", _source_enabled("REDPANDA_SOURCE"), lambda: _up("redpanda")),
    ServiceSpec("trino", _source_enabled("TRINO_SOURCE"), lambda: _up("trino")),
]
