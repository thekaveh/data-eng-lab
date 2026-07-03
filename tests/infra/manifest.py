"""Declarative expected-service manifest for the data-eng track (Phase 0: Layer 1)."""
from __future__ import annotations

import os
import shutil
import subprocess
from collections import namedtuple

ServiceSpec = namedtuple("ServiceSpec", "name enabled init_check")

PROJECT = os.environ.get("PROJECT_NAME", "data-eng-lab")


def _truthy(var: str) -> bool:
    return os.environ.get(var, "").lower() in ("1", "true", "yes", "on")


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


def _up(service: str):
    status = _container_status(service)
    ok = ("healthy" in status) or status.startswith("Up")
    return ok, status or "not running"


def _minio_ready():
    ok, detail = _up("minio")
    # Deeper bucket check lands in Phase 1 (needs mc/boto3 against the live endpoint).
    return ok, detail


# Base data-eng services always expected; A1/A5/A9/A7 services gated until enabled.
EXPECTED_SERVICES = [
    ServiceSpec("minio", True, _minio_ready),
    ServiceSpec("spark-master", True, lambda: _up("spark-master")),
    ServiceSpec("spark-connect", True, lambda: _up("spark-connect")),
    ServiceSpec("zeppelin", True, lambda: _up("zeppelin")),
    ServiceSpec("jupyterhub", True, lambda: _up("jupyterhub")),
    ServiceSpec("airflow-webserver", True, lambda: _up("airflow-webserver")),
    ServiceSpec("airflow-scheduler", True, lambda: _up("airflow-scheduler")),
    ServiceSpec("iceberg-rest", _truthy("ICEBERG_REST_ENABLED"), lambda: _up("iceberg-rest")),
    ServiceSpec("jenkins", _truthy("JENKINS_ENABLED"), lambda: _up("jenkins")),
    ServiceSpec("redpanda", _truthy("REDPANDA_ENABLED"), lambda: _up("redpanda")),
    ServiceSpec("trino", _truthy("TRINO_ENABLED"), lambda: _up("trino")),
]
