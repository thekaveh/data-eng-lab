"""Static guardrails for the Atlas consumer boundary.

The production artifacts deliberately use Docker-network names (for example,
``trino:8080``) when code runs inside Atlas.  Host-side code must instead use
an explicit override or a port resolved from Atlas configuration; it must not
smuggle a fixed host endpoint into a scenario, launcher, or CI helper.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from collections.abc import Iterable
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
ALLOWED_ATLAS_HOST_EXPORTS = {"ATLAS_MINIO_HOST_ENDPOINT"}
FIXED_HOST_ENDPOINT = re.compile(
    r"(?<![\w.-])(?:https?://)?(?:localhost|127\.0\.0\.1|host\.docker\.internal|0\.0\.0\.0):\d+(?![\w.-])"
)


def _artifacts() -> list[Path]:
    """Return executable and configuration artifacts that consume Atlas.

    Documentation, generated diagrams, historical plans, and tests are not
    runtime consumers.  Resolver/config modules are included: their dynamic
    ``localhost:${PORT}`` fallbacks are intentional, while fixed ports remain
    prohibited by the same check.
    """
    patterns = (
        "atlas.consumer.yml",
        "compose/**/*.yml",
        "scripts/**/*.py",
        "scripts/**/*.sh",
        "jenkins/**/*",
        "lakehouse/**/*.py",
        "datasets/**/*.py",
        "scenarios/**/dag.py",
        "scenarios/**/producer.py",
        "scenarios/**/notebook.zpln",
        "scenarios/**/notebook.ipynb",
        "spark-apps/**/dag.py",
        "spark-apps/**/Jenkinsfile",
        "Makefile",
    )
    return sorted({path for pattern in patterns for path in ROOT.glob(pattern) if path.is_file()})


def _runtime_text(path: Path) -> str:
    """Discard whole-line comments so explanatory docs do not become endpoints."""
    return "\n".join(
        line for line in path.read_text(encoding="utf-8").splitlines() if not line.lstrip().startswith("#")
    )


def _fixed_host_endpoint_offenders(paths: Iterable[Path], *, root: Path = ROOT) -> list[str]:
    return [path.relative_to(root).as_posix() for path in paths if FIXED_HOST_ENDPOINT.search(_runtime_text(path))]


def _unsupported_atlas_export_offenders(paths: Iterable[Path], *, root: Path = ROOT) -> list[str]:
    return [
        f"{path.relative_to(root).as_posix()}: {match.group(0)}"
        for path in paths
        for match in re.finditer(r"\bATLAS_[A-Z0-9_]+_HOST_ENDPOINT\b", _runtime_text(path))
        if match.group(0) not in ALLOWED_ATLAS_HOST_EXPORTS
    ]


def test_catalog_has_expected_atlas_artifacts():
    assert len(sorted((ROOT / "scenarios").rglob("dag.py"))) == 0
    assert len(sorted((ROOT / "spark-apps").rglob("dag.py"))) == 2
    assert len(sorted((ROOT / "scenarios").rglob("notebook.zpln"))) == 19
    assert len(sorted((ROOT / "scenarios").rglob("notebook.ipynb"))) == 19


def test_material_executable_and_config_artifacts_do_not_fix_host_endpoints():
    assert not _fixed_host_endpoint_offenders(_artifacts())


def test_material_artifacts_use_only_the_supported_atlas_host_export():
    assert not _unsupported_atlas_export_offenders(_artifacts())


def test_fixed_host_endpoint_guard_catches_supported_host_forms(tmp_path: Path):
    sample = tmp_path / "producer.py"
    for host in ("localhost", "127.0.0.1", "host.docker.internal", "0.0.0.0"):
        sample.write_text(f'endpoint = "http://{host}:8080"\n', encoding="utf-8")
        assert FIXED_HOST_ENDPOINT.search(_runtime_text(sample)), host


def test_atlas_export_guard_rejects_unsupported_exports(tmp_path: Path):
    sample = tmp_path / "launcher.sh"
    sample.write_text("echo $ATLAS_TRINO_HOST_ENDPOINT\n", encoding="utf-8")
    assert _unsupported_atlas_export_offenders([sample], root=tmp_path) == ["launcher.sh: ATLAS_TRINO_HOST_ENDPOINT"]


def test_current_docs_do_not_describe_atlas_791_as_pending():
    docs = [
        ROOT / "docs" / "atlas-feedback-go-live.md",
        ROOT / "docs" / "go-live.md",
    ]
    stale = [
        path.relative_to(ROOT).as_posix()
        for path in docs
        if "awaiting the upstream compose change" in path.read_text(encoding="utf-8")
        or "DAG execution is currently blocked upstream (atlas#791)" in path.read_text(encoding="utf-8")
    ]
    assert not stale


def test_current_docs_record_the_closed_atlas_850_fix_and_live_acceptance():
    text = (ROOT / "docs" / "atlas-feedback-go-live.md").read_text(encoding="utf-8")
    assert "#850](https://github.com/thekaveh/atlas/issues/850) is closed" in text
    assert "AIRFLOW__API_AUTH__JWT_SECRET" in text
    assert "acceptance gate for this pin" in text
    assert "`FINISHED` with `success=true`" in text
    assert "#791's in-network DNS repair is validated" in text


def test_dataset_resolver_is_consumer_owned_internal_and_health_wired():
    overlay = yaml.safe_load((ROOT / "compose" / "data-eng-lab.yml").read_text(encoding="utf-8"))
    services = overlay["services"]
    resolver = services["dataset-resolver"]
    assert resolver["build"] == {
        "context": "..",
        "dockerfile": "datasets/resolver.Dockerfile",
    }
    assert resolver["platform"] == "linux/amd64"
    assert resolver["environment"]["MINIO_ENDPOINT"] == "http://minio:9000"
    assert resolver["networks"] == ["backend-network"]
    assert "ports" not in resolver and "expose" not in resolver
    assert resolver["depends_on"]["minio"]["condition"] == "service_healthy"
    for name in ("airflow-scheduler", "jupyterhub", "zeppelin"):
        service = services[name]
        assert service["environment"]["DATASET_RESOLVER_URI"] == "http://dataset-resolver:8080"
        assert service["environment"]["DATASET_SCALE"] == "${DATASET_SCALE:-small}"
        assert service["depends_on"]["dataset-resolver"]["condition"] == "service_healthy"


def _assembled_compose(scale: str | None = None) -> dict:
    environment = os.environ.copy()
    environment.update(
        MINIO_ROOT_USER="ci-placeholder-user",
        MINIO_ROOT_PASSWORD="ci-placeholder-password",
    )
    if scale is None:
        environment.pop("DATASET_SCALE", None)
    else:
        environment["DATASET_SCALE"] = scale
    completed = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            str(ROOT / "infra" / ".env.example"),
            "-f",
            str(ROOT / "infra" / "docker-compose.yml"),
            "-f",
            str(ROOT / "compose" / "data-eng-lab.yml"),
            "config",
            "--format",
            "json",
        ],
        cwd=ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


@pytest.mark.parametrize(("environment_scale", "expected"), [(None, "small"), ("medium", "medium")])
def test_assembled_resolver_network_overlap_and_scale_precedence(environment_scale, expected):
    services = _assembled_compose(environment_scale)["services"]
    participants = ("minio", "dataset-resolver", "airflow-scheduler", "jupyterhub", "zeppelin")
    assert all("backend-network" in services[name]["networks"] for name in participants)
    resolver = services["dataset-resolver"]
    assert "default" not in resolver["networks"] and "ports" not in resolver and "expose" not in resolver
    for name in ("airflow-scheduler", "jupyterhub", "zeppelin"):
        assert services[name]["environment"]["DATASET_RESOLVER_URI"] == "http://dataset-resolver:8080"
        assert services[name]["environment"]["DATASET_SCALE"] == expected


def test_consumer_manifest_declares_runtime_scale_only_in_overlay():
    manifest = yaml.safe_load((ROOT / "atlas.consumer.yml").read_text(encoding="utf-8"))
    values = manifest["env"]["values"]
    nonnumeric_scale_values = {
        key: value for key, value in values.items() if key.endswith("_SCALE") and not str(value).strip().isdigit()
    }
    assert not nonnumeric_scale_values
    assert "DATASET_SCALE" not in values
    assert manifest["compose_overlays"] == ["./compose/data-eng-lab.yml"]
    assert "dataset-resolver" not in manifest


def test_consumer_manifest_env_passes_pinned_atlas_scale_validation(tmp_path: Path):
    script = r"""
import sys
from pathlib import Path

root = Path(sys.argv[1])
sys.path.insert(0, str(root / "infra" / "bootstrapper"))
from core.config_parser import ConfigParser
from core.consumer_manifest import load_consumer_config
from services.source_validator import SourceValidator

config = load_consumer_config(
    root / "infra",
    explicit_paths=[str(root / "atlas.consumer.yml")],
)
env_file = Path(sys.argv[2])
env_file.write_text(
    "".join(f"{key}={value}\n" for key, value in config.env_overrides.items()),
    encoding="utf-8",
)
parser = ConfigParser(str(root / "infra"))
parser.env_file_path = env_file
validator = SourceValidator(config_parser=parser)
if not validator.validate_scale_values():
    raise SystemExit("\n".join(validator.get_validation_errors()))
"""
    completed = subprocess.run(
        [sys.executable, "-c", script, str(ROOT), str(tmp_path / ".env")],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


def test_airflow_dag_import_performs_no_resolver_or_s3_access():
    guard = r"""
import importlib.util, socket, sys, types
import boto3, requests, urllib.request, urllib3
def denied(*args, **kwargs):
    raise AssertionError("DAG import attempted resolver, DNS, HTTP, or S3 access")
class DeniedSocket:
    def __init__(self, *args, **kwargs): denied()
socket.socket = DeniedSocket
socket.getaddrinfo = denied
socket.create_connection = denied
for helper in ("gethostbyname", "gethostbyname_ex", "gethostbyaddr", "getnameinfo"):
    if hasattr(socket, helper): setattr(socket, helper, denied)
boto3.client = denied
boto3.resource = denied
boto3.Session = denied
boto3.session.Session = denied
requests.request = denied
requests.get = denied
requests.post = denied
urllib.request.urlopen = denied
urllib3.request = denied
urllib3.PoolManager.request = denied
class FakeOperator:
    def __init__(self, *args, **kwargs): pass
class FakeDag(FakeOperator):
    def __enter__(self): return self
    def __exit__(self, *args): return False
airflow=types.ModuleType("airflow"); airflow.DAG=FakeDag
empty=types.ModuleType("airflow.operators.empty"); empty.EmptyOperator=FakeOperator
spark_submit=types.ModuleType("airflow.providers.apache.spark.operators.spark_submit")
spark_submit.SparkSubmitOperator=FakeOperator
pendulum=types.ModuleType("pendulum"); pendulum.datetime=lambda *args, **kwargs: object()
atlas=types.ModuleType("atlas_spark_utils"); atlas.RestConfirmingSparkHook=FakeOperator
resolver=types.ModuleType("datasets.resolver_service"); resolver.resolve_request=denied
host_cli=types.ModuleType("scripts.resolve_dataset"); host_cli.run=denied
for name, module in {
 "airflow":airflow, "airflow.operators":types.ModuleType("airflow.operators"), "airflow.operators.empty":empty,
 "airflow.providers":types.ModuleType("airflow.providers"),
 "airflow.providers.apache":types.ModuleType("airflow.providers.apache"),
 "airflow.providers.apache.spark":types.ModuleType("airflow.providers.apache.spark"),
 "airflow.providers.apache.spark.operators":types.ModuleType("airflow.providers.apache.spark.operators"),
 "airflow.providers.apache.spark.operators.spark_submit":spark_submit, "pendulum":pendulum,
 "atlas_spark_utils":atlas, "datasets.resolver_service":resolver, "scripts.resolve_dataset":host_cli,
}.items(): sys.modules[name]=module
path=sys.argv[1]
spec=importlib.util.spec_from_file_location("isolated_dag", path)
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
"""
    paths = sorted((ROOT / "scenarios").rglob("dag.py")) + sorted((ROOT / "spark-apps").rglob("dag.py"))
    for path in paths:
        completed = subprocess.run([sys.executable, "-c", guard, str(path)], cwd=ROOT, capture_output=True, text=True)
        assert completed.returncode == 0, f"{path.relative_to(ROOT)}: {completed.stderr}"


@pytest.mark.parametrize("helper", ["getaddrinfo", "gethostbyname", "gethostbyname_ex", "gethostbyaddr", "getnameinfo"])
def test_isolated_import_dns_negative_control_denies_every_helper(helper):
    guard = r"""
import socket, sys
def denied(*args, **kwargs): raise AssertionError("DNS denied")
for helper in ("getaddrinfo", "gethostbyname", "gethostbyname_ex", "gethostbyaddr", "getnameinfo"):
    if hasattr(socket, helper): setattr(socket, helper, denied)
getattr(socket, sys.argv[1])("example.invalid")
"""
    completed = subprocess.run([sys.executable, "-c", guard, helper], cwd=ROOT, capture_output=True, text=True)
    assert completed.returncode != 0
    assert "DNS denied" in completed.stderr


def test_pin_bump_runbook_describes_automatic_target_rebuild():
    text = (ROOT / "docs" / "atlas-pin-bump-runbook.md").read_text(encoding="utf-8")
    assert ".atlas-build-state" in text
    assert "automatically" in text
    assert "atlas#506, open" not in text
