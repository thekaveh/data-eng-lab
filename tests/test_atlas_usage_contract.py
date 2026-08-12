"""Static guardrails for the Atlas consumer boundary.

The production artifacts deliberately use Docker-network names (for example,
``trino:8080``) when code runs inside Atlas.  Host-side code must instead use
an explicit override or a port resolved from Atlas configuration; it must not
smuggle a fixed host endpoint into a scenario, launcher, or CI helper.
"""

from __future__ import annotations

import importlib.util
import re
import socket
import sys
import types
import uuid
from collections.abc import Iterable
from pathlib import Path

import boto3
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
    assert len(sorted((ROOT / "scenarios").rglob("dag.py"))) == 19
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
    assert "ports" not in resolver and "expose" not in resolver
    assert resolver["depends_on"]["minio"]["condition"] == "service_healthy"
    for name in ("airflow-scheduler", "jupyterhub", "zeppelin"):
        service = services[name]
        assert service["environment"]["DATASET_RESOLVER_URI"] == "http://dataset-resolver:8080"
        assert service["depends_on"]["dataset-resolver"]["condition"] == "service_healthy"


def test_consumer_manifest_declares_resolver_configuration_only_in_overlay():
    manifest = yaml.safe_load((ROOT / "atlas.consumer.yml").read_text(encoding="utf-8"))
    assert manifest["env"]["values"]["DATASET_SCALE"] == "small"
    assert manifest["compose_overlays"] == ["./compose/data-eng-lab.yml"]
    assert "dataset-resolver" not in manifest


def test_airflow_dag_import_performs_no_resolver_or_s3_access(monkeypatch):
    def fail_on_call(*_args, **_kwargs):
        raise AssertionError("DAG import attempted network or S3 access")

    class FakeOperator:
        def __init__(self, *args, **kwargs):
            pass

    class FakeDag(FakeOperator):
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    airflow = types.ModuleType("airflow")
    airflow.DAG = FakeDag
    airflow_operators = types.ModuleType("airflow.operators")
    airflow_empty = types.ModuleType("airflow.operators.empty")
    airflow_empty.EmptyOperator = FakeOperator
    airflow_providers = types.ModuleType("airflow.providers")
    airflow_apache = types.ModuleType("airflow.providers.apache")
    airflow_spark = types.ModuleType("airflow.providers.apache.spark")
    airflow_spark_operators = types.ModuleType("airflow.providers.apache.spark.operators")
    airflow_spark_submit = types.ModuleType("airflow.providers.apache.spark.operators.spark_submit")
    airflow_spark_submit.SparkSubmitOperator = FakeOperator
    pendulum = types.ModuleType("pendulum")
    pendulum.datetime = lambda *_args, **_kwargs: object()
    atlas_utils = types.ModuleType("atlas_spark_utils")
    atlas_utils.RestConfirmingSparkHook = FakeOperator
    for name, module in {
        "airflow": airflow,
        "airflow.operators": airflow_operators,
        "airflow.operators.empty": airflow_empty,
        "airflow.providers": airflow_providers,
        "airflow.providers.apache": airflow_apache,
        "airflow.providers.apache.spark": airflow_spark,
        "airflow.providers.apache.spark.operators": airflow_spark_operators,
        "airflow.providers.apache.spark.operators.spark_submit": airflow_spark_submit,
        "pendulum": pendulum,
        "atlas_spark_utils": atlas_utils,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.setattr(socket, "create_connection", fail_on_call)
    monkeypatch.setattr(boto3, "client", fail_on_call)

    paths = sorted((ROOT / "scenarios").rglob("dag.py")) + sorted((ROOT / "spark-apps").rglob("dag.py"))
    for path in paths:
        module_name = f"dataset_import_guard_{uuid.uuid4().hex}"
        spec = importlib.util.spec_from_file_location(module_name, path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)


def test_pin_bump_runbook_describes_automatic_target_rebuild():
    text = (ROOT / "docs" / "atlas-pin-bump-runbook.md").read_text(encoding="utf-8")
    assert ".atlas-build-state" in text
    assert "automatically" in text
    assert "atlas#506, open" not in text
