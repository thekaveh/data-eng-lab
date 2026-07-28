"""Static guardrails for the Atlas consumer boundary.

The production artifacts deliberately use Docker-network names (for example,
``trino:8080``) when code runs inside Atlas.  Host-side code must instead use
an explicit override or a port resolved from Atlas configuration; it must not
smuggle a fixed host endpoint into a scenario, launcher, or CI helper.
"""
from __future__ import annotations

import re
from collections.abc import Iterable
from pathlib import Path

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
    return sorted(
        {path for pattern in patterns for path in ROOT.glob(pattern) if path.is_file()}
    )


def _runtime_text(path: Path) -> str:
    """Discard whole-line comments so explanatory docs do not become endpoints."""
    return "\n".join(
        line for line in path.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("#")
    )


def _fixed_host_endpoint_offenders(paths: Iterable[Path], *, root: Path = ROOT) -> list[str]:
    return [
        path.relative_to(root).as_posix()
        for path in paths
        if FIXED_HOST_ENDPOINT.search(_runtime_text(path))
    ]


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
    assert _unsupported_atlas_export_offenders([sample], root=tmp_path) == [
        "launcher.sh: ATLAS_TRINO_HOST_ENDPOINT"
    ]


def test_current_docs_do_not_describe_atlas_791_as_pending():
    docs = [
        ROOT / "docs" / "atlas-feedback-go-live.md",
        ROOT / "docs" / "go-live.md",
    ]
    stale = [
        path.relative_to(ROOT).as_posix()
        for path in docs
        if "awaiting the upstream compose change" in path.read_text(encoding="utf-8")
        or "DAG execution is currently blocked upstream (atlas#791)"
        in path.read_text(encoding="utf-8")
    ]
    assert not stale


def test_current_docs_record_atlas_850_as_fixed_with_live_retest_pending():
    text = (ROOT / "docs" / "atlas-feedback-go-live.md").read_text(encoding="utf-8")
    assert "Atlas #850 is closed" in text
    assert "focused `nyc_taxi_etl` proof before promotion" in text
    assert "#850 tracks" not in text


def test_pin_bump_runbook_describes_automatic_target_rebuild():
    text = (ROOT / "docs" / "atlas-pin-bump-runbook.md").read_text(encoding="utf-8")
    assert ".atlas-build-state" in text
    assert "automatically" in text
    assert "atlas#506, open" not in text
