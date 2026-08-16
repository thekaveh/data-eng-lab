from __future__ import annotations

from pathlib import Path

import pytest

from scripts.docs.manifest import iter_leaf_sections, load_manifest
from scripts.release.contract import ReleaseContractFailure, ReleaseState, validate_repository

ROOT = Path(__file__).resolve().parents[2]


def test_repository_release_state_is_exact_and_intentionally_unreleased() -> None:
    assert validate_repository(ROOT) == ReleaseState(
        version="0.1.0",
        status="intentionally_unreleased",
        changelog="docs/CHANGELOG.md",
    )


def test_release_policy_defines_every_authority_and_transaction_boundary() -> None:
    text = (ROOT / "docs" / "release-policy.md").read_text(encoding="utf-8")
    assert text.startswith("# 9.2. Release Policy\n")
    phrases = (
        "0.1.0 (unreleased)",
        "`pyproject.toml`",
        "`docs/CHANGELOG.md`",
        "Semantic Versioning 2.0.0",
        "`v<version>`",
        "annotated tag",
        "verified `main` commit",
        "explicit owner authorization",
        "release notes",
        "immutable",
        "Maven",
        "No tag or GitHub Release",
    )
    for phrase in phrases:
        assert phrase in text
    assert "metadata means released" not in text


def test_readme_publishes_current_release_state_and_canonical_links() -> None:
    text = (ROOT / "README.md").read_text(encoding="utf-8")
    section = text.split("## 5. Release state", 1)[1]
    assert "0.1.0 (unreleased)" in section
    assert "[Release policy](docs/release-policy.md)" in section
    assert "[canonical changelog](docs/CHANGELOG.md)" in section
    assert "package metadata does not mean" in section


def test_manifest_projects_release_policy_as_repository_operations_9_2() -> None:
    manifest = load_manifest(ROOT / "docs" / "manifest.yaml", ROOT)
    leaves = {leaf.id: leaf for leaf in iter_leaf_sections(manifest.sections)}

    release = leaves["release-policy"]
    assert (release.number, release.title, release.source.as_posix()) == (
        "9.2",
        "Release Policy",
        "docs/release-policy.md",
    )
    changelog = leaves["changelog"]
    assert (changelog.number, changelog.source.as_posix()) == ("10", "docs/CHANGELOG.md")


def test_repository_validator_rejects_contradictory_policy_state(tmp_path: Path) -> None:
    for relative in (
        "pyproject.toml",
        "CHANGELOG.md",
        "README.md",
        "docs/CHANGELOG.md",
        "docs/release-policy.md",
        "docs/manifest.yaml",
    ):
        source = ROOT / relative
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source.read_bytes())
    policy = tmp_path / "docs" / "release-policy.md"
    policy.write_text(
        policy.read_text(encoding="utf-8").replace("0.1.0 (unreleased)", "0.1.0 (released)"), encoding="utf-8"
    )

    with pytest.raises(ReleaseContractFailure, match="^release_documentation_invalid$"):
        validate_repository(tmp_path)


def _copy_release_surface(root: Path) -> None:
    for relative in (
        "pyproject.toml",
        "CHANGELOG.md",
        "README.md",
        "docs/CHANGELOG.md",
        "docs/release-policy.md",
        "docs/manifest.yaml",
    ):
        source = ROOT / relative
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source.read_bytes())


def test_repository_validator_requires_manifest_even_without_workflow_directory(tmp_path: Path) -> None:
    _copy_release_surface(tmp_path)
    (tmp_path / "docs" / "manifest.yaml").unlink()

    with pytest.raises(ReleaseContractFailure, match="^release_documentation_invalid$"):
        validate_repository(tmp_path)


def test_repository_validator_rejects_duplicate_manifest_keys(tmp_path: Path) -> None:
    _copy_release_surface(tmp_path)
    manifest = tmp_path / "docs" / "manifest.yaml"
    manifest.write_text(manifest.read_text(encoding="utf-8") + "\nsurfaces: [repo, site, wiki]\n", encoding="utf-8")

    with pytest.raises(ReleaseContractFailure, match="^release_documentation_invalid$"):
        validate_repository(tmp_path)


def test_repository_validator_rejects_deep_manifest_without_traceback(tmp_path: Path) -> None:
    _copy_release_surface(tmp_path)
    manifest = tmp_path / "docs" / "manifest.yaml"
    nested = "value"
    for _ in range(40):
        nested = f"[{nested}]"
    manifest.write_text(f"surfaces: {nested}\n", encoding="utf-8")

    with pytest.raises(ReleaseContractFailure, match="^release_documentation_invalid$"):
        validate_repository(tmp_path)


def test_repository_validator_rejects_external_manifest_symlink(tmp_path: Path) -> None:
    _copy_release_surface(tmp_path)
    external = tmp_path.parent / f"{tmp_path.name}-external-manifest.yaml"
    external.write_bytes((ROOT / "docs" / "manifest.yaml").read_bytes())
    manifest = tmp_path / "docs" / "manifest.yaml"
    manifest.unlink()
    manifest.symlink_to(external)

    with pytest.raises(ReleaseContractFailure, match="^release_documentation_invalid$"):
        validate_repository(tmp_path)


def test_repository_validator_requires_complete_three_surface_manifest(tmp_path: Path) -> None:
    _copy_release_surface(tmp_path)
    (tmp_path / "docs" / "manifest.yaml").write_text(
        """sections:
  - {id: release-policy, number: '9.2', title: Release Policy, source: docs/release-policy.md}
  - {id: changelog, number: '10', title: Changelog, source: docs/CHANGELOG.md}
""",
        encoding="utf-8",
    )

    with pytest.raises(ReleaseContractFailure, match="^release_documentation_invalid$"):
        validate_repository(tmp_path)
