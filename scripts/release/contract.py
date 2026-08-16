from __future__ import annotations

import argparse
import re
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

from scripts.docs.manifest import ManifestError, iter_leaf_sections, parse_manifest

MAX_RELEASE_FILE_BYTES = 1_048_576
EXPECTED_VERSION = "0.1.0"
CANONICAL_CHANGELOG = "docs/CHANGELOG.md"
FORBIDDEN_RELEASE_AUTOMATION = (
    "gh release create",
    "actions/create-release@",
    "softprops/action-gh-release@",
    "pypa/gh-action-pypi-publish@",
    "twine upload",
)
SEMVER = re.compile(
    r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    r"(?:-(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)"
    r"(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*)?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$"
)


class ReleaseContractFailure(ValueError):
    """The repository release policy is malformed or contradictory."""


@dataclass(frozen=True)
class ReleaseState:
    version: str
    status: str
    changelog: str


def _read_owned_text(root: Path, relative: str) -> str:
    path = root / relative
    try:
        resolved_root = root.resolve(strict=True)
        resolved = path.resolve(strict=True)
    except OSError:
        raise ReleaseContractFailure("release_file_invalid") from None
    if path.is_symlink() or not path.is_file() or not resolved.is_relative_to(resolved_root):
        raise ReleaseContractFailure("release_file_invalid")
    try:
        with path.open("rb") as stream:
            body = stream.read(MAX_RELEASE_FILE_BYTES + 1)
    except OSError:
        raise ReleaseContractFailure("release_file_invalid") from None
    if len(body) > MAX_RELEASE_FILE_BYTES:
        raise ReleaseContractFailure("release_file_too_large")
    try:
        return body.decode("utf-8")
    except UnicodeDecodeError:
        raise ReleaseContractFailure("release_file_malformed") from None


def load_project_version(root: Path) -> str:
    """Return the exact static project version from bounded owned metadata."""
    text = _read_owned_text(root, "pyproject.toml")
    try:
        document = tomllib.loads(text)
    except tomllib.TOMLDecodeError:
        raise ReleaseContractFailure("project_metadata_invalid") from None
    project = document.get("project")
    if not isinstance(project, dict) or project.get("name") != "data-eng-lab":
        raise ReleaseContractFailure("project_metadata_invalid")
    version = project.get("version")
    if type(version) is not str or SEMVER.fullmatch(version) is None or version != EXPECTED_VERSION:
        raise ReleaseContractFailure("project_version_invalid")
    return version


def _root_changelog(version: str) -> str:
    return f"""# Changelog

Project version `{version}` is intentionally unreleased. No Git tag or GitHub
Release exists for it.

The [canonical changelog]({CANONICAL_CHANGELOG}) records all unreleased changes
and is the source projected to the documentation site and wiki. The
[release policy](docs/release-policy.md) defines version selection, tags,
release notes, and the explicit authorization required to publish.
"""


def validate_changelog_state(root: Path, version: str) -> str:
    """Validate one detailed changelog and one exact repository index."""
    canonical = _read_owned_text(root, CANONICAL_CHANGELOG)
    if (
        canonical.count("## 1. [Unreleased]") != 1
        or re.search(r"^## [0-9]+\. \[Unreleased\]$", canonical, re.MULTILINE) is None
    ):
        raise ReleaseContractFailure("canonical_changelog_invalid")
    unreleased = canonical.split("## 1. [Unreleased]", 1)[1].split("\n## ", 1)[0]
    if (
        re.search(r"^### (?:Added|Changed)$", unreleased, re.MULTILINE) is None
        or re.search(r"^- \S", unreleased, re.MULTILINE) is None
    ):
        raise ReleaseContractFailure("canonical_changelog_invalid")
    if re.search(
        rf"^## [0-9]+\. \[{re.escape(version)}\] - [0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$",
        canonical,
        re.MULTILINE,
    ):
        raise ReleaseContractFailure("release_state_contradictory")
    if _read_owned_text(root, "CHANGELOG.md") != _root_changelog(version):
        raise ReleaseContractFailure("root_changelog_invalid")
    return CANONICAL_CHANGELOG


def _validate_documentation(root: Path, version: str) -> None:
    policy = _read_owned_text(root, "docs/release-policy.md")
    required_policy = (
        f"{version} (unreleased)",
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
    readme = _read_owned_text(root, "README.md")
    required_readme = (
        "## 5. Release state",
        f"{version} (unreleased)",
        "[Release policy](docs/release-policy.md)",
        "[canonical changelog](docs/CHANGELOG.md)",
        "package metadata does not mean",
    )
    if any(phrase not in policy for phrase in required_policy) or any(
        phrase not in readme for phrase in required_readme
    ):
        raise ReleaseContractFailure("release_documentation_invalid")


def validate_no_release_automation(root: Path) -> None:
    """Reject repository workflows that publish a package or GitHub Release."""
    workflow_root = root / ".github" / "workflows"
    if not workflow_root.exists():
        return
    try:
        paths = sorted(path for path in workflow_root.iterdir() if path.suffix in {".yml", ".yaml"})
    except OSError:
        raise ReleaseContractFailure("release_file_invalid") from None
    for path in paths:
        relative = path.relative_to(root).as_posix()
        text = _read_owned_text(root, relative).lower()
        if any(token in text for token in FORBIDDEN_RELEASE_AUTOMATION):
            raise ReleaseContractFailure("release_automation_forbidden")
    try:
        manifest = parse_manifest(_read_owned_text(root, "docs/manifest.yaml"))
    except ManifestError:
        raise ReleaseContractFailure("release_documentation_invalid") from None
    leaves = {leaf.id: leaf for leaf in iter_leaf_sections(manifest.sections)}
    release = leaves.get("release-policy")
    changelog = leaves.get("changelog")
    if (
        release is None
        or release.number != "9.2"
        or release.title != "Release Policy"
        or release.source != Path("docs/release-policy.md")
        or changelog is None
        or changelog.number != "10"
        or changelog.source != Path(CANONICAL_CHANGELOG)
    ):
        raise ReleaseContractFailure("release_documentation_invalid")


def validate_repository(root: Path) -> ReleaseState:
    """Validate the complete static intentionally-unreleased repository state."""
    version = load_project_version(root)
    changelog = validate_changelog_state(root, version)
    _validate_documentation(root, version)
    validate_no_release_automation(root)
    return ReleaseState(version, "intentionally_unreleased", changelog)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate the repository release policy")
    parser.add_argument("--root", type=Path, default=Path("."))
    args = parser.parse_args(argv)
    try:
        validate_repository(args.root)
    except ReleaseContractFailure as error:
        print(str(error), file=sys.stderr)
        return 1
    print("release_contract_ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
