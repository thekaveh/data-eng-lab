from __future__ import annotations

import argparse
import hashlib
import re
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

from scripts.docs.manifest import ManifestError, iter_leaf_sections, parse_manifest
from scripts.security.contract import ContractFailure, load_yaml_exact

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
EXPECTED_WORKFLOW_SHA256 = {
    "ci.yml": "ab10e67213663ae00e78950d9058ca5f802f84de96957360da083ebf6dcecb63",
    "codeql.yml": "dd954c7817e6ae3e7731377f8753bc9cae54bfac58cf9c4c62debaf5f54cbc17",
    "dependency-security.yml": "4b9d2f0ac3306d65ec995ef4e6a4c88cbe18c074fa98ca120316ed19da1e4d3b",
    "docs-deploy.yml": "f0bb96bd0c83d293ba4378344815ac3d43072ca1ed76be4e62e768727c476f0e",
}
FORBIDDEN_WORKFLOW_PATTERNS = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"\bgh\s+release\b",
        r"\bgh\s+api\b[^\r\n]*(?:/releases|/git/refs(?:/tags)?)",
        r"\bgit\s+tag\b",
        r"\bgit\s+push\b[^\r\n]*(?:--tags\b|refs/tags/|\bv[0-9]+\.)",
        r"(?:^|[\s|;&])\./scripts/[A-Za-z0-9_./-]+",
        r"(?:^|[\s|;&])(?:ba)?sh\s+scripts/[A-Za-z0-9_./-]+",
    )
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
    except (tomllib.TOMLDecodeError, RecursionError):
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
    headings = re.findall(r"^[ ]{0,3}##[ \t]+(.+?)(?:[ \t]+#+)?[ \t]*$", canonical, re.MULTILINE)
    unreleased_headings = [
        heading for heading in headings if re.fullmatch(r"(?:[0-9]+\.[ \t]+)?\[Unreleased\](?:[ \t].*)?", heading)
    ]
    if unreleased_headings != ["1. [Unreleased]"]:
        raise ReleaseContractFailure("canonical_changelog_invalid")
    unreleased = canonical.split("## 1. [Unreleased]", 1)[1].split("\n## ", 1)[0]
    if (
        re.search(r"^### (?:Added|Changed)$", unreleased, re.MULTILINE) is None
        or re.search(r"^- \S", unreleased, re.MULTILINE) is None
    ):
        raise ReleaseContractFailure("canonical_changelog_invalid")
    version_heading = re.compile(rf"(?:[0-9]+\.[ \t]+)?\[{re.escape(version)}\](?:[ \t].*)?")
    if any(version_heading.fullmatch(heading) for heading in headings):
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
    try:
        manifest_text = _read_owned_text(root, "docs/manifest.yaml")
        load_yaml_exact(root / "docs" / "manifest.yaml")
        manifest = parse_manifest(manifest_text)
    except (ContractFailure, ManifestError, ReleaseContractFailure, RecursionError):
        raise ReleaseContractFailure("release_documentation_invalid") from None
    leaves = {leaf.id: leaf for leaf in iter_leaf_sections(manifest.sections)}
    release_policy = leaves.get("release-policy")
    changelog = leaves.get("changelog")
    if (
        release_policy is None
        or (release_policy.number, release_policy.title, release_policy.source)
        != (
            "9.2",
            "Release Policy",
            Path("docs/release-policy.md"),
        )
        or changelog is None
        or (changelog.number, changelog.title, changelog.source)
        != (
            "10",
            "Changelog",
            Path(CANONICAL_CHANGELOG),
        )
    ):
        raise ReleaseContractFailure("release_documentation_invalid")


def validate_no_release_automation(root: Path) -> None:
    """Reject repository workflows that publish a package or GitHub Release."""
    workflow_root = root / ".github" / "workflows"
    try:
        paths = sorted(path for path in workflow_root.iterdir() if path.suffix in {".yml", ".yaml"})
    except OSError:
        raise ReleaseContractFailure("release_automation_forbidden") from None
    if tuple(path.name for path in paths) != tuple(EXPECTED_WORKFLOW_SHA256):
        raise ReleaseContractFailure("release_automation_forbidden")
    for path in paths:
        relative = path.relative_to(root).as_posix()
        raw_text = _read_owned_text(root, relative)
        if hashlib.sha256(raw_text.encode("utf-8")).hexdigest() != EXPECTED_WORKFLOW_SHA256[path.name]:
            raise ReleaseContractFailure("release_automation_forbidden")
        text = raw_text.lower()
        try:
            document = load_yaml_exact(path)
        except ContractFailure:
            raise ReleaseContractFailure("release_automation_forbidden") from None
        events = document.get("on")
        if isinstance(events, dict):
            push = events.get("push")
            if "release" in events or (isinstance(push, dict) and any(key in push for key in ("tags", "tags-ignore"))):
                raise ReleaseContractFailure("release_automation_forbidden")
        if any(token in text for token in FORBIDDEN_RELEASE_AUTOMATION) or any(
            pattern.search(text) for pattern in FORBIDDEN_WORKFLOW_PATTERNS
        ):
            raise ReleaseContractFailure("release_automation_forbidden")


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
