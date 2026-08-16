from __future__ import annotations

import re
import tomllib
from pathlib import Path

MAX_RELEASE_FILE_BYTES = 1_048_576
EXPECTED_VERSION = "0.1.0"
SEMVER = re.compile(
    r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    r"(?:-(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)"
    r"(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*)?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$"
)


class ReleaseContractFailure(ValueError):
    """The repository release policy is malformed or contradictory."""


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
