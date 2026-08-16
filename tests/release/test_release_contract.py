from __future__ import annotations

from pathlib import Path

import pytest

from scripts.release.contract import ReleaseContractFailure, load_project_version


def _write_project(root: Path, version: object = "0.1.0") -> Path:
    root.mkdir(parents=True, exist_ok=True)
    if isinstance(version, str):
        encoded = f'"{version}"'
    elif version is True:
        encoded = "true"
    else:
        encoded = str(version)
    path = root / "pyproject.toml"
    path.write_text(f'[project]\nname = "data-eng-lab"\nversion = {encoded}\n', encoding="utf-8")
    return path


def test_load_project_version_accepts_exact_unreleased_version(tmp_path: Path) -> None:
    _write_project(tmp_path)

    assert load_project_version(tmp_path) == "0.1.0"


@pytest.mark.parametrize("version", [True, "01.0.0", "0.1", "0.1.0+", "1.0.0"])
def test_load_project_version_rejects_wrong_type_shape_or_value(tmp_path: Path, version: object) -> None:
    _write_project(tmp_path, version)

    with pytest.raises(ReleaseContractFailure, match="^project_version_invalid$"):
        load_project_version(tmp_path)


def test_load_project_version_rejects_symlink(tmp_path: Path) -> None:
    target = _write_project(tmp_path / "target")
    root = tmp_path / "repo"
    root.mkdir()
    (root / "pyproject.toml").symlink_to(target)

    with pytest.raises(ReleaseContractFailure, match="^release_file_invalid$"):
        load_project_version(root)


def test_load_project_version_rejects_oversized_file(tmp_path: Path) -> None:
    path = _write_project(tmp_path)
    path.write_bytes(b" " * 1_048_577)

    with pytest.raises(ReleaseContractFailure, match="^release_file_too_large$"):
        load_project_version(tmp_path)


@pytest.mark.parametrize(
    "body,code",
    [
        (b"\xff", "release_file_malformed"),
        (b"[project\nversion='0.1.0'", "project_metadata_invalid"),
        (b'name = "data-eng-lab"\n', "project_metadata_invalid"),
    ],
)
def test_load_project_version_rejects_malformed_metadata(tmp_path: Path, body: bytes, code: str) -> None:
    tmp_path.joinpath("pyproject.toml").write_bytes(body)

    with pytest.raises(ReleaseContractFailure, match=f"^{code}$"):
        load_project_version(tmp_path)
