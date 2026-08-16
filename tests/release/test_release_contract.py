from __future__ import annotations

import sys
from pathlib import Path
from shutil import copy2
from subprocess import run

import pytest

from scripts.release.contract import (
    ReleaseContractFailure,
    load_project_version,
    validate_changelog_state,
    validate_no_release_automation,
)

ROOT = Path(__file__).resolve().parents[2]


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


def _copy_changelogs(root: Path) -> None:
    (root / "docs").mkdir(parents=True)
    copy2(ROOT / "CHANGELOG.md", root / "CHANGELOG.md")
    copy2(ROOT / "docs" / "CHANGELOG.md", root / "docs" / "CHANGELOG.md")


def test_changelog_state_uses_one_canonical_unreleased_history(tmp_path: Path) -> None:
    _copy_changelogs(tmp_path)

    assert validate_changelog_state(tmp_path, "0.1.0") == "docs/CHANGELOG.md"


@pytest.mark.parametrize(
    "heading",
    [
        "##  1. [Unreleased]",
        "##\t1. [Unreleased]",
        "1. [Unreleased]\n-----------------",
    ],
)
def test_changelog_state_accepts_equivalent_canonical_h2_without_traceback(tmp_path: Path, heading: str) -> None:
    _copy_changelogs(tmp_path)
    canonical = tmp_path / "docs" / "CHANGELOG.md"
    canonical.write_text(
        canonical.read_text(encoding="utf-8").replace("## 1. [Unreleased]", heading),
        encoding="utf-8",
    )

    assert validate_changelog_state(tmp_path, "0.1.0") == "docs/CHANGELOG.md"


def test_changelog_state_rejects_duplicate_unreleased_heading(tmp_path: Path) -> None:
    _copy_changelogs(tmp_path)
    canonical = tmp_path / "docs" / "CHANGELOG.md"
    canonical.write_text(canonical.read_text(encoding="utf-8") + "\n## 1. [Unreleased]\n", encoding="utf-8")

    with pytest.raises(ReleaseContractFailure, match="^canonical_changelog_invalid$"):
        validate_changelog_state(tmp_path, "0.1.0")


@pytest.mark.parametrize(
    "heading",
    [
        "## 2. [Unreleased]\r\n",
        "## 2. [0.1.0] - 2026-08-16\r\n",
    ],
)
def test_changelog_state_rejects_crlf_release_headings(tmp_path: Path, heading: str) -> None:
    _copy_changelogs(tmp_path)
    canonical = tmp_path / "docs" / "CHANGELOG.md"
    canonical.write_bytes(canonical.read_bytes() + b"\n" + heading.encode("utf-8"))

    code = "canonical_changelog_invalid" if "Unreleased" in heading else "release_state_contradictory"
    with pytest.raises(ReleaseContractFailure, match=f"^{code}$"):
        validate_changelog_state(tmp_path, "0.1.0")


@pytest.mark.parametrize(
    "opening,closing",
    [
        ("```markdown\n", "\n```"),
        ("<!--\n", "\n-->"),
        ("<div>\n", "\n</div>"),
    ],
)
def test_changelog_state_rejects_hidden_only_unreleased_heading(tmp_path: Path, opening: str, closing: str) -> None:
    _copy_changelogs(tmp_path)
    canonical = tmp_path / "docs" / "CHANGELOG.md"
    canonical.write_text(opening + canonical.read_text(encoding="utf-8") + closing, encoding="utf-8")

    with pytest.raises(ReleaseContractFailure, match="^canonical_changelog_invalid$"):
        validate_changelog_state(tmp_path, "0.1.0")


@pytest.mark.parametrize(
    "example",
    [
        "```markdown\n## 2. [0.1.0] - example\n```",
        "<!--\n## 2. [0.1.0] - example\n-->",
    ],
)
def test_changelog_state_ignores_hidden_release_heading_examples(tmp_path: Path, example: str) -> None:
    _copy_changelogs(tmp_path)
    canonical = tmp_path / "docs" / "CHANGELOG.md"
    canonical.write_text(canonical.read_text(encoding="utf-8") + f"\n{example}\n", encoding="utf-8")

    assert validate_changelog_state(tmp_path, "0.1.0") == "docs/CHANGELOG.md"


@pytest.mark.parametrize(
    "heading",
    [
        "## 2. [Unreleased]",
        "  ## 2. [Unreleased]",
        "##  2. [Unreleased]",
        "2. [Unreleased]\n-----------------",
        "## 2. [0.1.0]",
        "  ## 2. [0.1.0] - 2026-08-16",
        "##  2. [0.1.0]",
        "2. [0.1.0] - 2026-08-16\n----------------------------",
        "## 2. [0.1.0] - released",
    ],
)
def test_changelog_state_rejects_any_duplicate_or_current_release_heading(tmp_path: Path, heading: str) -> None:
    _copy_changelogs(tmp_path)
    canonical = tmp_path / "docs" / "CHANGELOG.md"
    canonical.write_text(canonical.read_text(encoding="utf-8") + f"\n{heading}\n", encoding="utf-8")

    code = "canonical_changelog_invalid" if "Unreleased" in heading else "release_state_contradictory"
    with pytest.raises(ReleaseContractFailure, match=f"^{code}$"):
        validate_changelog_state(tmp_path, "0.1.0")


def test_changelog_state_rejects_released_current_version(tmp_path: Path) -> None:
    _copy_changelogs(tmp_path)
    canonical = tmp_path / "docs" / "CHANGELOG.md"
    canonical.write_text(
        canonical.read_text(encoding="utf-8") + "\n## 2. [0.1.0] - 2026-08-16\n",
        encoding="utf-8",
    )

    with pytest.raises(ReleaseContractFailure, match="^release_state_contradictory$"):
        validate_changelog_state(tmp_path, "0.1.0")


@pytest.mark.parametrize(
    "mutation",
    [
        "\n- independently maintained entry\n",
        "\n[canonical changelog](docs/other.md)\n",
        "\nProject version `0.1.0` is released.\n",
    ],
)
def test_changelog_state_rejects_root_index_drift(tmp_path: Path, mutation: str) -> None:
    _copy_changelogs(tmp_path)
    root_changelog = tmp_path / "CHANGELOG.md"
    root_changelog.write_text(root_changelog.read_text(encoding="utf-8") + mutation, encoding="utf-8")

    with pytest.raises(ReleaseContractFailure, match="^root_changelog_invalid$"):
        validate_changelog_state(tmp_path, "0.1.0")


def test_repository_has_no_automatic_release_or_publish_workflow() -> None:
    validate_no_release_automation(ROOT)


def _copy_workflows(root: Path) -> Path:
    target = root / ".github" / "workflows"
    target.mkdir(parents=True)
    for source in sorted((ROOT / ".github" / "workflows").glob("*.yml")):
        copy2(source, target / source.name)
    privileged = root / "scripts" / "docs" / "push_wiki.py"
    privileged.parent.mkdir(parents=True)
    copy2(ROOT / "scripts" / "docs" / "push_wiki.py", privileged)
    return target


@pytest.mark.parametrize(
    "token",
    [
        "gh release create",
        "gh api --method POST repos/example/project/releases",
        "curl -X POST https://api.github.com/repos/example/project/releases",
        "git tag v0.1.0",
        "git -c user.name=release tag v0.1.0",
        "git push origin refs/tags/v0.1.0",
        "actions/create-release@",
        "ncipollo/release-action@0123456789012345678901234567890123456789",
        "softprops/action-gh-release@",
        "pypa/gh-action-pypi-publish@",
        "twine upload",
        "uv publish",
        "./scripts/publish.sh",
        "uv run python scripts/release/publish.py",
    ],
)
def test_release_automation_contract_rejects_publish_tokens(tmp_path: Path, token: str) -> None:
    workflow = _copy_workflows(tmp_path) / "ci.yml"
    workflow.write_text(workflow.read_text(encoding="utf-8") + f"\n# {token}\n", encoding="utf-8")

    with pytest.raises(ReleaseContractFailure, match="^release_automation_forbidden$"):
        validate_no_release_automation(tmp_path)


def test_release_automation_contract_rejects_tag_trigger(tmp_path: Path) -> None:
    workflow = _copy_workflows(tmp_path) / "ci.yml"
    workflow.write_text(
        workflow.read_text(encoding="utf-8") + "\n# trigger injection\non:\n  push:\n    tags: ['v*']\n",
        encoding="utf-8",
    )

    with pytest.raises(ReleaseContractFailure, match="^release_automation_forbidden$"):
        validate_no_release_automation(tmp_path)


def test_release_automation_contract_rejects_new_workflow_even_without_known_token(tmp_path: Path) -> None:
    workflows = _copy_workflows(tmp_path)
    (workflows / "publish.yml").write_text("name: opaque\non: workflow_dispatch\njobs: {}\n", encoding="utf-8")

    with pytest.raises(ReleaseContractFailure, match="^release_automation_forbidden$"):
        validate_no_release_automation(tmp_path)


def test_release_automation_contract_rejects_any_allowlisted_workflow_byte_change(tmp_path: Path) -> None:
    workflow = _copy_workflows(tmp_path) / "ci.yml"
    workflow.write_text(workflow.read_text(encoding="utf-8") + "\n# harmless drift\n", encoding="utf-8")

    with pytest.raises(ReleaseContractFailure, match="^release_automation_forbidden$"):
        validate_no_release_automation(tmp_path)


def test_release_automation_contract_rejects_privileged_local_code_drift(tmp_path: Path) -> None:
    _copy_workflows(tmp_path)
    privileged = tmp_path / "scripts" / "docs" / "push_wiki.py"
    privileged.write_text(
        privileged.read_text(encoding="utf-8") + "\n# release-capable token consumer\n",
        encoding="utf-8",
    )

    with pytest.raises(ReleaseContractFailure, match="^release_automation_forbidden$"):
        validate_no_release_automation(tmp_path)


def test_wiki_push_runs_fixed_local_code_without_dependency_sync() -> None:
    workflow = (ROOT / ".github" / "workflows" / "docs-deploy.yml").read_text(encoding="utf-8")

    assert "wiki-build:" in workflow
    assert "needs: wiki-build" in workflow
    assert "actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02" in workflow
    assert "actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093" in workflow
    assert "/usr/bin/python3 -I scripts/docs/push_wiki.py --push --root ." in workflow


def test_load_project_version_rejects_deep_toml_recursion(tmp_path: Path) -> None:
    nested = "'0.1.0'"
    for _ in range(500):
        nested = f"[{nested}]"
    tmp_path.joinpath("pyproject.toml").write_text(
        f'[project]\nname = "data-eng-lab"\nversion = {nested}\n', encoding="utf-8"
    )

    with pytest.raises(ReleaseContractFailure, match="^project_metadata_invalid$"):
        load_project_version(tmp_path)


def test_release_contract_cli_emits_one_success_token() -> None:
    completed = run(
        [sys.executable, "-m", "scripts.release.contract", "--root", str(ROOT)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0
    assert completed.stdout == "release_contract_ok\n"
    assert completed.stderr == ""


def test_makefile_exposes_exact_release_check_command() -> None:
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    assert "release-check:" in makefile
    assert "uv run python -m scripts.release.contract --root ." in makefile
