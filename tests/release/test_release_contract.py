from __future__ import annotations

import sys
from pathlib import Path
from shutil import copy2, copytree
from subprocess import run

import pytest

from scripts.docs.build_docs import render_mkdocs_yml
from scripts.docs.manifest import load_manifest
from scripts.release.contract import (
    RELEASE_MARKDOWN_EXTENSIONS,
    ReleaseContractFailure,
    _validate_documentation,
    load_project_version,
    validate_changelog_state,
    validate_no_release_automation,
)

ROOT = Path(__file__).resolve().parents[2]


def test_release_changelog_uses_the_site_markdown_extension_set() -> None:
    expected = (
        "admonition",
        "attr_list",
        "md_in_html",
        "tables",
        "toc",
        "pymdownx.highlight",
        "pymdownx.inlinehilite",
        "pymdownx.superfences",
        "pymdownx.details",
        "pymdownx.tabbed",
        "pymdownx.emoji",
        "pymdownx.critic",
        "pymdownx.caret",
        "pymdownx.keys",
        "pymdownx.mark",
        "pymdownx.tilde",
    )
    mkdocs = render_mkdocs_yml(load_manifest(ROOT / "docs" / "manifest.yaml", ROOT))

    assert RELEASE_MARKDOWN_EXTENSIONS == expected
    for extension in expected:
        assert f"  - {extension}" in mkdocs


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
        "2. [Unreleased]\r\n---\r\n",
    ],
)
def test_changelog_state_rejects_crlf_release_headings(tmp_path: Path, heading: str) -> None:
    _copy_changelogs(tmp_path)
    canonical = tmp_path / "docs" / "CHANGELOG.md"
    canonical.write_bytes(canonical.read_bytes() + b"\n" + heading.encode("utf-8"))

    code = "canonical_changelog_invalid" if "Unreleased" in heading else "release_state_contradictory"
    with pytest.raises(ReleaseContractFailure, match=f"^{code}$"):
        validate_changelog_state(tmp_path, "0.1.0")


@pytest.mark.parametrize("setext", [False, True])
def test_changelog_state_accepts_wholly_crlf_canonical_document(tmp_path: Path, setext: bool) -> None:
    _copy_changelogs(tmp_path)
    canonical = tmp_path / "docs" / "CHANGELOG.md"
    text = canonical.read_text(encoding="utf-8")
    if setext:
        text = text.replace("## 1. [Unreleased]", "1. [Unreleased]\n-----------------")
    canonical.write_bytes(text.replace("\n", "\r\n").encode("utf-8"))

    assert validate_changelog_state(tmp_path, "0.1.0") == "docs/CHANGELOG.md"


@pytest.mark.parametrize(
    "opening,closing",
    [
        ("```markdown\n", "\n```"),
        ("<!--\n", "\n-->"),
        ("<div>\n", "\n</div>"),
        ("<div>release history\n", "\n</div> trailing"),
        ("<![CDATA[\n", "\n]]>"),
        ("<?release-policy\n", "\n?>"),
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
        "  ## 2. [Unreleased]",
        "  ## 2. [0.1.0] - 2026-08-16",
    ],
)
def test_changelog_state_ignores_hidden_release_heading_examples(tmp_path: Path, example: str) -> None:
    _copy_changelogs(tmp_path)
    canonical = tmp_path / "docs" / "CHANGELOG.md"
    canonical.write_text(canonical.read_text(encoding="utf-8") + f"\n{example}\n", encoding="utf-8")

    assert validate_changelog_state(tmp_path, "0.1.0") == "docs/CHANGELOG.md"


def test_changelog_state_ignores_comment_marker_inside_fenced_example(tmp_path: Path) -> None:
    _copy_changelogs(tmp_path)
    canonical = tmp_path / "docs" / "CHANGELOG.md"
    canonical.write_text(
        "```markdown\n<!-- literal unclosed comment\n```\n\n" + canonical.read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    assert validate_changelog_state(tmp_path, "0.1.0") == "docs/CHANGELOG.md"


@pytest.mark.parametrize(
    "hidden_evidence",
    [
        "<template><h3>Added</h3><ul><li>hidden only</li></ul></template>",
        '<div style="display:none"><h3>Added</h3><ul><li>hidden only</li></ul></div>',
        '<div style="display:none" style="display:block"><h3>Added</h3><ul><li>hidden only</li></ul></div>',
        "<dialog><h3>Added</h3><ul><li>hidden only</li></ul></dialog>",
        "<details><h3>Added</h3><ul><li>hidden only</li></ul></details>",
    ],
)
def test_changelog_state_rejects_hidden_only_subsection_evidence(tmp_path: Path, hidden_evidence: str) -> None:
    _copy_changelogs(tmp_path)
    canonical = tmp_path / "docs" / "CHANGELOG.md"
    canonical.write_text(
        f"# Changelog\n\n## 1. [Unreleased]\n{hidden_evidence}\n",
        encoding="utf-8",
    )

    with pytest.raises(ReleaseContractFailure, match="^canonical_changelog_invalid$"):
        validate_changelog_state(tmp_path, "0.1.0")


def test_changelog_state_rejects_nonvoid_self_closing_heading(tmp_path: Path) -> None:
    _copy_changelogs(tmp_path)
    canonical = tmp_path / "docs" / "CHANGELOG.md"
    canonical.write_text(
        canonical.read_text(encoding="utf-8") + "\nx <h2/>[0.1.0]\n",
        encoding="utf-8",
    )

    with pytest.raises(ReleaseContractFailure, match="^canonical_changelog_invalid$"):
        validate_changelog_state(tmp_path, "0.1.0")


def test_changelog_state_accepts_self_closing_svg_foreign_content(tmp_path: Path) -> None:
    _copy_changelogs(tmp_path)
    canonical = tmp_path / "docs" / "CHANGELOG.md"
    canonical.write_text(
        canonical.read_text(encoding="utf-8") + '\n<svg viewBox="0 0 1 1"><circle cx=".5" cy=".5" r=".5"/></svg>\n',
        encoding="utf-8",
    )

    assert validate_changelog_state(tmp_path, "0.1.0") == "docs/CHANGELOG.md"


@pytest.mark.parametrize(
    "ambiguous_html",
    [
        '<div style="display:none;display:block"><h2>2. [0.1.0]</h2></div>',
        '<div style="visibility:hidden;visibility:visible"><h2>2. [0.1.0]</h2></div>',
        '<h2 class="visibility-is-external">2. [0.1.0]</h2>',
        '<div class="visible"><h2/>[0.1.0]</div>',
        '<div class="visibility-is-external"><h3>Added</h3><ul><li>entry</li></ul></div>',
        "<svg><foreignObject><h2>2. [0.1.0]</h2></foreignObject></svg>",
        '<math><annotation-xml encoding="text/html"><h2>2. [0.1.0]</h2></annotation-xml></math>',
    ],
)
def test_changelog_state_rejects_ambiguous_html_visibility(tmp_path: Path, ambiguous_html: str) -> None:
    _copy_changelogs(tmp_path)
    canonical = tmp_path / "docs" / "CHANGELOG.md"
    canonical.write_text(
        canonical.read_text(encoding="utf-8") + f"\n{ambiguous_html}\n",
        encoding="utf-8",
    )

    with pytest.raises(ReleaseContractFailure, match="^canonical_changelog_invalid$"):
        validate_changelog_state(tmp_path, "0.1.0")


@pytest.mark.parametrize(
    "foreign",
    [
        '<svg><title>hidden</title><circle r="1"/></svg>0',
        "<svg><text>0</text></svg>",
        "<math><mtext>0</mtext></math>",
    ],
)
def test_foreign_graphics_cannot_enter_release_heading(tmp_path: Path, foreign: str) -> None:
    _copy_changelogs(tmp_path)
    canonical = tmp_path / "docs" / "CHANGELOG.md"
    canonical.write_text(
        canonical.read_text(encoding="utf-8") + f"\n<h2>2. [0.1.{foreign}]</h2>\n",
        encoding="utf-8",
    )

    with pytest.raises(ReleaseContractFailure, match="^canonical_changelog_invalid$"):
        validate_changelog_state(tmp_path, "0.1.0")


def test_foreign_graphics_cannot_supply_unreleased_heading_text(tmp_path: Path) -> None:
    _copy_changelogs(tmp_path)
    canonical = tmp_path / "docs" / "CHANGELOG.md"
    canonical.write_text(
        canonical.read_text(encoding="utf-8").replace(
            "## 1. [Unreleased]",
            "<h2>1. [Unre<svg><text>leased</text></svg>]</h2>",
        ),
        encoding="utf-8",
    )

    with pytest.raises(ReleaseContractFailure, match="^canonical_changelog_invalid$"):
        validate_changelog_state(tmp_path, "0.1.0")


@pytest.mark.parametrize(
    "integration",
    [
        "<svg><foreignObject/></svg>",
        '<math><annotation-xml encoding="text/html"/></math>',
    ],
)
def test_foreign_html_integration_rejects_when_self_closing(tmp_path: Path, integration: str) -> None:
    _copy_changelogs(tmp_path)
    canonical = tmp_path / "docs" / "CHANGELOG.md"
    canonical.write_text(
        canonical.read_text(encoding="utf-8") + f"\n{integration}\n",
        encoding="utf-8",
    )

    with pytest.raises(ReleaseContractFailure, match="^canonical_changelog_invalid$"):
        validate_changelog_state(tmp_path, "0.1.0")


@pytest.mark.parametrize("nested", ["h1", "h3", "h4", "h5", "h6"])
def test_changelog_state_rejects_nested_raw_headings(tmp_path: Path, nested: str) -> None:
    _copy_changelogs(tmp_path)
    canonical = tmp_path / "docs" / "CHANGELOG.md"
    canonical.write_text(
        canonical.read_text(encoding="utf-8") + f"\n<h2>2. [0.1.0]<{nested}>nested</{nested}></h2>\n",
        encoding="utf-8",
    )

    with pytest.raises(ReleaseContractFailure, match="^canonical_changelog_invalid$"):
        validate_changelog_state(tmp_path, "0.1.0")


@pytest.mark.parametrize(
    "heading",
    [
        "## 2. **[0.1.0]** - 2026-08-16",
        "## 2. [0.1.0]<br> - 2026-08-16",
        '## 2. [0.1.0]<img alt="release"> - 2026-08-16',
        "## 2. [0.1.0]<span> - 2026-08-16",
        "> ## 2. [0.1.0] - 2026-08-16",
        "- ## 2. [0.1.0] - 2026-08-16",
        "```lang`invalid\n## 2. [0.1.0] - 2026-08-16\n```",
        "2. [0.1.0] - 2026-08-16\r\n---\r\n",
    ],
)
def test_changelog_state_rejects_every_rendered_current_release_h2(tmp_path: Path, heading: str) -> None:
    _copy_changelogs(tmp_path)
    canonical = tmp_path / "docs" / "CHANGELOG.md"
    canonical.write_bytes(canonical.read_bytes() + b"\n" + heading.encode("utf-8"))

    with pytest.raises(ReleaseContractFailure, match="^release_state_contradictory$"):
        validate_changelog_state(tmp_path, "0.1.0")


@pytest.mark.parametrize(
    "heading",
    [
        "## 2. [Unreleased]",
        "##  2. [Unreleased]",
        "2. [Unreleased]\n-----------------",
        "## 2. [0.1.0]",
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


def test_release_documentation_rejects_missing_manifest_source(tmp_path: Path) -> None:
    copytree(ROOT / "docs", tmp_path / "docs")
    copy2(ROOT / "README.md", tmp_path / "README.md")
    manifest = tmp_path / "docs" / "manifest.yaml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace("docs/index.md", "docs/DOES-NOT-EXIST.md"),
        encoding="utf-8",
    )

    with pytest.raises(ReleaseContractFailure, match="^release_documentation_invalid$"):
        _validate_documentation(tmp_path, "0.1.0")


@pytest.mark.parametrize("relative", ["docs/index.md", "docs/diagrams/overview.html"])
def test_release_documentation_closes_manifest_symlink_loops(tmp_path: Path, relative: str) -> None:
    copytree(ROOT / "docs", tmp_path / "docs")
    copy2(ROOT / "README.md", tmp_path / "README.md")
    target = tmp_path / relative
    target.unlink()
    target.symlink_to(target.name)

    with pytest.raises(ReleaseContractFailure, match="^release_documentation_invalid$"):
        _validate_documentation(tmp_path, "0.1.0")


def test_release_documentation_closes_invalid_manifest_path_value(tmp_path: Path) -> None:
    copytree(ROOT / "docs", tmp_path / "docs")
    copy2(ROOT / "README.md", tmp_path / "README.md")
    manifest = tmp_path / "docs" / "manifest.yaml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace("source: docs/index.md", 'source: "docs/\\0index.md"'),
        encoding="utf-8",
    )

    with pytest.raises(ReleaseContractFailure, match="^release_documentation_invalid$"):
        _validate_documentation(tmp_path, "0.1.0")


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
