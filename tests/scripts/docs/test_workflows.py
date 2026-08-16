"""Contracts for documentation CI and publication workflows."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
WORKFLOWS = ROOT / ".github/workflows"


def _load_workflow(name: str) -> dict:
    return yaml.load((WORKFLOWS / name).read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


def _executable_lines(job: dict) -> list[str]:
    lines: list[str] = []
    for step in job["steps"]:
        for line in step.get("run", "").splitlines():
            normalized = line.strip()
            if normalized and not normalized.startswith("#"):
                lines.append(normalized)
    return lines


def _command_index(lines: list[str], command: str, *, prefix: bool = False) -> int:
    for index, line in enumerate(lines):
        if line == command or (prefix and line.startswith(f"{command} ")):
            return index
    raise AssertionError(f"executable command not found: {command}")


def _step_index(steps: list[dict], command: str) -> int:
    for index, step in enumerate(steps):
        if command in _executable_lines({"steps": [step]}):
            return index
    raise AssertionError(f"executable step not found: {command}")


def test_executable_command_matcher_rejects_echoes_and_comments():
    lines = _executable_lines(
        {
            "steps": [
                {
                    "run": """
                        # make docs-check
                        echo make docs-check
                    """
                }
            ]
        }
    )

    assert lines == ["echo make docs-check"]
    with pytest.raises(AssertionError, match="executable command not found"):
        _command_index(lines, "make docs-check")


def test_ci_docs_job_uses_canonical_gate():
    workflow = _load_workflow("ci.yml")
    commands = _executable_lines(workflow["jobs"]["docs-build"])
    required = [
        "sudo apt-get install -y libcairo2",
        "make docs-check",
        "uv run --group dev ruff check scripts/docs/",
        "uv run --group dev pytest tests/scripts/docs/ -q",
    ]

    assert [_command_index(commands, command) for command in required] == sorted(
        _command_index(commands, command) for command in required
    )
    for legacy in (
        "uv run --group dev python scripts/build_docs.py",
        "uv run --group dev python scripts/check_surfaces.py",
        "uv run --group dev python scripts/check_diagrams.py",
    ):
        with pytest.raises(AssertionError, match="executable command not found"):
            _command_index(commands, legacy, prefix=True)


def test_make_docs_check_is_non_mutating_and_checks_before_strict_build():
    lines = (ROOT / "Makefile").read_text(encoding="utf-8").splitlines()
    start = lines.index("docs-check: ## Verify all documentation surfaces and build the strict site")
    recipe: list[str] = []
    for line in lines[start + 1 :]:
        if line and not line.startswith("\t"):
            break
        if line.startswith("\t"):
            recipe.append(line.strip())

    check = "uv run --group dev python -m scripts.docs.check_docs --root ."
    strict_build = "uv run --group dev mkdocs build --strict"
    renderer = "uv run --group dev python -m scripts.docs.render_diagrams --root ."
    assert recipe == [check, strict_build]
    assert renderer not in recipe


def test_make_docs_wiki_regenerates_and_validates_before_staging():
    lines = (ROOT / "Makefile").read_text(encoding="utf-8").splitlines()
    start = lines.index("docs-wiki: ## Generate and validate the wiki projection without pushing")
    recipe: list[str] = []
    for line in lines[start + 1 :]:
        if line and not line.startswith("\t"):
            break
        if line.startswith("\t"):
            recipe.append(line.strip())

    assert recipe == [
        "uv run --group dev python -m scripts.docs.check_docs --root .",
        "uv run --group dev python -m scripts.docs.push_wiki --check --root .",
    ]


def test_all_publication_jobs_are_main_ref_gated():
    workflow = _load_workflow("docs-deploy.yml")

    assert "workflow_dispatch" in workflow["on"]
    for job_name in ("build", "deploy", "wiki"):
        assert workflow["jobs"][job_name]["if"] == "github.ref == 'refs/heads/main'"


def test_publish_workflow_generates_site_before_pages_deploy():
    workflow = _load_workflow("docs-deploy.yml")

    assert workflow["on"]["push"]["branches"] == ["main"]
    assert workflow["permissions"] == {
        "contents": "read",
        "pages": "write",
        "id-token": "write",
    }
    assert workflow["concurrency"] == {"group": "pages", "cancel-in-progress": "false"}

    build = workflow["jobs"]["build"]
    build_steps = build["steps"]
    commands = _executable_lines(build)
    cairo = _step_index(build_steps, "sudo apt-get install -y libcairo2")
    setup_uv = next(
        index for index, step in enumerate(build_steps) if step.get("uses", "").startswith("astral-sh/setup-uv@")
    )
    upload = next(
        index
        for index, step in enumerate(build_steps)
        if step.get("uses", "").startswith("actions/upload-pages-artifact@")
    )
    required = [
        "sudo apt-get install -y libcairo2",
        "uv run --group dev python -m scripts.docs.check_docs --root .",
        "uv run --group dev python -m scripts.docs.build_docs --site --root .",
        "uv run --group dev mkdocs build --strict",
    ]
    command_positions = [_command_index(commands, command, prefix=True) for command in required]

    assert command_positions == sorted(command_positions)
    assert cairo < setup_uv
    assert _step_index(build_steps, required[-1]) < upload
    assert build_steps[upload]["with"]["path"] == "site"
    assert workflow["jobs"]["deploy"]["needs"] == "build"


def test_publish_workflow_pushes_generated_wiki_after_pages_deploy():
    workflow = _load_workflow("docs-deploy.yml")
    wiki_build = workflow["jobs"]["wiki-build"]
    wiki = workflow["jobs"]["wiki"]

    assert wiki_build["needs"] == "deploy"
    assert wiki_build["permissions"] == {"contents": "read"}
    build_commands = _executable_lines(wiki_build)
    required_build = [
        "sudo apt-get install -y libcairo2",
        "uv run --group dev python -m scripts.docs.check_docs --root .",
        "uv run --group dev python -m scripts.docs.build_docs --wiki --root .",
    ]
    assert [_command_index(build_commands, command) for command in required_build] == sorted(
        _command_index(build_commands, command) for command in required_build
    )
    upload = next(step for step in wiki_build["steps"] if step.get("uses", "").startswith("actions/upload-artifact@"))
    assert upload["uses"] == "actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02"
    assert upload["with"] == {
        "name": "wiki-${{ github.sha }}",
        "path": "generated/wiki",
        "if-no-files-found": "error",
        "retention-days": "1",
    }

    assert wiki["needs"] == "wiki-build"
    assert wiki["permissions"] == {"contents": "write"}
    download = next(step for step in wiki["steps"] if step.get("uses", "").startswith("actions/download-artifact@"))
    assert download["uses"] == "actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093"
    assert download["with"] == {
        "name": "wiki-${{ github.sha }}",
        "path": "generated/wiki",
    }

    push_step = next(step for step in wiki["steps"] if step.get("name") == "Push generated wiki")
    assert push_step["run"] == "/usr/bin/python3 -I scripts/docs/push_wiki.py --push --root ."
    assert push_step["env"]["WIKI_REMOTE"] == (
        "https://x-access-token:${{ secrets.GITHUB_TOKEN }}@github.com/${{ github.repository }}.wiki.git"
    )
    assert "WIKI_SSH_KEY" not in push_step["env"]
    assert not (WORKFLOWS / "docs-sync.yml").exists()


def test_publication_never_regenerates_committed_png_projections():
    workflow = (WORKFLOWS / "docs-deploy.yml").read_text(encoding="utf-8")
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")

    assert "--force-png" not in workflow
    docs_wiki = makefile.split("docs-wiki:", 1)[1].split("\n\n", 1)[0]
    assert "--force-png" not in docs_wiki


def test_mkdocs_dependencies_are_bounded_to_supported_major_lines():
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert '"mkdocs>=1.6,<2"' in text
    assert '"mkdocs-material>=9.5,<10"' in text
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    assert "export NO_MKDOCS_2_WARNING := 1" in makefile
