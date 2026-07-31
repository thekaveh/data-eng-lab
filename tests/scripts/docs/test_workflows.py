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
        index
        for index, step in enumerate(build_steps)
        if step.get("uses", "").startswith("astral-sh/setup-uv@")
    )
    upload = next(
        index
        for index, step in enumerate(build_steps)
        if step.get("uses", "").startswith("actions/upload-pages-artifact@")
    )
    required = [
        "sudo apt-get install -y libcairo2",
        "uv run --group dev python -m scripts.docs.render_diagrams --root .",
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
    wiki = workflow["jobs"]["wiki"]

    assert wiki["needs"] == "deploy"
    assert wiki["permissions"] == {"contents": "write"}
    commands = _executable_lines(wiki)
    required = [
        "uv run --group dev python -m scripts.docs.build_docs --wiki --root .",
        "uv run --group dev python -m scripts.docs.push_wiki --push --root .",
    ]
    command_positions = [_command_index(commands, command) for command in required]
    assert command_positions == sorted(command_positions)

    push_step = next(
        step for step in wiki["steps"] if "scripts.docs.push_wiki" in step.get("run", "")
    )
    assert push_step["env"]["WIKI_REMOTE"] == (
        "https://x-access-token:${{ secrets.GITHUB_TOKEN }}@github.com/"
        "${{ github.repository }}.wiki.git"
    )
    assert "WIKI_SSH_KEY" not in push_step["env"]
    assert not (WORKFLOWS / "docs-sync.yml").exists()
