"""Contracts for documentation CI and publication workflows."""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]
WORKFLOWS = ROOT / ".github/workflows"


def _load_workflow(name: str) -> dict:
    return yaml.load((WORKFLOWS / name).read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


def _run_steps(job: dict) -> list[dict]:
    return [step for step in job["steps"] if "run" in step]


def _step_index(steps: list[dict], command: str) -> int:
    return next(
        index for index, step in enumerate(steps) if command in step.get("run", "")
    )


def test_ci_docs_job_uses_canonical_gate():
    workflow = _load_workflow("ci.yml")
    steps = workflow["jobs"]["docs-build"]["steps"]
    commands = "\n".join(step["run"] for step in _run_steps(workflow["jobs"]["docs-build"]))
    cairo = _step_index(steps, "sudo apt-get install -y libcairo2")
    canonical_gate = _step_index(steps, "make docs-check")

    assert cairo < canonical_gate
    assert "uv run --group dev ruff check scripts/docs/" in commands
    assert "uv run --group dev pytest tests/scripts/docs/ -q" in commands
    assert "python scripts/build_docs.py" not in commands
    assert "python scripts/check_surfaces.py" not in commands
    assert "python scripts/check_diagrams.py" not in commands


def test_publish_workflow_generates_site_before_pages_deploy():
    workflow = _load_workflow("docs-deploy.yml")

    assert workflow["on"]["push"]["branches"] == ["main"]
    assert workflow["permissions"] == {
        "contents": "read",
        "pages": "write",
        "id-token": "write",
    }
    assert workflow["concurrency"] == {"group": "pages", "cancel-in-progress": "false"}

    build_steps = workflow["jobs"]["build"]["steps"]
    cairo = _step_index(build_steps, "sudo apt-get install -y libcairo2")
    render_diagrams = _step_index(
        build_steps,
        "python -m scripts.docs.render_diagrams --root .",
    )
    render_site = _step_index(
        build_steps,
        "python -m scripts.docs.build_docs --site --root .",
    )
    build_site = _step_index(build_steps, "mkdocs build --strict")
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

    assert cairo < setup_uv < render_diagrams < render_site < build_site < upload
    assert build_steps[upload]["with"]["path"] == "site"
    assert workflow["jobs"]["deploy"]["needs"] == "build"


def test_publish_workflow_pushes_generated_wiki_after_pages_deploy():
    workflow = _load_workflow("docs-deploy.yml")
    wiki = workflow["jobs"]["wiki"]

    assert wiki["needs"] == "deploy"
    assert wiki["permissions"] == {"contents": "write"}
    commands = "\n".join(step["run"] for step in _run_steps(wiki))
    assert "python -m scripts.docs.build_docs --wiki --root ." in commands
    assert "python -m scripts.docs.push_wiki --push --root ." in commands

    push_step = next(
        step for step in wiki["steps"] if "scripts.docs.push_wiki" in step.get("run", "")
    )
    assert push_step["env"]["WIKI_REMOTE"] == (
        "https://x-access-token:${{ secrets.GITHUB_TOKEN }}@github.com/"
        "${{ github.repository }}.wiki.git"
    )
    assert "WIKI_SSH_KEY" not in push_step["env"]
    assert not (WORKFLOWS / "docs-sync.yml").exists()
