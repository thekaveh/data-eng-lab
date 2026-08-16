from __future__ import annotations

from pathlib import Path

from scripts.security.repository_protections import validate_evidence

ROOT = Path(__file__).resolve().parents[2]
RUNBOOK = ROOT / "docs" / "security-automation.md"
EVIDENCE = ROOT / "docs" / "evidence" / "repository-security-protections.json"
DESIGN = ROOT / "docs" / "superpowers" / "specs" / "2026-08-16-repository-security-protections-design.md"
PLAN = ROOT / "docs" / "superpowers" / "plans" / "2026-08-16-repository-security-protections.md"


def test_runbook_defines_authoritative_settings_and_safe_probe() -> None:
    text = " ".join(RUNBOOK.read_text(encoding="utf-8").split())
    required = (
        "Repository security protections",
        "PUT /repos/thekaveh/data-eng-lab/vulnerability-alerts",
        "PUT /repos/thekaveh/data-eng-lab/automated-security-fixes",
        "PATCH /repos/thekaveh/data-eng-lab",
        "GET /repos/thekaveh/data-eng-lab/secret-scanning/alerts",
        "GET /repos/thekaveh/data-eng-lab/code-scanning/analyses",
        "https://github.com/thekaveh/data-eng-lab/settings/security_analysis",
        "GitHub's published dummy token",
        "never bypass",
        "exact remote probe ref",
        "repository_security_ok",
    )
    for value in required:
        assert value in text


def test_runbook_records_optional_plan_boundary_and_no_real_secret() -> None:
    text = " ".join(RUNBOOK.read_text(encoding="utf-8").split())
    assert "GitHub Team or Enterprise with Secret Protection" in text
    assert "no real credential" in text
    assert "workflow files are not settings evidence" in text
    assert "private vulnerability reporting remains outside issue #93" in text.lower()


def test_design_and_plan_preserve_offline_and_gitflow_boundaries() -> None:
    design = DESIGN.read_text(encoding="utf-8")
    plan = PLAN.read_text(encoding="utf-8")
    for text in (design, plan):
        assert "C0/I0/M0" in text
        assert "GitFlow" in text
    assert "no recurring workflow" in design
    assert "no recurring workflow" in plan
    assert "Closes #93" in plan


def test_committed_evidence_is_canonical_and_valid() -> None:
    assert not EVIDENCE.is_symlink()
    body = EVIDENCE.read_bytes()
    value = validate_evidence(body)
    assert value["repository"] == "thekaveh/data-eng-lab"


def test_changelog_records_server_side_protections() -> None:
    text = (ROOT / "docs" / "CHANGELOG.md").read_text(encoding="utf-8")
    assert "GitHub repository security protections" in text
    assert "safe dummy-token rejection probe" in text
