from __future__ import annotations

from pathlib import Path

import yaml

from scripts.docs.build_docs import render_site, render_wiki
from scripts.docs.manifest import load_manifest

ROOT = Path(__file__).resolve().parents[2]
RUNBOOK = ROOT / "docs" / "security-automation.md"
POLICY = ROOT / ".github" / "SECURITY.md"
DESIGN = ROOT / "docs" / "superpowers" / "specs" / "2026-08-16-security-automation-design.md"
PLAN = ROOT / "docs" / "superpowers" / "plans" / "2026-08-16-security-automation.md"


def test_security_policy_defines_private_reporting_and_supported_line() -> None:
    text = POLICY.read_text(encoding="utf-8")

    assert "| `main` | Supported |" in text
    assert "private vulnerability reporting" in text.lower()
    assert "Do not open a public issue" in text
    assert "request a private channel without including vulnerability details" in text


def test_runbook_names_exact_coverage_and_limitations() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    required = (
        "uv.lock",
        "gh-archive-pipeline/pom.xml",
        "movielens-feature-pipeline/pom.xml",
        "nyc-taxi-data-quality/pom.xml",
        "nyc-taxi-etl/pom.xml",
        "nyc-taxi-medallion/pom.xml",
        "tpch-star-schema/pom.xml",
        "tpch-lock-requirements.txt",
        "CodeQL does not support Scala",
        "Maven test dependencies",
        "infra/",
        "default branch",
        "feature → develop → main",
        "Issue #93",
    )
    for value in required:
        assert value in text


def test_runbook_discloses_and_compensates_for_event_only_scanning() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    assert "does not provide continuous late-disclosure detection" in text
    assert "Repository Dependabot alerts are now the" in text
    assert "continuous compensating monitor" in text
    assert "observed both version-update and security-update pull requests" in text
    assert "when authoritative Dependabot state is unavailable" in text


def test_design_and_plan_bind_the_complete_dependency_inventory() -> None:
    required = "`datasets/tpch-lock-requirements.txt`"
    assert DESIGN.read_text(encoding="utf-8").count(required) >= 2
    assert PLAN.read_text(encoding="utf-8").count(required) >= 3


def test_exception_boundary_does_not_claim_unimplemented_enforcement() -> None:
    runbook = RUNBOOK.read_text(encoding="utf-8")
    design = DESIGN.read_text(encoding="utf-8")

    assert "provides no executable exception mechanism" in runbook
    assert "provides no executable exception mechanism" in design
    assert "Expired exceptions fail the repository contract" not in runbook
    assert "Malformed or expired exception records fail validation" not in design


def test_security_runbook_is_a_three_surface_manifest_leaf(tmp_path: Path) -> None:
    raw = yaml.safe_load((ROOT / "docs" / "manifest.yaml").read_text(encoding="utf-8"))
    leaves = [
        child
        for section in raw["sections"]
        for child in section.get("children", [])
        if child.get("id") == "security-automation"
    ]
    assert leaves == [
        {
            "id": "security-automation",
            "number": "9.1",
            "title": "Security Automation",
            "source": "docs/security-automation.md",
        }
    ]

    manifest = load_manifest(ROOT / "docs" / "manifest.yaml", ROOT)
    site = tmp_path / "site"
    wiki = tmp_path / "wiki"
    render_site(manifest, ROOT, site)
    render_wiki(manifest, ROOT, wiki)
    site_text = (site / "security-automation.md").read_text(encoding="utf-8")
    assert "CodeQL does not support Scala" in site_text
    assert "../.github/SECURITY.md" not in site_text
    wiki_text = (wiki / "Security-Automation.md").read_text(encoding="utf-8")
    assert "CodeQL does not support Scala" in wiki_text
    assert "security-automation.md" not in wiki_text
