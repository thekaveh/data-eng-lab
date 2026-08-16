from __future__ import annotations

from pathlib import Path

import yaml

from scripts.docs.build_docs import render_site, render_wiki
from scripts.docs.manifest import load_manifest

ROOT = Path(__file__).resolve().parents[2]
RUNBOOK = ROOT / "docs" / "security-automation.md"
POLICY = ROOT / ".github" / "SECURITY.md"


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
        "CodeQL does not support Scala",
        "Maven test dependencies",
        "infra/",
        "default branch",
        "feature → develop → main",
        "Issue #93",
    )
    for value in required:
        assert value in text


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
