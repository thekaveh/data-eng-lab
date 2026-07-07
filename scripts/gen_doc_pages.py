"""Generate MkDocs SUMMARY.md navigation from scenario and spark-app READMEs.

This script is optional because navigation is now defined explicitly in mkdocs.yml.
Run this if nav needs to be auto-generated (e.g., after adding a new scenario).

Usage:
    python3 scripts/gen_doc_pages.py
"""

import re
from pathlib import Path

import mkdocs_gen_files

REPO_ROOT = Path(__file__).parent.parent


def _h1_label(readme: Path) -> str | None:
    """Return the first H1 heading text from *readme*, or None if absent."""
    for line in readme.read_text(encoding="utf-8").splitlines():
        m = re.match(r"^#\s+(.+)", line)
        if m:
            return m.group(1).strip()
    return None


def _nav_entry(readme: Path, dest_path: str) -> tuple[str, str] | None:
    """Return (label, dest_path) for a README, or None if absent."""
    label = _h1_label(readme)
    return (label or readme.parent.name.replace("-", " ").title(), dest_path)


def main():
    scenario_entries: list[tuple[str, str]] = []
    for scenario_dir in sorted((REPO_ROOT / "scenarios").iterdir()):
        readme = scenario_dir / "README.md"
        if not readme.exists():
            continue
        dest = f"scenarios/{scenario_dir.name}.md"
        entry = _nav_entry(readme, dest)
        if entry:
            scenario_entries.append(entry)

    app_entries: list[tuple[str, str]] = []
    for app_dir in sorted((REPO_ROOT / "spark-apps").iterdir()):
        readme = app_dir / "README.md"
        if not readme.exists():
            continue
        dest = f"spark-apps/{app_dir.name}.md"
        entry = _nav_entry(readme, dest)
        if entry:
            app_entries.append(entry)

    summary_lines: list[str] = [
        "- [Home](index.md)",
        "- [Getting Started](getting-started.md)",
        "",
        "- Scenarios:",
    ]
    for label, page in scenario_entries:
        indent = "    - " if len(scenario_entries) <= 25 else "      - "
        summary_lines.append(f"{indent}[{label}]({page})")

    summary_lines.append("")
    summary_lines.append("- Spark Apps:")
    for label, page in app_entries:
        indent = "    - " if len(app_entries) <= 25 else "        - "
        summary_lines.append(f"{indent}[{label}]({page})")

    summary_lines += [
        "",
        "- Lakehouse & Atlas:",
        "    - [Lakehouse](lakehouse.md)",
        "    - [Atlas Expectations](atlas-expectations.md)",
        "    - [Atlas Go-Live Runbook](go-live.md)",
        "    - [Atlas Go-Live Results](go-live-results.md)",
        "    - [Atlas Feedback (A7/A9)](atlas-feedback-a7a9.md)",
        "    - [Atlas Go-Live Findings](atlas-feedback-go-live.md)",
        "    - [Atlas Enablement](atlas-enablement.md)",
        "",
        "- [Datasets](datasets.md)",
        "- [Changelog](CHANGELOG.md)",
    ]

    with mkdocs_gen_files.open("SUMMARY.md", "w") as nav_fh:
        nav_fh.write("\n".join(summary_lines) + "\n")

    print(f"SUMMARY.md written: {len(scenario_entries)} scenarios, {len(app_entries)} spark apps")


if __name__ == "__main__":
    main()
