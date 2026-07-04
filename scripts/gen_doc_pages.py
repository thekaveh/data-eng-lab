"""Generate MkDocs virtual pages from scenario/spark-app READMEs.

Uses mkdocs-gen-files to copy README content into the docs virtual filesystem
and mkdocs-literate-nav to drive navigation from a generated SUMMARY.md.
"""

import re
from pathlib import Path

import mkdocs_gen_files

REPO_ROOT = Path(__file__).parent.parent

SCENARIO_DIRS = sorted((REPO_ROOT / "scenarios").iterdir())
APP_DIRS = sorted((REPO_ROOT / "spark-apps").iterdir())


def _h1_label(readme: Path) -> str | None:
    """Return the first H1 heading text from *readme*, or None if absent."""
    for line in readme.read_text(encoding="utf-8").splitlines():
        m = re.match(r"^#\s+(.+)", line)
        if m:
            return m.group(1).strip()
    return None


def _write_page(src_readme: Path, dest_path: str) -> None:
    """Copy README content into the virtual docs filesystem."""
    content = src_readme.read_text(encoding="utf-8")
    with mkdocs_gen_files.open(dest_path, "w") as fh:
        fh.write(content)
    mkdocs_gen_files.set_edit_path(dest_path, str(src_readme.relative_to(REPO_ROOT)))


# ── Scenarios ─────────────────────────────────────────────────────────────────
scenario_entries: list[tuple[str, str]] = []  # (nav_label, dest_path)
for scenario_dir in SCENARIO_DIRS:
    readme = scenario_dir / "README.md"
    if not readme.exists():
        continue
    name = scenario_dir.name
    dest = f"scenarios/{name}.md"
    _write_page(readme, dest)
    label = _h1_label(readme) or name
    scenario_entries.append((label, dest))

# ── Spark apps ────────────────────────────────────────────────────────────────
app_entries: list[tuple[str, str]] = []  # (nav_label, dest_path)
for app_dir in APP_DIRS:
    readme = app_dir / "README.md"
    if not readme.exists():
        continue
    name = app_dir.name
    dest = f"spark-apps/{name}.md"
    _write_page(readme, dest)
    label = _h1_label(readme) or name
    app_entries.append((label, dest))

# ── SUMMARY.md (literate-nav) ─────────────────────────────────────────────────
summary_lines: list[str] = [
    "- [Home](index.md)\n",
    "- [Getting started](getting-started.md)\n",
    "- Scenarios:\n",
]
for label, page in scenario_entries:
    summary_lines.append(f"    - [{label}]({page})\n")

summary_lines.append("- Spark apps:\n")
for label, page in app_entries:
    summary_lines.append(f"    - [{label}]({page})\n")

summary_lines += [
    "- Lakehouse & Atlas:\n",
    "    - [Lakehouse](lakehouse.md)\n",
    "    - [Atlas expectations](atlas-expectations.md)\n",
    "    - [Atlas enablement](atlas-enablement.md)\n",
    "    - [A7/A9 feedback](atlas-feedback-a7a9.md)\n",
    "    - [Go-live runbook](go-live.md)\n",
    "- [Datasets](datasets.md)\n",
    "- [Scenario catalog](scenarios.md)\n",
    "- [Spark apps overview](spark-apps.md)\n",
]

with mkdocs_gen_files.open("SUMMARY.md", "w") as nav_fh:
    nav_fh.writelines(summary_lines)
