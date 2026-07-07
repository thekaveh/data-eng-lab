#!/usr/bin/env python3
"""Assemble a wiki/ staging directory mirroring the MkDocs docs.

Run from the repository root:
    python3 scripts/build_wiki.py

Output:
    wiki/Home.md          — landing page with section index
    wiki/_Sidebar.md      — GitHub wiki sidebar navigation
    wiki/<Guide>.md       — one file per docs/*.md guide
    wiki/Scenario-<name>.md  — one file per scenarios/<name>/README.md
    wiki/App-<name>.md       — one file per spark-apps/<name>/README.md
"""

from __future__ import annotations

import re
import shutil
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
WIKI_DIR = REPO_ROOT / "wiki"
DOCS_DIR = REPO_ROOT / "docs"
SITE_URL = "https://thekaveh.github.io/data-eng-lab/"

BANNER_TEMPLATE = (
    "> 📖 **Full docs site:** [{url}]({url})  \n"
    "> This page is a mirror. For the rendered, canonical version visit the link above.\n\n"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _h1(path: Path) -> str | None:
    """Return the first H1 heading text from *path*, or None."""
    for line in path.read_text(encoding="utf-8").splitlines():
        m = re.match(r"^#\s+(.+)", line)
        if m:
            return m.group(1).strip()
    return None


def _write(dest: Path, content: str) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(content, encoding="utf-8")


def _mirror_with_banner(src: Path, dest: Path, section_url: str | None = None) -> None:
    """Copy *src* into *dest* with a site-link banner prepended."""
    url = section_url or SITE_URL
    banner = BANNER_TEMPLATE.format(url=url)
    original = src.read_text(encoding="utf-8")
    _write(dest, banner + original)


# ---------------------------------------------------------------------------
# Guide pages  (docs/*.md, excluding index.md and superpowers/)
# ---------------------------------------------------------------------------

# Map docs filename → wiki page name (spaces → hyphens; title-ish)
GUIDE_MAP: dict[str, str] = {
    "getting-started.md": "Getting-Started.md",
    "datasets.md": "Datasets.md",
    "lakehouse.md": "Lakehouse.md",
    "scenarios.md": "Scenario-Catalog.md",
    "spark-apps.md": "Spark-Apps-Overview.md",
    "go-live.md": "Go-Live-Runbook.md",
    "atlas-expectations.md": "Atlas-Expectations.md",
    "atlas-enablement.md": "Atlas-Enablement.md",
    "atlas-feedback-a7a9.md": "Atlas-Feedback-A7-A9.md",
}


def _build_guides() -> list[tuple[str, str]]:
    """Return list of (wiki_page_stem, wiki_filename) for guides."""
    entries: list[tuple[str, str]] = []
    for src_name, wiki_name in GUIDE_MAP.items():
        src = DOCS_DIR / src_name
        if not src.exists():
            continue
        dest = WIKI_DIR / wiki_name
        title = _h1(src) or Path(wiki_name).stem.replace("-", " ")
        section_url = SITE_URL + src_name.replace(".md", "/")
        _mirror_with_banner(src, dest, section_url)
        entries.append((title, wiki_name))
    return entries


# ---------------------------------------------------------------------------
# Scenario pages
# ---------------------------------------------------------------------------

def _build_scenarios() -> list[tuple[str, str]]:
    docs_dir = REPO_ROOT / "docs" / "scenarios"
    entries: list[tuple[str, str]] = []
    for md_file in sorted(docs_dir.glob("*.md")):
        if md_file.name == "index.md":
            continue           # Skip index; it's handled separately
        name = md_file.stem
        wiki_name = f"Scenario-{name}.md"
        dest = WIKI_DIR / wiki_name
        title = _h1(md_file) or name
        section_url = SITE_URL + f"scenarios/{name}/"
        original = md_file.read_text(encoding="utf-8")
        content = BANNER_TEMPLATE.format(url=section_url) + original
        _write(dest, content)
        entries.append((title, wiki_name))
    return entries

# ---------------------------------------------------------------------------
# Spark-app pages
# ---------------------------------------------------------------------------

def _build_apps() -> list[tuple[str, str]]:
    docs_dir = REPO_ROOT / "docs" / "spark-apps"
    entries: list[tuple[str, str]] = []
    for md_file in sorted(docs_dir.glob("*.md")):
        if md_file.name == "index.md":
            continue
        name = md_file.stem
        wiki_name = f"App-{name}.md"
        dest = WIKI_DIR / wiki_name
        title = _h1(md_file) or name
        section_url = SITE_URL + f"spark-apps/{name}/"
        original = md_file.read_text(encoding="utf-8")
        content = BANNER_TEMPLATE.format(url=section_url) + original
        _write(dest, content)
        entries.append((title, wiki_name))
    return entries


# ---------------------------------------------------------------------------
# Home.md
# ---------------------------------------------------------------------------

def _build_home(
    guide_entries: list[tuple[str, str]],
    scenario_entries: list[tuple[str, str]],
    app_entries: list[tuple[str, str]],
) -> None:
    lines: list[str] = [
        "# data-eng-lab\n\n",
        "An Iceberg-lakehouse-on-MinIO data-engineering lab on the "
        "[Atlas platform](https://github.com/thekaveh/atlas) — "
        "Scala/PySpark scenarios, Maven Spark apps, and the full medallion.\n\n",
        f"> 📖 **Full docs site:** [{SITE_URL}]({SITE_URL})\n\n",
        "---\n\n",
        "## Guides\n\n",
    ]
    for title, wiki_name in guide_entries:
        stem = Path(wiki_name).stem
        lines.append(f"- [[{stem}|{title}]]\n")

    lines += [
        "\n## Scenarios\n\n",
    ]
    for title, wiki_name in scenario_entries:
        stem = Path(wiki_name).stem
        lines.append(f"- [[{stem}|{title}]]\n")

    lines += [
        "\n## Spark Apps\n\n",
    ]
    for title, wiki_name in app_entries:
        stem = Path(wiki_name).stem
        lines.append(f"- [[{stem}|{title}]]\n")

    _write(WIKI_DIR / "Home.md", "".join(lines))


# ---------------------------------------------------------------------------
# _Sidebar.md
# ---------------------------------------------------------------------------

def _build_sidebar(
    guide_entries: list[tuple[str, str]],
    scenario_entries: list[tuple[str, str]],
    app_entries: list[tuple[str, str]],
) -> None:
    lines: list[str] = [
        "**[[Home]]**\n\n",
        "**Guides**\n\n",
    ]
    for title, wiki_name in guide_entries:
        stem = Path(wiki_name).stem
        lines.append(f"- [[{stem}|{title}]]\n")

    lines += ["\n**Scenarios**\n\n"]
    for title, wiki_name in scenario_entries:
        stem = Path(wiki_name).stem
        lines.append(f"- [[{stem}|{title}]]\n")

    lines += ["\n**Spark Apps**\n\n"]
    for title, wiki_name in app_entries:
        stem = Path(wiki_name).stem
        lines.append(f"- [[{stem}|{title}]]\n")

    _write(WIKI_DIR / "_Sidebar.md", "".join(lines))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if WIKI_DIR.exists():
        shutil.rmtree(WIKI_DIR)
    WIKI_DIR.mkdir()

    guide_entries = _build_guides()
    scenario_entries = _build_scenarios()
    app_entries = _build_apps()

    _build_home(guide_entries, scenario_entries, app_entries)
    _build_sidebar(guide_entries, scenario_entries, app_entries)

    wiki_files = list(WIKI_DIR.glob("*.md"))
    print(f"wiki/ built: {len(wiki_files)} files")
    for f in sorted(wiki_files):
        print(f"  {f.name}")


if __name__ == "__main__":
    main()
