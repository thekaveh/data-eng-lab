"""Render the GitHub Wiki surface (self-contained, no .io banner)."""
from __future__ import annotations

import re
from pathlib import Path

from .links import rewrite_for_wiki
from .model import Page, SiteModel
from .notebooks import extract_notebook_doc

AUTOGEN_HEADER = "<!-- AUTO-GENERATED — do not edit; run scripts/build_docs.py -->\n"
_LINK = re.compile(r"(!?)\[([^\]]*)\]\(([^)]+)\)")
# docs concept page → wiki page filename
_CONCEPT_WIKI = {
    "index.md": "Home.md", "getting-started.md": "Getting-Started.md",
    "lakehouse.md": "Lakehouse.md", "datasets.md": "Datasets.md",
    "CHANGELOG.md": "Changelog.md",
    "go-live.md": "Go-Live-Runbook.md", "go-live-results.md": "Go-Live-Results.md",
    "atlas-expectations.md": "Atlas-Expectations.md",
    "atlas-enablement.md": "Atlas-Enablement.md",
    "atlas-feedback-a7a9.md": "Atlas-Feedback-A7-A9.md",
    "atlas-feedback-go-live.md": "Atlas-Feedback-Go-Live.md",
}


def _rewrite_links(body: str, doc_posix: str, doc_map) -> str:
    def repl(m: re.Match) -> str:
        bang, text, target = m.group(1), m.group(2), m.group(3)
        new = rewrite_for_wiki(target, doc_posix, doc_map)
        if new == "":
            return text
        if new.startswith("[["):
            return new  # wiki-link replaces whole [text](target)
        return f"{bang}[{text}]({new})"
    return _LINK.sub(repl, body)


def _page_body(page: Page, doc_map) -> str:
    return AUTOGEN_HEADER + _rewrite_links(page.body, page.src.as_posix(), doc_map).rstrip() + "\n"


def _home(model: SiteModel, doc_map) -> str:
    index = model.pages.get("index.md")
    body = index.body if index else "# data-eng-lab\n"
    lines = [AUTOGEN_HEADER, _rewrite_links(body, "index.md", doc_map).rstrip(),
             "\n\n## Scenarios\n\n"]
    for s in model.scenarios:
        lines.append(f"- [[Scenario-{s.name}|{s.page.title}]]\n")
    if model.apps:
        lines.append("\n## Spark Apps\n\n")
        for a in model.apps:
            lines.append(f"- [[App-{a.src.stem}|{a.title}]]\n")
    return "".join(lines)


def _sidebar(model: SiteModel) -> str:
    lines = ["**[[Home]]**\n\n**Guides**\n",
             "- [[Getting-Started]]\n- [[Lakehouse]]\n- [[Datasets]]\n",
             "\n**Scenarios**\n"]
    for s in model.scenarios:
        lines.append(f"- [[Scenario-{s.name}]]\n")
    lines.append("\n**Notebooks**\n")
    for s in model.scenarios:
        lines.append(f"- [[Notebook-{s.name}]]\n")
    if model.apps:
        lines.append("\n**Spark Apps**\n")
        for a in model.apps:
            lines.append(f"- [[App-{a.src.stem}]]\n")
    lines.append("\n**Atlas**\n- [[Atlas-Expectations]]\n- [[Go-Live-Runbook]]\n"
                 "- [[Go-Live-Results]]\n- [[Atlas-Feedback-A7-A9]]\n"
                 "- [[Atlas-Feedback-Go-Live]]\n- [[Atlas-Enablement]]\n")
    return "".join(lines)


def render_wiki_surface(model: SiteModel, doc_map, repo_root: Path) -> dict[Path, str]:
    out: dict[Path, str] = {Path("Home.md"): _home(model, doc_map),
                            Path("_Sidebar.md"): _sidebar(model)}
    for posix, page in model.pages.items():
        if posix in _CONCEPT_WIKI and posix != "index.md":
            out[Path(_CONCEPT_WIKI[posix])] = _page_body(page, doc_map)
    for s in model.scenarios:
        out[Path(f"Scenario-{s.name}.md")] = _page_body(s.page, doc_map)
        if "jupyter" in s.notebook_paths and "zeppelin" in s.notebook_paths:
            md = extract_notebook_doc(s.name, s.notebook_paths["jupyter"], s.notebook_paths["zeppelin"])
            out[Path(f"Notebook-{s.name}.md")] = AUTOGEN_HEADER + md
    for a in model.apps:
        out[Path(f"App-{a.src.stem}.md")] = _page_body(a, doc_map)
    return out
