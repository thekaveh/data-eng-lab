"""Render the in-repo README surface (self-contained, no cross-surface links)."""
from __future__ import annotations

import re
from pathlib import Path

from .links import rewrite_for_readme
from .model import Page, SiteModel
from .notebooks import extract_notebook_doc

AUTOGEN_HEADER = "<!-- AUTO-GENERATED — do not edit; run scripts/build_docs.py -->\n"
_LINK = re.compile(r"(!?)\[([^\]]*)\]\(([^)]+)\)")


def _rewrite_links(body: str, doc_posix: str, doc_map) -> str:
    def repl(m: re.Match) -> str:
        bang, text, target = m.group(1), m.group(2), m.group(3)
        new = rewrite_for_readme(target, doc_posix, doc_map)
        if new == "":
            return text  # dropped cross-surface link → plain text
        return f"{bang}[{text}]({new})"
    return _LINK.sub(repl, body)


def _scenario_readme(page: Page, doc_map) -> str:
    body = _rewrite_links(page.body, page.src.as_posix(), doc_map)
    return AUTOGEN_HEADER + body.rstrip() + "\n"


def _app_readme(page: Page, doc_map) -> str:
    return _scenario_readme(page, doc_map)


def _root_readme(model: SiteModel, doc_map) -> str:
    index = model.pages.get("index.md")
    head = index.body if index else "# data-eng-lab\n"
    head = _rewrite_links(head, "index.md", doc_map)
    lines = [AUTOGEN_HEADER, head.rstrip(), "\n\n## Scenario catalog\n\n",
             "| # | Scenario | Notebook doc |\n|---|---|---|\n"]
    for i, s in enumerate(model.scenarios, 1):
        lines.append(f"| {i} | [{s.page.title}](scenarios/{s.name}/README.md) | "
                     f"[notebooks](scenarios/{s.name}/notebooks.md) |\n")
    if model.apps:
        lines.append("\n## Spark Apps\n\n")
        for a in model.apps:
            lines.append(f"- [{a.title}](spark-apps/{a.src.stem}/README.md)\n")
    return "".join(lines)


def render_readme_surface(model: SiteModel, doc_map, repo_root: Path) -> dict[Path, str]:
    out: dict[Path, str] = {Path("README.md"): _root_readme(model, doc_map)}
    for s in model.scenarios:
        out[Path(f"scenarios/{s.name}/README.md")] = _scenario_readme(s.page, doc_map)
        if "jupyter" in s.notebook_paths and "zeppelin" in s.notebook_paths:
            md = extract_notebook_doc(s.name, s.notebook_paths["jupyter"], s.notebook_paths["zeppelin"])
            out[Path(f"scenarios/{s.name}/notebooks.md")] = AUTOGEN_HEADER + md
    for a in model.apps:
        out[Path(f"spark-apps/{a.src.stem}/README.md")] = _app_readme(a, doc_map)
    return out
