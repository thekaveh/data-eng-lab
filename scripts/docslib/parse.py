"""Parse docs/ + mkdocs.yml nav into a SiteModel."""
from __future__ import annotations

import re
from pathlib import Path

import yaml

from .links import DocMap
from .model import NavItem, Page, Scenario, Section, SiteModel

_H = re.compile(r"^(#{1,6})\s+(.*)$")
_NUM = re.compile(r"^(\d+(?:\.\d+)*)\.?\s+(.*)$")
_SLUG = re.compile(r"[^a-z0-9]+")


def _slug(text: str) -> str:
    return _SLUG.sub("-", text.lower()).strip("-")


def _sections(body: str) -> list[Section]:
    out: list[Section] = []
    for line in body.splitlines():
        m = _H.match(line)
        if not m:
            continue
        level = len(m.group(1))
        raw = m.group(2).strip()
        nm = _NUM.match(raw)
        number = nm.group(1) if nm else ""
        title = nm.group(2) if nm else raw
        out.append(Section(number=number, title=title, level=level, anchor=_slug(title)))
    return out


def _see_also(body: str) -> list[str]:
    in_block = False
    targets: list[str] = []
    for line in body.splitlines():
        s = line.strip()
        if re.match(r"^#+\s*See Also\s*$", s, re.I):
            in_block = True
            continue
        if in_block:
            if re.match(r"^#+\s", s):
                break
            for m in re.finditer(r"\[[^\]]+\]\(([^)]+)\)", s):
                targets.append(m.group(1))
    return targets


def _first_h1(body: str) -> str:
    for line in body.splitlines():
        m = re.match(r"^#\s+(.+)", line)
        if m:
            return m.group(1).strip()
    return ""


def _page(src: Path, docs_root: Path, number: str) -> Page:
    body = src.read_text(encoding="utf-8")
    rel = src.relative_to(docs_root.parent).as_posix()  # "docs/scenarios/foo.md"
    rel = rel[len("docs/"):]  # "scenarios/foo.md"
    return Page(src=Path(rel), title=_first_h1(body), number=number,
                body=body, sections=_sections(body), see_also=_see_also(body))


def _walk_nav(node, docs_root: Path, counter: list[int], pages: dict[str, Page],
              depth: int = 0) -> NavItem:
    # node is either {Title: <entry>} or a string "path.md"
    if isinstance(node, str):
        path = node
        title = Path(path).stem.replace("-", " ").title()
        entry = {title: path}
    else:
        entry = node
    (title, val), = entry.items()
    counter_at_depth = counter[depth] if depth < len(counter) else 0
    if depth == len(counter):
        counter.append(0)
    counter[depth] = counter_at_depth + 1
    number = ".".join(str(counter[i]) for i in range(depth + 1))
    children: list[NavItem] = []
    page: Page | None = None
    if isinstance(val, str):
        src = docs_root / val
        if src.exists():
            page = _page(src, docs_root, number)
            pages[val] = page
    elif isinstance(val, list):
        for child in val:
            children.append(_walk_nav(child, docs_root, counter, pages, depth + 1))
    return NavItem(number=number, title=title, page=page, children=children)


def parse_site(repo_root: Path) -> SiteModel:
    mkdocs = yaml.safe_load((repo_root / "mkdocs.yml").read_text(encoding="utf-8"))
    docs_root = repo_root / mkdocs.get("docs_dir", "docs")
    pages: dict[str, Page] = {}
    nav: list[NavItem] = []
    counter: list[int] = []
    for entry in mkdocs.get("nav", []):
        nav.append(_walk_nav(entry, docs_root, counter, pages, 0))
    # scenarios + apps derived from docs/scenarios|spark-apps/*.md that have a repo dir
    scenarios: list[Scenario] = []
    for md in sorted((docs_root / "scenarios").glob("*.md")):
        if md.name == "index.md":
            continue
        posix = md.relative_to(docs_root).as_posix()
        page = pages.get(posix) or _page(md, docs_root, "")
        pages[posix] = page
        name = md.stem
        sdir = repo_root / "scenarios" / name
        notebooks = {}
        if (sdir / "jupyter" / "notebook.ipynb").exists():
            notebooks["jupyter"] = sdir / "jupyter" / "notebook.ipynb"
        if (sdir / "zeppelin" / "notebook.zpln").exists():
            notebooks["zeppelin"] = sdir / "zeppelin" / "notebook.zpln"
        scenarios.append(Scenario(name=name, page=page, notebook_paths=notebooks))
    apps: list[Page] = []
    for md in sorted((docs_root / "spark-apps").glob("*.md")):
        if md.name == "index.md":
            continue
        posix = md.relative_to(docs_root).as_posix()
        page = pages.get(posix) or _page(md, docs_root, "")
        pages[posix] = page
        apps.append(page)
    return SiteModel(pages=pages, nav=nav, scenarios=scenarios, apps=apps)


def build_doc_map(model: SiteModel) -> DocMap:
    readme: dict[str, str] = {}
    wiki: dict[str, str] = {}
    for s in model.scenarios:
        posix = s.page.src.as_posix()
        readme[posix] = f"scenarios/{s.name}/README.md"
        wiki[posix] = f"Scenario-{s.name}"
        nbposix = f"notebooks/{s.name}.md"
        readme[nbposix] = f"scenarios/{s.name}/notebooks.md"
        wiki[nbposix] = f"Notebook-{s.name}"
    for a in model.apps:
        name = a.src.stem
        posix = a.src.as_posix()
        readme[posix] = f"spark-apps/{name}/README.md"
        wiki[posix] = f"App-{name}"
    return DocMap(readme=readme, wiki=wiki)
