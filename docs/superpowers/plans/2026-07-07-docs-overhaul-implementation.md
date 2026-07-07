# Documentation Overhaul Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Re-architect `data-eng-lab` docs into three self-contained, synced surfaces (in-repo READMEs, GitHub Wiki, `.io` MkDocs site) generated from `docs/` as the single source of truth — with embedded landscape SVG diagrams, 19 auto-extracted notebook docs, an Orbital/sci-fi theme, and CI that enforces sync.

**Architecture:** `docs/` is authored by humans and renders the `.io` site. A new `scripts/build_docs.py` orchestrator (backed by a `scripts/docslib/` package) parses `docs/` and the `mkdocs.yml` nav into a `SiteModel`, then renders self-contained READMEs and Wiki pages with **zero cross-surface links** and **embedded (locally-copied) SVG diagrams**. A `check_surfaces.py` CI assertion and `build_docs.py --check` dry-run enforce the invariants. Diagrams are generated as landscape SVG via the `architecture-diagram` skill into a canonical `docs/architectures/` location.

**Tech Stack:** Python 3.11, `uv` (group `dev`), MkDocs Material, PyYAML, nbformat, pytest, ruff (line-length 120), GitHub Actions.

## Global Constraints

Copied verbatim from the spec (`docs/superpowers/specs/2026-07-07-docs-overhaul-design.md`); every task's requirements implicitly include these:

- **No cross-surface links.** No `.io` URLs (`thekaveh.github.io/data-eng-lab`) in any README or wiki page; no `docs/`-relative paths in any README; no repo URLs in any wiki page; no banners.
- **Diagrams embedded, not linked.** Each surface holds its own copy of every SVG it uses. All diagrams are **landscape** SVG, script-free (so GitHub renders them via `<img>`).
- **Generated files are output-only.** READMEs and wiki pages start with an `<!-- AUTO-GENERATED — do not edit; run scripts/build_docs.py -->` comment.
- **infra/ submodule is out of scope.** Never modify `infra/`; ruff already excludes `./infra/**`.
- **Tests are offline.** All new tests must pass under `uv run pytest -m "not infra and not network" -q`. Lint must pass `uv run ruff check .`.
- **Scenario README template is the 8-section form:** `1. Purpose`, `2. Data Model`, `3. Architecture`, `4. Notebooks`, `5. Orchestration`, `6. Usage`, `7. Dependencies`, `8. Known Issues & Caveats` (+ `See Also`). This must match `verify_repo_config.yaml`.
- **Run commands from repo root** unless a step says otherwise. Python via `uv run --group dev python …`.

---

## File Structure

### New files (engine — `scripts/docslib/`)
- `scripts/docslib/__init__.py` — package marker.
- `scripts/docslib/model.py` — dataclasses: `Section`, `Page`, `Scenario`, `NavItem`, `SiteModel`.
- `scripts/docslib/links.py` — link classification (`classify`) + per-surface rewriters (`rewrite_for_readme`, `rewrite_for_wiki`) + `DocMap` (docs-page → README path / wiki slug). The heart of the no-cross-link rule.
- `scripts/docslib/parse.py` — parse `docs/` + `mkdocs.yml` nav into `SiteModel`; extract sections + See-Also.
- `scripts/docslib/notebooks.py` — parse `.ipynb` (nbformat) + `.zpln` (JSON paragraphs), align by numbered section, emit notebook-doc markdown.
- `scripts/docslib/render_readme.py` — render root `README.md`, per-scenario `README.md` + `notebooks.md`, per-app `README.md` from `SiteModel`.
- `scripts/docslib/render_wiki.py` — render `wiki/*.md` (Home, Scenario-, Notebook-, App-, concepts, _Sidebar) from `SiteModel`, no banner.
- `scripts/docslib/assets.py` — copy canonical SVGs into scenario/app dirs (README surface) and into a wiki clone dir.

### New files (entry points + CI)
- `scripts/build_docs.py` — CLI orchestrator: `parse → render_readme → render_wiki → assets`; flags `--check`, `--stage {readme,wiki,assets,all}`.
- `scripts/check_surfaces.py` — CI assertion: no cross-surface links; every diagram embedded locally; every nav page has a counterpart in all three surfaces.
- `scripts/check_diagrams.py` — asserts every canonical SVG exists, is landscape, script-free.
- `scripts/diagrams_manifest.yaml` — declarative inputs for each diagram (title, kind, sources, output path) consumed by diagram generation + checks.
- `.github/workflows/docs-sync.yml` — on push to main: run `build_docs.py`, commit regenerated READMEs, push `wiki/` to `*.wiki.git`.

### New files (theme + content)
- `docs/overrides/main.html` — Material override: loads fonts, injects the corner-glow background layer.
- `docs/css/custom.css` — **replaces** the current file; Orbital palette (flat dark + faint cyan corner glow, no grid; cyan accent; Space Grotesk / IBM Plex Mono / Inter).
- `docs/notebooks/index.md` — Notebooks section landing page (links each scenario's notebook doc).
- `docs/architectures/*.svg` — canonical landscape diagrams (overview, lakehouse, 19 scenarios, 2 apps).

### Modified files
- `mkdocs.yml` — Orbital palette/fonts; numbered `nav:` per spec §4 (add Notebooks section); remove `gen-files` + `literate-nav` plugins and `SUMMARY.md` reference; repoint `extra_css`.
- `scripts/verify_repo_config.yaml` — `scenario_readme_sections` → the 8-section template (fixes the CI gate).
- `tests/test_verify_repo.py` — `CFG["scenario_readme_sections"]` → the 8-section template.
- `.github/workflows/ci.yml` — `docs-build` job: replace `mkdocs build --strict` only step with `mkdocs build --strict` **+** `build_docs.py --check` **+** `check_surfaces.py`.
- `.github/workflows/docs-deploy.yml` — remove the dead `render_readme.py` step (README regen moves to `docs-sync.yml`); keep Pages build/deploy.
- All 19 `docs/scenarios/*.md` + 2 `docs/spark-apps/*.md` — diagram refs repointed to `docs/architectures/...svg`; concept cross-links normalized (content otherwise unchanged).
- Root `README.md`, `scenarios/*/README.md`, `scenarios/*/notebooks.md`, `spark-apps/*/README.md` — regenerated (output-only).

### Deleted files
- `scripts/gen_doc_pages.py`, `scripts/render_readme.py`, `scripts/build_wiki.py` (logic subsumed into `docslib/`).
- `docs/scenarios/architectures/*.html`, `docs/spark-apps/architectures/*.html`, `docs/architecture.html`, `docs/lakehouse-architecture.html` (replaced by canonical SVGs in `docs/architectures/`).

### New tests (all under `tests/scripts/docslib/` + `tests/scripts/`)
- `tests/scripts/docslib/__init__.py`
- `tests/scripts/docslib/test_links.py`, `test_parse.py`, `test_notebooks.py`, `test_render_readme.py`, `test_render_wiki.py`
- `tests/scripts/test_build_docs.py`, `tests/scripts/test_check_surfaces.py`, `tests/scripts/test_check_diagrams.py`
- `tests/scripts/_fixtures/` — a tiny synthetic docs/scenarios/wiki tree for integration tests.

---

## Task 1: `docslib` package + `model.py`

**Files:**
- Create: `scripts/docslib/__init__.py`, `scripts/docslib/model.py`, `conftest.py` (root — puts `scripts/` on `sys.path` so tests and `build_docs.py` both import `docslib.…` uniformly)
- Test: `tests/scripts/docslib/__init__.py`, `tests/scripts/docslib/test_model.py`

**Interfaces:**
- Produces: dataclasses `Section`, `Page`, `Scenario`, `NavItem`, `SiteModel` (signatures below) — used by every later `docslib` module.

- [ ] **Step 1: Write the failing test**

`tests/scripts/docslib/test_model.py`:
```python
from pathlib import Path
from docslib.model import Page, Scenario, Section, SiteModel, NavItem


def test_section_defaults():
    s = Section(number="2.1", title="Input", level=2, anchor="input")
    assert s.number == "2.1" and s.title == "Input"


def test_page_roundtrip():
    p = Page(src=Path("scenarios/foo.md"), title="foo", number="5.3",
             body="# foo\n", sections=[Section("1", "Purpose", 2, "purpose")],
             see_also=[])
    assert p.title == "foo" and p.sections[0].anchor == "purpose"


def test_navitem_children_default_empty():
    n = NavItem(number="5", title="Scenarios", page=None)
    assert n.children == []


def test_sitemodel_holds_collections():
    sm = SiteModel(pages={}, nav=[], scenarios=[], apps=[])
    assert sm.pages == {} and sm.scenarios == []
```

`tests/scripts/docslib/__init__.py` and `tests/scripts/__init__.py` already exist — leave them.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/scripts/docslib/test_model.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'scripts.docslib'`.

- [ ] **Step 3: Write minimal implementation**

`scripts/docslib/__init__.py`:
```python
"""docs/ → README + Wiki generation library for data-eng-lab."""
```

`conftest.py` (repo root — lets every test import `docslib.…` the same way `build_docs.py` does):
```python
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "scripts"))
```

`scripts/docslib/model.py`:
```python
"""In-memory model of the docs/ source tree + mkdocs nav."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Section:
    number: str        # "1", "2.1", or "" if unnumbered
    title: str
    level: int         # 1 = H1, 2 = H2, ...
    anchor: str        # url slug


@dataclass
class Page:
    src: Path                  # docs/-relative path, e.g. Path("scenarios/batch_ingest-...md")
    title: str                 # first H1 text
    number: str                # nav number, e.g. "5.3"; "" if unnumbered
    body: str                  # raw markdown
    sections: list[Section]
    see_also: list[str]        # raw link targets from the "See Also" block


@dataclass
class Scenario:
    name: str                  # dir name under scenarios/
    page: Page                 # docs/scenarios/<name>.md
    notebook_paths: dict[str, Path]  # {"jupyter": ..., "zeppelin": ...}


@dataclass
class NavItem:
    number: str
    title: str
    page: Page | None
    children: list["NavItem"] = field(default_factory=list)


@dataclass
class SiteModel:
    pages: dict[str, Page]     # keyed by docs-relative posix path (e.g. "scenarios/foo.md")
    nav: list[NavItem]
    scenarios: list[Scenario]
    apps: list[Page]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/scripts/docslib/test_model.py -q`
Expected: PASS (4 passed).

- [ ] **Step 5: Commit**

```bash
git add conftest.py scripts/docslib/__init__.py scripts/docslib/model.py tests/scripts/docslib/test_model.py
git commit -m "feat(docs): add docslib package + SiteModel dataclasses + root conftest"
```

---

## Task 2: `docslib/links.py` — link classification + per-surface rewriters

This module enforces the **no-cross-surface-links** rule. It is the most logic-heavy unit; test it exhaustively.

**Files:**
- Create: `scripts/docslib/links.py`
- Test: `tests/scripts/docslib/test_links.py`

**Interfaces:**
- Consumes: nothing (pure).
- Produces:
  - `LinkKind` enum: `EXTERNAL_SITE`, `EXTERNAL_OTHER`, `DIAGRAM`, `DOC_RELATIVE`, `ANCHOR`, `MAILTO`, `BARE`.
  - `SITE_URL_PREFIX = "https://thekaveh.github.io/data-eng-lab"` (constant).
  - `classify(target: str) -> LinkKind`
  - `DocMap` dataclass: `readme: dict[str, str]` (docs posix path → repo-relative README path), `wiki: dict[str, str]` (docs posix path → wiki slug), `concepts_readme_anchor: dict[str, str]` (docs posix path → root-README anchor).
  - `rewrite_for_readme(target: str, current_doc_posix: str, doc_map: DocMap) -> str`
  - `rewrite_for_wiki(target: str, current_doc_posix: str, doc_map: DocMap) -> str`

- [ ] **Step 1: Write the failing test**

`tests/scripts/docslib/test_links.py`:
```python
import pytest
from docslib.links import (DocMap, LinkKind, classify,
                                   rewrite_for_readme, rewrite_for_wiki)


def _map():
    return DocMap(
        readme={"scenarios/batch_ingest-nyc_taxi-spark-iceberg.md":
                "scenarios/batch_ingest-nyc_taxi-spark-iceberg/README.md",
                "spark-apps/nyc-taxi-etl.md": "spark-apps/nyc-taxi-etl/README.md"},
        wiki={"scenarios/batch_ingest-nyc_taxi-spark-iceberg.md": "Scenario-batch_ingest-nyc_taxi-spark-iceberg",
              "spark-apps/nyc-taxi-etl.md": "App-nyc-taxi-etl"},
        concepts_readme_anchor={"datasets.md": "datasets",
                                "lakehouse.md": "lakehouse-architecture"},
    )


@pytest.mark.parametrize("target,kind", [
    ("https://thekaveh.github.io/data-eng-lab/scenarios/foo/", LinkKind.EXTERNAL_SITE),
    ("http://example.com", LinkKind.EXTERNAL_OTHER),
    ("https://github.com/thekaveh/atlas", LinkKind.EXTERNAL_OTHER),
    ("architectures/foo.svg", LinkKind.DIAGRAM),
    ("architectures/foo.html", LinkKind.DIAGRAM),
    ("../bar.md", LinkKind.DOC_RELATIVE),
    ("datasets.md", LinkKind.DOC_RELATIVE),
    ("#anchor", LinkKind.ANCHOR),
    ("mailto:a@b.com", LinkKind.MAILTO),
])
def test_classify(target, kind):
    assert classify(target) == kind


def test_readme_rewrites_internal_and_localizes_diagram():
    m = _map()
    cur = "scenarios/batch_ingest-nyc_taxi-spark-iceberg.md"
    # doc-relative sibling → sibling README (internal)
    assert rewrite_for_readme("medallion-nyc_taxi-spark-iceberg.md", cur, m) == \
        "../medallion-nyc_taxi-spark-iceberg/README.md"
    # concept → root README anchor
    assert rewrite_for_readme("datasets.md", cur, m) == "../../README.md#datasets"
    # diagram → local svg (relative to the scenario README's dir)
    assert rewrite_for_readme("architectures/batch_ingest-nyc_taxi-spark-iceberg.svg", cur, m) == \
        "architectures/batch_ingest-nyc_taxi-spark-iceberg.svg"
    # site URL must NOT appear
    assert "thekaveh.github.io" not in \
        rewrite_for_readme("https://thekaveh.github.io/data-eng-lab/", cur, m)


def test_readme_rejects_site_url():
    m = _map()
    out = rewrite_for_readme("https://thekaveh.github.io/data-eng-lab/foo/", "index.md", m)
    assert out == ""  # cross-surface link dropped


def test_wiki_uses_wiki_links():
    m = _map()
    cur = "scenarios/batch_ingest-nyc_taxi-spark-iceberg.md"
    assert rewrite_for_wiki("medallion-nyc_taxi-spark-iceberg.md", cur, m) == \
        "[[Scenario-medallion-nyc_taxi-spark-iceberg]]"
    assert rewrite_for_wiki("datasets.md", cur, m) == "[[Datasets]]"
    assert rewrite_for_wiki("architectures/batch_ingest-nyc_taxi-spark-iceberg.svg", cur, m) == \
        "batch_ingest-nyc_taxi-spark-iceberg.svg"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/scripts/docslib/test_links.py -q`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Write minimal implementation**

`scripts/docslib/links.py`:
```python
"""Link classification + per-surface rewriting (enforces no cross-surface links)."""
from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum

SITE_URL_PREFIX = "https://thekaveh.github.io/data-eng-lab"

# docs/ concept pages render as sections of the root README in the in-repo surface.
# Map: docs posix path → (README anchor, wiki page slug).
_CONCEPTS = {
    "index.md": ("overview", "Home"),
    "getting-started.md": ("getting-started", "Getting-Started"),
    "lakehouse.md": ("lakehouse-architecture", "Lakehouse"),
    "datasets.md": ("datasets", "Datasets"),
    "CHANGELOG.md": ("changelog", "Changelog"),
}


class LinkKind(Enum):
    EXTERNAL_SITE = "external_site"      # the .io site — FORBIDDEN in derived surfaces
    EXTERNAL_OTHER = "external_other"    # genuinely external — keep
    DIAGRAM = "diagram"
    DOC_RELATIVE = "doc_relative"        # a docs/ markdown page
    ANCHOR = "anchor"
    MAILTO = "mailto"
    BARE = "bare"


@dataclass
class DocMap:
    readme: dict[str, str]               # docs posix path → repo-relative README path
    wiki: dict[str, str]                 # docs posix path → wiki slug
    concepts_readme_anchor: dict[str, str] = None  # type: ignore[assignment]

    def __post_init__(self):
        if self.concepts_readme_anchor is None:
            self.concepts_readme_anchor = {k: v[0] for k, v in _CONCEPTS.items()}


def classify(target: str) -> LinkKind:
    t = target.strip()
    if t.startswith("#"):
        return LinkKind.ANCHOR
    if t.startswith("mailto:"):
        return LinkKind.MAILTO
    if t.startswith(("http://", "https://")):
        return LinkKind.EXTERNAL_SITE if t.startswith(SITE_URL_PREFIX) else LinkKind.EXTERNAL_OTHER
    if "/architectures/" in t or t.startswith("architectures/") or "architecture.html" in t:
        return LinkKind.DIAGRAM
    if t.endswith(".md") or "/" in t:
        return LinkKind.DOC_RELATIVE
    return LinkKind.BARE


def _diagram_basename(target: str) -> str:
    base = target.split("/")[-1]
    return re.sub(r"\.(html|svg)$", ".svg", base)


def _resolve_doc_relative(target: str, current_doc_posix: str) -> str:
    """Resolve a relative markdown target to a docs/-posix path."""
    if "/" not in target and not target.startswith("."):
        return target  # already a docs-root-relative posix like "datasets.md"
    # relative to current doc's directory
    cur_dir = "/".join(current_doc_posix.split("/")[:-1])
    parts = (cur_dir + "/" + target).split("/") if cur_dir else target.split("/")
    stack: list[str] = []
    for p in parts:
        if p == "" or p == ".":
            continue
        if p == "..":
            if stack:
                stack.pop()
            continue
        stack.append(p)
    return "/".join(stack)


def rewrite_for_readme(target: str, current_doc_posix: str, doc_map: DocMap) -> str:
    kind = classify(target)
    if kind in (LinkKind.ANCHOR, LinkKind.MAILTO):
        return target
    if kind == LinkKind.EXTERNAL_SITE:
        return ""  # cross-surface link — drop
    if kind == LinkKind.EXTERNAL_OTHER:
        return target
    if kind == LinkKind.DIAGRAM:
        return _diagram_basename(target)  # local copy in <surface-dir>/architectures/
    # DOC_RELATIVE
    doc_posix = _resolve_doc_relative(target, current_doc_posix)
    if doc_posix in doc_map.readme:
        return _relpath_between_docs(current_doc_posix, doc_map.readme[doc_posix])
    if doc_posix in _CONCEPTS:
        anchor = _CONCEPTS[doc_posix][0]
        return _relpath_to_root_readme(current_doc_posix, anchor)
    return ""  # unknown doc target — drop rather than risk a cross-surface link


def rewrite_for_wiki(target: str, current_doc_posix: str, doc_map: DocMap) -> str:
    kind = classify(target)
    if kind in (LinkKind.ANCHOR, LinkKind.MAILTO):
        return target
    if kind == LinkKind.EXTERNAL_SITE:
        return ""  # cross-surface link — drop
    if kind == LinkKind.EXTERNAL_OTHER:
        return target
    if kind == LinkKind.DIAGRAM:
        return _diagram_basename(target)  # svg copied into wiki repo root
    doc_posix = _resolve_doc_relative(target, current_doc_posix)
    if doc_posix in doc_map.wiki:
        return f"[[{doc_map.wiki[doc_posix]}]]"
    if doc_posix in _CONCEPTS:
        return f"[[{_CONCEPTS[doc_posix][1]}]]"
    return ""


def _relpath_between_docs(current_doc_posix: str, target_readme: str) -> str:
    """Relative path from the README mirroring current_doc_posix to target_readme."""
    cur_readme_dir = _readme_dir_for(current_doc_posix)
    return _relpath(cur_readme_dir, target_readme)


def _readme_dir_for(doc_posix: str) -> str:
    # scenarios/foo.md → scenarios/foo ; spark-apps/bar.md → spark-apps/bar ; else ""
    if "/" in doc_posix:
        parent, name = doc_posix.rsplit("/", 1)
        stem = name[:-3]  # strip .md
        if parent in ("scenarios", "spark-apps"):
            return f"{parent}/{stem}"
    return ""


def _relpath_to_root_readme(current_doc_posix: str, anchor: str) -> str:
    cur_readme_dir = _readme_dir_for(current_doc_posix)
    up = "../" * (cur_readme_dir.count("/") + 1) if cur_readme_dir else ""
    return f"{up}README.md#{anchor}"


def _relpath(src_dir: str, dst: str) -> str:
    src_parts = src_dir.split("/") if src_dir else []
    dst_parts = dst.split("/")
    up = [".."] * len(src_parts)
    return "/".join(up + dst_parts) if up else dst
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/scripts/docslib/test_links.py -q`
Expected: PASS (all parametrized + 3 others).

- [ ] **Step 5: Commit**

```bash
git add scripts/docslib/links.py tests/scripts/docslib/test_links.py
git commit -m "feat(docs): docslib.links — classify + per-surface rewrite (no cross-surface links)"
```

---

## Task 3: `docslib/parse.py` — parse `docs/` + nav into `SiteModel`

**Files:**
- Create: `scripts/docslib/parse.py`
- Test: `tests/scripts/docslib/test_parse.py`, fixture under `tests/scripts/_fixtures/`

**Interfaces:**
- Consumes: `model.py`, `links.py` (`DocMap`).
- Produces: `parse_site(repo_root: Path) -> SiteModel`, `build_doc_map(model: SiteModel) -> DocMap`.

- [ ] **Step 1: Create the fixture**

Create `tests/scripts/_fixtures/repo/docs/index.md`:
```markdown
# data-eng-lab
Overview text.
## Quick Start
\`\`\`bash
make up
\`\`\`
```
(use real backticks; the plan shows escaped backticks only for readability.)

Create `tests/scripts/_fixtures/repo/docs/datasets.md`:
```markdown
# Datasets
Five datasets.
## NYC Taxi
text
```
Create `tests/scripts/_fixtures/repo/docs/scenarios/batch_ingest-nyc_taxi-spark-iceberg.md`:
```markdown
# batch_ingest-nyc_taxi-spark-iceberg
Lede.
## 1. Purpose
p
## 8. Known Issues & Caveats
p
## See Also
- [medallion-nyc_taxi-spark-iceberg](medallion-nyc_taxi-spark-iceberg.md)
- [Datasets](../datasets.md)
```
Create `tests/scripts/_fixtures/repo/mkdocs.yml`:
```yaml
site_name: data-eng-lab
docs_dir: docs
nav:
  - Home: index.md
  - Datasets: datasets.md
  - Scenarios:
    - Overview: scenarios/index.md
    - batch_ingest: scenarios/batch_ingest-nyc_taxi-spark-iceberg.md
```
Create empty `tests/scripts/_fixtures/repo/docs/scenarios/index.md` (`# Scenarios`).
Create `tests/scripts/_fixtures/repo/scenarios/batch_ingest-nyc_taxi-spark-iceberg/README.md` (any content — only existence matters for scenario discovery) and `zeppelin/notebook.zpln` + `jupyter/notebook.ipynb` as minimal valid JSON (`{"paragraphs":[]}` / `{"cells":[],"nbformat":4,"nbformat_minor":5}`).

- [ ] **Step 2: Write the failing test**

`tests/scripts/docslib/test_parse.py`:
```python
from pathlib import Path
from docslib.parse import parse_site, build_doc_map

FIX = Path(__file__).resolve().parents[1] / "_fixtures" / "repo"


def test_parse_builds_pages_and_scenarios():
    sm = parse_site(FIX)
    key = "scenarios/batch_ingest-nyc_taxi-spark-iceberg.md"
    assert key in sm.pages
    page = sm.pages[key]
    assert page.title == "batch_ingest-nyc_taxi-spark-iceberg"
    nums = {s.number for s in page.sections}
    assert "1" in nums and "8" in nums
    assert "medallion-nyc_taxi-spark-iceberg.md" in page.see_also
    assert len(sm.scenarios) == 1
    assert sm.scenarios[0].name == "batch_ingest-nyc_taxi-spark-iceberg"


def test_nav_numbers_assigned():
    sm = parse_site(FIX)
    titles = {n.title: n.number for n in sm.nav}
    assert titles["Datasets"] == "2"


def test_doc_map_readme_and_wiki():
    sm = parse_site(FIX)
    dm = build_doc_map(sm)
    k = "scenarios/batch_ingest-nyc_taxi-spark-iceberg.md"
    assert dm.readme[k].endswith("/README.md")
    assert dm.wiki[k].startswith("Scenario-")
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/scripts/docslib/test_parse.py -q`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 4: Write minimal implementation**

`scripts/docslib/parse.py`:
```python
"""Parse docs/ + mkdocs.yml nav into a SiteModel."""
from __future__ import annotations

import re
from pathlib import Path

import yaml

from .links import DocMap
from .model import NavItem, Page, Scenario, Section

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
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/scripts/docslib/test_parse.py -q`
Expected: PASS (3 passed).

- [ ] **Step 6: Commit**

```bash
git add scripts/docslib/parse.py tests/scripts/docslib/test_parse.py tests/scripts/_fixtures/
git commit -m "feat(docs): docslib.parse — build SiteModel + DocMap from docs/ and mkdocs.yml"
```

---

## Task 4: `docslib/notebooks.py` — auto-extract a notebook doc

**Files:**
- Create: `scripts/docslib/notebooks.py`
- Test: `tests/scripts/docslib/test_notebooks.py`, fixtures `tests/scripts/_fixtures/nb/`

**Interfaces:**
- Consumes: nothing beyond stdlib + `nbformat`.
- Produces: `extract_notebook_doc(scenario_name, jupyter_path, zeppelin_path) -> str` (markdown), `NOTEBOOK_SECTIONS = ["1. Overview","2. Setup","3. Read","4. Transform","5. Write","6. Verify"]`.

- [ ] **Step 1: Create fixtures**

`tests/scripts/_fixtures/nb/jupyter/notebook.ipynb` — a real 4.5 notebook with markdown + code cells titled `## 1. Overview`, `## 2. Setup`, `## 3. Read`, `## 4. Transform`, `## 5. Write`, `## 6. Verify` (use `nbformat` to write it in the test setup, or commit a static one). Static minimal:
```json
{"cells":[{"cell_type":"markdown","source":["## 1. Overview\n"]},{"cell_type":"code","source":["spark = 1\n"]},{"cell_type":"markdown","source":["## 2. Setup\n"]}],"nbformat":4,"nbformat_minor":5}
```
`tests/scripts/_fixtures/nb/zeppelin/notebook.zpln`:
```json
{"paragraphs":[{"title":"1. Overview","text":"%md\n## 1. Overview"},{"title":"2. Setup","text":"%spark\nval s = 1"}]}
```

- [ ] **Step 2: Write the failing test**

`tests/scripts/docslib/test_notebooks.py`:
```python
from pathlib import Path
from docslib.notebooks import extract_notebook_doc, NOTEBOOK_SECTIONS

FIX = Path(__file__).resolve().parents[1] / "_fixtures" / "nb"


def test_extracts_sections_and_code():
    md = extract_notebook_doc("foo", FIX / "jupyter" / "notebook.ipynb",
                              FIX / "zeppelin" / "notebook.zpln")
    assert "# Notebooks — foo" in md
    assert "## 1. Overview" in md
    assert "## 2. Section map" in md
    assert "## 4. Scala / PySpark parity" in md
    # both languages' code present
    assert "spark = 1" in md
    assert "val s = 1" in md


def test_known_section_list():
    assert NOTEBOOK_SECTIONS[0] == "1. Overview"
    assert len(NOTEBOOK_SECTIONS) == 6
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/scripts/docslib/test_notebooks.py -q`
Expected: FAIL.

- [ ] **Step 4: Write minimal implementation**

`scripts/docslib/notebooks.py`:
```python
"""Auto-extract a per-scenario notebook doc from the .ipynb + .zpln."""
from __future__ import annotations

import json
import re
from pathlib import Path

import nbformat

NOTEBOOK_SECTIONS = ["1. Overview", "2. Setup", "3. Read", "4. Transform", "5. Write", "6. Verify"]
_H = re.compile(r"^##\s+(\d+\.\s+.*)$")


def _ipython_cells(path: Path) -> list[tuple[str, str, str]]:
    """Return [(section_title, cell_type, source_text)] grouped under ## N. headers."""
    nb = nbformat.read(path, as_version=4)
    out: list[tuple[str, str, str]] = []
    cur = "0. Preamble"
    for c in nb.cells:
        if c.cell_type == "markdown":
            src = "".join(c.source) if isinstance(c.source, list) else c.source
            m = _H.match(src.strip())
            if m:
                cur = m.group(1).strip()
            out.append((cur, "markdown", src))
        elif c.cell_type == "code":
            src = "".join(c.source) if isinstance(c.source, list) else c.source
            out.append((cur, "code", src))
    return out


def _zeppelin_cells(path: Path) -> list[tuple[str, str, str]]:
    doc = json.loads(path.read_text(encoding="utf-8"))
    out: list[tuple[str, str, str]] = []
    cur = "0. Preamble"
    for p in doc.get("paragraphs", []):
        text = p.get("text", "")
        title = p.get("title", "")
        m = _H.match(text.strip())
        if m:
            cur = m.group(1).strip()
        lang = "markdown" if text.lstrip().startswith("%md") else "code"
        out.append((cur, lang, text))
    return out


def _strip_interp(text: str) -> str:
    # drop zeppelin %md / %spark / %trino leading directives
    return re.sub(r"^(%\w+\s*)+", "", text).strip()


def extract_notebook_doc(scenario_name: str, jupyter_path: Path, zeppelin_path: Path) -> str:
    py = _ipython_cells(jupyter_path)
    sc = _zeppelin_cells(zeppelin_path)
    sections_seen: dict[str, dict[str, str]] = {}
    for sec, lang, src in py:
        sections_seen.setdefault(sec, {"py_md": "", "py_code": ""})
        key = "py_md" if lang == "markdown" else "py_code"
        if src.strip():
            sections_seen[sec][key] += src.rstrip() + "\n"
    for sec, lang, src in sc:
        sections_seen.setdefault(sec, {"py_md": "", "py_code": "", "sc_md": "", "sc_code": ""})
        clean = _strip_interp(src)
        key = "sc_md" if lang == "markdown" else "sc_code"
        if clean:
            sections_seen[sec].setdefault(key, "")
            sections_seen[sec][key] += clean + "\n"

    lines = [f"# Notebooks — {scenario_name}\n",
             f"Auto-extracted from `jupyter/notebook.ipynb` and `zeppelin/notebook.zpln`.\n",
             "Both notebooks implement identical logic in PySpark and Scala.\n\n",
             "## 2. Section map\n\n",
             "| Section | Scala (Zeppelin) | PySpark (Jupyter) |\n|---|---|---|\n"]
    for sec in NOTEBOOK_SECTIONS:
        has_sc = "✓" if any(s == sec and (v.get("sc_md") or v.get("sc_code")) for s, v in sections_seen.items()) else "—"
        has_py = "✓" if any(s == sec and (v.get("py_md") or v.get("py_code")) for s, v in sections_seen.items()) else "—"
        lines.append(f"| {sec} | {has_sc} | {has_py} |\n")
    lines.append("\n## 3. Walkthrough\n\n")
    for sec in NOTEBOOK_SECTIONS:
        v = sections_seen.get(sec)
        if not v:
            continue
        lines.append(f"### {sec}\n\n")
        sc_code = v.get("sc_code", "").strip()
        py_code = v.get("py_code", "").strip()
        if sc_code or py_code:
            lines.append("**Scala (Zeppelin):**\n\n```scala\n" + sc_code + "\n```\n\n")
            lines.append("**PySpark (Jupyter):**\n\n```python\n" + py_code + "\n```\n\n")
        sc_md = v.get("sc_md", "").strip()
        py_md = v.get("py_md", "").strip()
        if sc_md:
            lines.append(sc_md + "\n\n")
        elif py_md:
            lines.append(py_md + "\n\n")
    lines.append("## 4. Scala / PySpark parity\n\n")
    lines.append("Both notebooks share the same numbered sections and produce identical Iceberg tables; "
                 "only the language and interpreter differ.\n\n")
    lines.append("## 5. How to run\n\n")
    lines.append("Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or "
                 "`jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.\n")
    return "".join(lines)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/scripts/docslib/test_notebooks.py -q`
Expected: PASS (2 passed).

- [ ] **Step 6: Commit**

```bash
git add scripts/docslib/notebooks.py tests/scripts/docslib/test_notebooks.py tests/scripts/_fixtures/nb/
git commit -m "feat(docs): docslib.notebooks — auto-extract per-scenario notebook doc from .ipynb + .zpln"
```

---

## Task 5: `docslib/render_readme.py` — self-contained README surface

**Files:**
- Create: `scripts/docslib/render_readme.py`
- Test: `tests/scripts/docslib/test_render_readme.py`

**Interfaces:**
- Consumes: `model.py`, `links.py` (`DocMap`, rewriters), `notebooks.py`.
- Produces: `render_readme_surface(model: SiteModel, doc_map: DocMap, repo_root: Path) -> dict[Path, str]` — a map of repo-relative output path → file content, for root `README.md`, each `scenarios/<n>/README.md`, each `scenarios/<n>/notebooks.md`, each `spark-apps/<n>/README.md`.

- [ ] **Step 1: Write the failing test**

`tests/scripts/docslib/test_render_readme.py`:
```python
from pathlib import Path
from docslib.parse import parse_site, build_doc_map
from docslib.render_readme import render_readme_surface, AUTOGEN_HEADER

FIX = Path(__file__).resolve().parents[1] / "_fixtures" / "repo"


def test_root_readme_is_self_contained(tmp_path):
    sm = parse_site(FIX)
    dm = build_doc_map(sm)
    out = render_readme_surface(sm, dm, FIX)
    root = out[Path("README.md")]
    assert root.startswith(AUTOGEN_HEADER)
    assert "thekaveh.github.io" not in root          # no .io link
    assert "docs/" not in root.split("## ")[0]       # no docs/ path in head
    assert "README.md#datasets" in root              # concept → root anchor


def test_scenario_readme_internal_links_only(tmp_path):
    sm = parse_site(FIX)
    dm = build_doc_map(sm)
    out = render_readme_surface(sm, dm, FIX)
    sreadme = out[Path("scenarios/batch_ingest-nyc_taxi-spark-iceberg/README.md")]
    assert sreadme.startswith(AUTOGEN_HEADER)
    assert "thekaveh.github.io" not in sreadme
    assert "../../docs/" not in sreadme              # no docs/ cross-link
    assert "../README.md#datasets" in sreadme        # concept → root README
    # diagram localized
    assert "architectures/batch_ingest-nyc_taxi-spark-iceberg.svg" in sreadme


def test_notebooks_md_emitted():
    sm = parse_site(FIX)
    dm = build_doc_map(sm)
    out = render_readme_surface(sm, dm, FIX)
    assert Path("scenarios/batch_ingest-nyc_taxi-spark-iceberg/notebooks.md") in out
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/scripts/docslib/test_render_readme.py -q`
Expected: FAIL.

- [ ] **Step 3: Write minimal implementation**

`scripts/docslib/render_readme.py`:
```python
"""Render the in-repo README surface (self-contained, no cross-surface links)."""
from __future__ import annotations

import re
from pathlib import Path

from .links import classify, rewrite_for_readme
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/scripts/docslib/test_render_readme.py -q`
Expected: PASS (3 passed). (Requires the fixture scenario to have both notebooks present — Task 3 fixture created them.)

- [ ] **Step 5: Commit**

```bash
git add scripts/docslib/render_readme.py tests/scripts/docslib/test_render_readme.py
git commit -m "feat(docs): docslib.render_readme — self-contained README surface, no cross-surface links"
```

---

## Task 6: `docslib/render_wiki.py` — self-contained Wiki surface

**Files:**
- Create: `scripts/docslib/render_wiki.py`
- Test: `tests/scripts/docslib/test_render_wiki.py`

**Interfaces:**
- Consumes: `model.py`, `links.py`, `notebooks.py`.
- Produces: `render_wiki_surface(model, doc_map, repo_root) -> dict[Path, str]` — wiki files (`Home.md`, `_Sidebar.md`, `Scenario-*.md`, `Notebook-*.md`, `App-*.md`, concept pages, `Changelog.md`). **No `.io` banner.**

- [ ] **Step 1: Write the failing test**

`tests/scripts/docslib/test_render_wiki.py`:
```python
from pathlib import Path
from docslib.parse import parse_site, build_doc_map
from docslib.render_wiki import render_wiki_surface

FIX = Path(__file__).resolve().parents[1] / "_fixtures" / "repo"


def test_wiki_has_home_sidebar_scenarios_no_banner():
    sm = parse_site(FIX)
    dm = build_doc_map(sm)
    out = render_wiki_surface(sm, dm, FIX)
    names = {p.name for p in out}
    assert "Home.md" in names and "_Sidebar.md" in names
    scen = out[Path("Scenario-batch_ingest-nyc_taxi-spark-iceberg.md")]
    assert "Full docs site" not in scen            # no banner
    assert "thekaveh.github.io" not in scen
    assert "[[Datasets]]" in scen                  # concept → wiki link
    assert "batch_ingest-nyc_taxi-spark-iceberg.svg" in scen  # diagram localized


def test_sidebar_lists_sections():
    sm = parse_site(FIX)
    dm = build_doc_map(sm)
    out = render_wiki_surface(sm, dm, FIX)
    bar = out[Path("_Sidebar.md")]
    assert "[[Home]]" in bar
    assert "Scenario-batch_ingest" in bar or "batch_ingest" in bar
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/scripts/docslib/test_render_wiki.py -q`
Expected: FAIL.

- [ ] **Step 3: Write minimal implementation**

`scripts/docslib/render_wiki.py`:
```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/scripts/docslib/test_render_wiki.py -q`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add scripts/docslib/render_wiki.py tests/scripts/docslib/test_render_wiki.py
git commit -m "feat(docs): docslib.render_wiki — self-contained wiki surface, no .io banner"
```

---

## Task 7: `docslib/assets.py` + `scripts/build_docs.py` orchestrator

**Files:**
- Create: `scripts/docslib/assets.py`, `scripts/build_docs.py`
- Test: `tests/scripts/test_build_docs.py`

**Interfaces:**
- Consumes: all `docslib` modules.
- Produces: CLI `build_docs.py [--check] [--stage {readme,wiki,assets,all}] [--wiki-dir DIR]`. `assets.copy_assets(model, repo_root, wiki_dir)` copies canonical SVGs into scenario/app dirs + wiki dir.

- [ ] **Step 1: Write the failing test**

`tests/scripts/test_build_docs.py`:
```python
import shutil
from pathlib import Path
import importlib.util
from docslib.parse import parse_site

FIX = Path(__file__).resolve().parents[1] / "_fixtures" / "repo"


def _spec():
    return importlib.util.spec_from_file_location("build_docs", Path("scripts/build_docs.py"))


def test_build_all_writes_readme_and_wiki(tmp_path):
    repo = tmp_path / "repo"
    shutil.copytree(FIX, repo)
    (repo / "docs" / "architectures").mkdir(parents=True, exist_ok=True)
    (repo / "docs" / "architectures" / "batch_ingest-nyc_taxi-spark-iceberg.svg").write_text("<svg/>")
    wd = repo / "wiki"
    import subprocess
    r = subprocess.run(["uv", "run", "--group", "dev", "python", "scripts/build_docs.py",
                        "--root", str(repo), "--wiki-dir", str(wd)], cwd=".", capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    assert (repo / "README.md").exists()
    assert (repo / "scenarios" / "batch_ingest-nyc_taxi-spark-iceberg" / "README.md").exists()
    assert (repo / "scenarios" / "batch_ingest-nyc_taxi-spark-iceberg" / "notebooks.md").exists()
    assert (wd / "Home.md").exists()
    # asset copied into scenario dir
    assert (repo / "scenarios" / "batch_ingest-nyc_taxi-spark-iceberg" /
            "architectures" / "batch_ingest-nyc_taxi-spark-iceberg.svg").exists()
    # asset copied into wiki
    assert (wd / "batch_ingest-nyc_taxi-spark-iceberg.svg").exists()


def test_check_mode_clean_after_build(tmp_path):
    repo = tmp_path / "repo"
    shutil.copytree(FIX, repo)
    (repo / "docs" / "architectures").mkdir(parents=True, exist_ok=True)
    (repo / "docs" / "architectures" / "batch_ingest-nyc_taxi-spark-iceberg.svg").write_text("<svg/>")
    import subprocess
    run = lambda *a: subprocess.run(["uv", "run", "--group", "dev", "python", "scripts/build_docs.py",
                                     "--root", str(repo), "--wiki-dir", str(repo / "wiki"), *a],
                                    capture_output=True, text=True)
    assert run().returncode == 0
    chk = run("--check")
    assert chk.returncode == 0, chk.stdout + chk.stderr
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/scripts/test_build_docs.py -q`
Expected: FAIL (no `build_docs.py`).

- [ ] **Step 3: Write minimal implementation**

`scripts/docslib/assets.py`:
```python
"""Copy canonical SVG diagrams into each derived surface."""
from __future__ import annotations

import shutil
from pathlib import Path


def _canonical(repo_root: Path) -> Path:
    return repo_root / "docs" / "architectures"


def copy_assets(model, repo_root: Path, wiki_dir: Path) -> list[Path]:
    """Copy every canonical SVG referenced by a scenario/app into that surface's local dir + wiki."""
    canon = _canonical(repo_root)
    written: list[Path] = []
    for s in model.scenarios:
        svg = canon / f"{s.name}.svg"
        if not svg.exists():
            continue
        dest = repo_root / "scenarios" / s.name / "architectures" / svg.name
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(svg, dest)
        written.append(dest)
        wdest = wiki_dir / svg.name
        wdest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(svg, wdest)
        written.append(wdest)
    for a in model.apps:
        svg = canon / f"{a.src.stem}.svg"
        if not svg.exists():
            continue
        dest = repo_root / "spark-apps" / a.src.stem / "architectures" / svg.name
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(svg, dest)
        written.append(dest)
        shutil.copy2(svg, wiki_dir / svg.name)
        written.append(wiki_dir / svg.name)
    return written
```

`scripts/build_docs.py`:
```python
#!/usr/bin/env python3
"""Build all three doc surfaces from docs/ (SOT).

    uv run --group dev python scripts/build_docs.py [--check] [--stage STAGE] [--root ROOT] [--wiki-dir DIR]
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

from docslib.parse import parse_site, build_doc_map
from docslib.render_readme import render_readme_surface
from docslib.render_wiki import render_wiki_surface
from docslib.assets import copy_assets


def _write_surface(files: dict[Path, str], base: Path) -> list[Path]:
    written = []
    for rel, content in files.items():
        dest = base / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(content, encoding="utf-8")
        written.append(dest)
    return written


def _build(repo_root: Path, wiki_dir: Path, stage: str) -> None:
    model = parse_site(repo_root)
    doc_map = build_doc_map(model)
    if stage in ("all", "readme"):
        _write_surface(render_readme_surface(model, doc_map, repo_root), repo_root)
    if stage in ("all", "wiki"):
        if wiki_dir.exists():
            shutil.rmtree(wiki_dir)
        wiki_dir.mkdir(parents=True)
        _write_surface(render_wiki_surface(model, doc_map, repo_root), wiki_dir)
    if stage in ("all", "assets"):
        copy_assets(model, repo_root, wiki_dir)


def _check(repo_root: Path, wiki_dir: Path) -> int:
    """Re-build into a temp dir; diff against committed output. 0 = clean."""
    import tempfile
    model = parse_site(repo_root)
    doc_map = build_doc_map(model)
    diffs = 0
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        _write_surface(render_readme_surface(model, doc_map, repo_root), tmp)
        wtmp = tmp / "wiki"
        wtmp.mkdir()
        _write_surface(render_wiki_surface(model, doc_map, repo_root), wtmp)
        copy_assets(model, tmp, wtmp)
        for rel in _expected_outputs(model):
            committed = repo_root / rel
            fresh = tmp / rel
            c = committed.read_text(encoding="utf-8") if committed.exists() else None
            f = fresh.read_text(encoding="utf-8") if fresh.exists() else None
            if c != f:
                print(f"DRIFT: {rel}", file=sys.stderr)
                diffs += 1
    return 1 if diffs else 0


def _expected_outputs(model) -> list[str]:
    out = ["README.md"]
    for s in model.scenarios:
        out += [f"scenarios/{s.name}/README.md", f"scenarios/{s.name}/notebooks.md"]
    for a in model.apps:
        out.append(f"spark-apps/{a.src.stem}/README.md")
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", type=Path)
    ap.add_argument("--wiki-dir", default="wiki", type=Path)
    ap.add_argument("--stage", default="all", choices=["all", "readme", "wiki", "assets"])
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args(argv)
    repo_root = args.root.resolve()
    wiki_dir = (repo_root / args.wiki_dir).resolve() if not args.wiki_dir.is_absolute() else args.wiki_dir
    if args.check:
        return _check(repo_root, wiki_dir)
    _build(repo_root, wiki_dir, args.stage)
    print(f"built {args.stage} → {repo_root}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

Note: `build_docs.py` imports `docslib` as a top-level package, so it must run with `scripts/` on `sys.path`. Add a tiny path bootstrap at the top of `build_docs.py` (before the `docslib` imports):
```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
```
(Insert this immediately after the docstring, before `from docslib.parse import …`.)

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/scripts/test_build_docs.py -q`
Expected: PASS (2 passed).

- [ ] **Step 5: Run the orchestrator on the real repo (smoke)**

Run: `uv run --group dev python scripts/build_docs.py --root .`
Then: `uv run --group dev python scripts/build_docs.py --root . --check`
Expected: builds READMEs + wiki; `--check` exits 0.
Then **discard the real-repo output** (the content tasks aren't done yet — diagrams/nav still pending): `git checkout -- README.md scenarios spark-apps && rm -rf wiki`.

- [ ] **Step 6: Commit**

```bash
git add scripts/docslib/assets.py scripts/build_docs.py tests/scripts/test_build_docs.py
git commit -m "feat(docs): build_docs.py orchestrator + assets.copy_assets (readme+wiki+svg, --check)"
```

---

## Task 8: `scripts/check_surfaces.py` — CI assertions

**Files:**
- Create: `scripts/check_surfaces.py`
- Test: `tests/scripts/test_check_surfaces.py`

**Interfaces:**
- Produces: CLI `check_surfaces.py [--root ROOT]` → exit 1 if any violation. Rules:
  1. No file under `scenarios/`, `spark-apps/`, or root `README.md` contains `thekaveh.github.io` or a `docs/`-relative link `(docs/…` / `](../../docs/`.
  2. No file under `wiki/` contains `thekaveh.github.io` or the banner text `Full docs site`.
  3. Every `![…](architectures/X.svg)` reference in a README has a local `architectures/X.svg` file beside it.

- [ ] **Step 1: Write the failing test**

`tests/scripts/test_check_surfaces.py`:
```python
import importlib.util
from pathlib import Path

SPEC = importlib.util.spec_from_file_location("check_surfaces", Path("scripts/check_surfaces.py"))


def _load():
    m = importlib.util.module_from_spec(SPEC)
    SPEC.loader.exec_module(m)
    return m


def test_clean_repo_passes(tmp_path):
    cs = _load()
    (tmp_path / "README.md").write_text("ok no links\n")
    (tmp_path / "scenarios").mkdir()
    (tmp_path / "scenarios" / "a").mkdir()
    (tmp_path / "scenarios" / "a" / "README.md").write_text("# a\n")
    assert cs.main(["--root", str(tmp_path)]) == 0


def test_io_link_in_readme_fails(tmp_path):
    cs = _load()
    (tmp_path / "README.md").write_text("see https://thekaveh.github.io/data-eng-lab/\n")
    assert cs.main(["--root", str(tmp_path)]) == 1


def test_docs_relative_link_fails(tmp_path):
    cs = _load()
    (tmp_path / "scenarios").mkdir()
    (tmp_path / "scenarios" / "a").mkdir()
    (tmp_path / "scenarios" / "a" / "README.md").write_text("[x](../../docs/datasets.md)\n")
    assert cs.main(["--root", str(tmp_path)]) == 1


def test_wiki_banner_fails(tmp_path):
    cs = _load()
    (tmp_path / "wiki").mkdir()
    (tmp_path / "wiki" / "Home.md").write_text("> Full docs site: https://thekaveh.github.io/...\n")
    assert cs.main(["--root", str(tmp_path)]) == 1


def test_missing_local_svg_fails(tmp_path):
    cs = _load()
    (tmp_path / "scenarios").mkdir()
    (tmp_path / "scenarios" / "a").mkdir()
    (tmp_path / "scenarios" / "a" / "README.md").write_text("![d](architectures/a.svg)\n")
    assert cs.main(["--root", str(tmp_path)]) == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/scripts/test_check_surfaces.py -q`
Expected: FAIL.

- [ ] **Step 3: Write minimal implementation**

`scripts/check_surfaces.py`:
```python
#!/usr/bin/env python3
"""Assert the three doc surfaces are self-contained (no cross-surface links; diagrams embedded)."""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

SITE = "thekaveh.github.io"
BANNER = "Full docs site"
DOCS_REL = re.compile(r"\]\((\.\./)+docs/|\]\(docs/")
SVG_REF = re.compile(r"!\[[^\]]*\]\(([^)]+\.svg)\)")


def _scan(root: Path) -> list[str]:
    findings: list[str] = []
    # in-repo markdown surface
    candidates = [root / "README.md"]
    candidates += sorted((root / "scenarios").rglob("*.md")) if (root / "scenarios").exists() else []
    candidates += sorted((root / "spark-apps").rglob("*.md")) if (root / "spark-apps").exists() else []
    for f in candidates:
        if not f.exists():
            continue
        text = f.read_text(encoding="utf-8")
        if SITE in text:
            findings.append(f"{f.relative_to(root)}: links to .io site")
        if DOCS_REL.search(text):
            findings.append(f"{f.relative_to(root)}: docs/-relative link")
        for m in SVG_REF.finditer(text):
            svg = f.parent / m.group(1)
            if not svg.exists():
                findings.append(f"{f.relative_to(root)}: missing local SVG {m.group(1)}")
    # wiki surface
    wdir = root / "wiki"
    if wdir.exists():
        for f in wdir.glob("*.md"):
            text = f.read_text(encoding="utf-8")
            if SITE in text:
                findings.append(f"wiki/{f.name}: links to .io site")
            if BANNER in text:
                findings.append(f"wiki/{f.name}: contains mirror banner")
    return findings


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", type=Path)
    args = ap.parse_args(argv)
    root = args.root.resolve()
    findings = _scan(root)
    for f in findings:
        print(f"VIOLATION: {f}", file=sys.stderr)
    if findings:
        print(f"\n{len(findings)} surface violation(s)", file=sys.stderr)
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/scripts/test_check_surfaces.py -q`
Expected: PASS (5 passed).

- [ ] **Step 5: Commit**

```bash
git add scripts/check_surfaces.py tests/scripts/test_check_surfaces.py
git commit -m "feat(docs): check_surfaces.py — CI assertion for no cross-surface links + embedded SVGs"
```

---

## Task 9: Realign `verify_repo` config to the 8-section template (fixes CI gate)

**Files:**
- Modify: `scripts/verify_repo_config.yaml`
- Modify: `tests/test_verify_repo.py` (the hardcoded `CFG["scenario_readme_sections"]` + `_make_valid_scenario` helper)

- [ ] **Step 1: Update the config**

In `scripts/verify_repo_config.yaml`, replace the `scenario_readme_sections` list with:
```yaml
scenario_readme_sections:
  - "1. Purpose"
  - "2. Data Model"
  - "3. Architecture"
  - "4. Notebooks"
  - "5. Orchestration"
  - "6. Usage"
  - "7. Dependencies"
  - "8. Known Issues & Caveats"
```

- [ ] **Step 2: Update the test helper + CFG**

In `tests/test_verify_repo.py`:
- In `_make_valid_scenario`, change the `sections` list to:
```python
sections = ["1. Purpose", "2. Data Model", "3. Architecture", "4. Notebooks",
            "5. Orchestration", "6. Usage", "7. Dependencies", "8. Known Issues & Caveats"]
```
- In the `CFG` and `SPARK_CFG` dicts, replace the `scenario_readme_sections` value with the same 8-item list.
- In `test_missing_readme_section_flags_error`, change the written README to:
```python
(d / "README.md").write_text("# t\n\n## 1. Purpose\n\ntext\n")  # only 1 of 8
```

- [ ] **Step 3: Verify against the real repo**

Run: `uv run python scripts/verify_repo.py --root .`
Expected: `0 error(s)` for scenario.readme (all 19 READMEs have the 8 sections).

Run: `uv run pytest tests/test_verify_repo.py -q`
Expected: PASS (all).

- [ ] **Step 4: Commit**

```bash
git add scripts/verify_repo_config.yaml tests/test_verify_repo.py
git commit -m "fix(docs): realign verify_repo scenario README sections to the 8-section template"
```

---

## Task 10: Standardize diagram location + manifest; delete orphans

**Files:**
- Create: `scripts/diagrams_manifest.yaml`, `scripts/check_diagrams.py`, `tests/scripts/test_check_diagrams.py`
- Delete: `docs/architecture.html`, `docs/lakehouse-architecture.html`, all `docs/scenarios/architectures/*.html`, all `docs/spark-apps/architectures/*.html` (incl. the 3 orphans).
- Create: empty `docs/architectures/.gitkeep`.

- [ ] **Step 1: Write the manifest**

`scripts/diagrams_manifest.yaml`:
```yaml
# Declarative inputs for architecture-diagram generation (Task 11) + checks.
# Every diagram is a landscape SVG written to docs/architectures/<output>.
orientation: landscape
diagrams:
  - {name: overview, title: "Full-stack lakehouse", kind: overview, output: overview.svg,
     sources: ["docs/index.md", "docs/lakehouse.md"]}
  - {name: lakehouse, title: "Medallion flow", kind: lakehouse, output: lakehouse.svg,
     sources: ["docs/lakehouse.md"]}
  - {name: batch_ingest-nyc_taxi-spark-iceberg, kind: scenario,
     output: batch_ingest-nyc_taxi-spark-iceberg.svg,
     sources: ["scenarios/batch_ingest-nyc_taxi-spark-iceberg/jupyter/notebook.ipynb",
               "scenarios/batch_ingest-nyc_taxi-spark-iceberg/dag.py"]}
  # ... 18 more scenario entries, one per scenario dir, sources = [jupyter notebook, dag.py] ...
  - {name: nyc-taxi-etl, kind: app, output: nyc-taxi-etl.svg,
     sources: ["spark-apps/nyc-taxi-etl/pom.xml", "spark-apps/nyc-taxi-etl/Jenkinsfile"]}
  - {name: nyc-taxi-medallion, kind: app, output: nyc-taxi-medallion.svg,
     sources: ["spark-apps/nyc-taxi-medallion/pom.xml", "spark-apps/nyc-taxi-medallion/Jenkinsfile"]}
```
(The implementer enumerates all 19 scenario entries from `scenarios/*/` and both apps; each scenario entry copies the two-line shape above with its own `name`/`output`/`sources`.)

- [ ] **Step 2: Write `check_diagrams.py` + test**

`tests/scripts/test_check_diagrams.py`:
```python
import importlib.util
from pathlib import Path

SPEC = importlib.util.spec_from_file_location("check_diagrams", Path("scripts/check_diagrams.py"))


def _load():
    m = importlib.util.module_from_spec(SPEC)
    SPEC.loader.exec_module(m)
    return m


def _svg(w, h):
    return f'<?xml version="1.0"?><svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}"></svg>'


def test_landscape_passes(tmp_path):
    cs = _load()
    arch = tmp_path / "docs" / "architectures"
    arch.mkdir(parents=True)
    (arch / "a.svg").write_text(_svg(800, 400))
    (tmp_path / "scripts" / "diagrams_manifest.yaml").parent.mkdir(parents=True)
    (tmp_path / "scripts" / "diagrams_manifest.yaml").write_text(
        "orientation: landscape\ndiagrams:\n  - {output: a.svg}\n")
    assert cs.main(["--root", str(tmp_path)]) == 0


def test_portrait_or_script_fails(tmp_path):
    cs = _load()
    arch = tmp_path / "docs" / "architectures"
    arch.mkdir(parents=True)
    (arch / "a.svg").write_text(_svg(400, 800))            # portrait
    (arch / "b.svg").write_text(_svg(800, 400) + "<script>x</script>")  # script
    (tmp_path / "scripts" / "diagrams_manifest.yaml").parent.mkdir(parents=True)
    (tmp_path / "scripts" / "diagrams_manifest.yaml").write_text(
        "orientation: landscape\ndiagrams:\n  - {output: a.svg}\n  - {output: b.svg}\n")
    assert cs.main(["--root", str(tmp_path)]) == 1
```

`scripts/check_diagrams.py`:
```python
#!/usr/bin/env python3
"""Assert every canonical diagram in docs/architectures/ is a landscape, script-free SVG."""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import yaml

_W = re.compile(r'\bwidth="([\d.]+)"')
_H = re.compile(r'\bheight="([\d.]+)"')


def _scan(root: Path) -> list[str]:
    findings: list[str] = []
    arch = root / "docs" / "architectures"
    if not arch.exists():
        return ["docs/architectures/ missing"]
    for svg in sorted(arch.glob("*.svg")):
        text = svg.read_text(encoding="utf-8")
        if "<script" in text:
            findings.append(f"{svg.name}: contains <script> (GitHub won't render)")
        wm, hm = _W.search(text), _H.search(text)
        if not (wm and hm):
            findings.append(f"{svg.name}: missing width/height")
            continue
        w, h = float(wm.group(1)), float(hm.group(1))
        if h >= w:
            findings.append(f"{svg.name}: portrait ({w}x{h}); must be landscape")
    return findings


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", type=Path)
    args = ap.parse_args(argv)
    findings = _scan(args.root.resolve())
    for f in findings:
        print(f"DIAGRAM: {f}", file=sys.stderr)
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 3: Run test, then delete the old HTML diagrams + orphans**

Run: `uv run pytest tests/scripts/test_check_diagrams.py -q` → PASS.
Then:
```bash
mkdir -p docs/architectures && touch docs/architectures/.gitkeep
git rm docs/architecture.html docs/lakehouse-architecture.html
git rm docs/scenarios/architectures/*.html
git rm docs/spark-apps/architectures/*.html
```

- [ ] **Step 4: Commit**

```bash
git add scripts/diagrams_manifest.yaml scripts/check_diagrams.py tests/scripts/test_check_diagrams.py docs/architectures/.gitkeep
git commit -m "feat(docs): diagram manifest + check_diagrams; remove old HTML diagrams + 3 orphans"
```

---

## Task 11: Generate landscape SVG diagrams via the `architecture-diagram` skill

**Files:**
- Create: 23 SVGs under `docs/architectures/` (`overview.svg`, `lakehouse.svg`, 19 `scenario.svg`, 2 `app.svg`).

This task generates assets with a skill, not hand-written code.

- [ ] **Step 1: Generate each diagram**

For each entry in `scripts/diagrams_manifest.yaml`, invoke the **`architecture-diagram` skill** to produce `docs/architectures/<output>`. Pass the skill:
- **Orientation: landscape** (hard requirement).
- **Palette:** dark variant — cyan `#22d3ee` primary, slate node fills (`#1e293b`/`#334155`), `#0a0f1f` background, white text; provide a light variant (`#0e7490` primary, `#f6f8fb` background) where the skill supports a second theme.
- **Content sources:** read the files listed in the entry's `sources:` to derive nodes/edges (scenario: source → processing → Iceberg sink from the notebook; app: Jenkins → shade → JAR → Airflow → Spark → Iceberg from `pom.xml`/`Jenkinsfile`).
- **Output:** a script-free standalone SVG at the manifest path.

- [ ] **Step 2: Verify all diagrams**

Run: `uv run --group dev python scripts/check_diagrams.py --root .`
Expected: exit 0 (all landscape, script-free, sized).
Run: `ls docs/architectures/*.svg | wc -l` → expected `23` (overview + lakehouse + 19 + 2).

- [ ] **Step 3: Repoint doc references + commit**

Update every `docs/scenarios/*.md` and `docs/spark-apps/*.md` architecture reference from `architectures/<name>.html` (or `../../docs/...`) to `../architectures/<name>.svg` (scenario docs are in `docs/scenarios/`, canonical SVGs in `docs/architectures/`, so the relative path is `../architectures/<name>.svg`). Update `docs/index.md` → `architectures/overview.svg`; `docs/lakehouse.md` → `architectures/lakehouse.svg`.

```bash
git add docs/architectures/*.svg docs/scenarios/*.md docs/spark-apps/*.md docs/index.md docs/lakehouse.md
git commit -m "feat(docs): generate 23 landscape SVG diagrams (architecture-diagram skill); repoint refs"
```

---

## Task 12: Orbital theme — `custom.css` + `main.html` + `mkdocs.yml`

**Files:**
- Modify: `docs/css/custom.css` (full replacement)
- Create: `docs/overrides/main.html`
- Modify: `mkdocs.yml` (`theme.palette`, `theme.font`, plugins, `extra_css`)

- [ ] **Step 1: Write `docs/overrides/main.html`**

```html
{% extends "base.html" %}

{% block styles %}
  {{ super() }}
  <style>
    :root{
      --orbital-bg:#0a0f1f; --orbital-accent:#22d3ee;
    }
    [data-md-color-scheme="default"]{
      --orbital-bg:#f6f8fb; --orbital-accent:#0e7490;
    }
    body{ background-color:var(--orbital-bg); position:relative; }
    body::before{
      content:""; position:fixed; inset:0; pointer-events:none; z-index:-1;
      background:radial-gradient(circle at 88% -5%,
        color-mix(in srgb, var(--orbital-accent) 7%, transparent) 0%, transparent 38%);
    }
  </style>
{% endblock %}

{% block fonts %}
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;600;700&family=IBM+Plex+Mono:wght@400;500;600&family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
{% endblock %}
```

- [ ] **Step 2: Replace `docs/css/custom.css`**

```css
/* Orbital theme — data-eng-lab MkDocs Material. Flat dark canvas, faint cyan corner glow, no grid. */

:root{
  --md-accent-fg-color:#22d3ee;
  --md-accent-fg-color--transparent:#22d3ee22;
  --md-primary-fg-color:#22d3ee;
  --md-primary-fg-color--light:#67e8f9;
  --md-primary-fg-color--dark:#0e7490;
  --md-default-bg-color:#0a0f1f;
  --md-default-fg-color:#e2e8f0;
  --md-text-font:"Inter",system-ui,sans-serif;
  --md-code-font:"IBM Plex Mono",monospace;
}
[data-md-color-scheme="default"]{
  --md-default-bg-color:#f6f8fb;
  --md-default-fg-color:#1e293b;
  --md-accent-fg-color:#0e7490;
  --md-accent-fg-color--transparent:#0e749022;
  --md-primary-fg-color:#0e7490;
  --md-primary-fg-color--light:#0891b2;
  --md-primary-fg-color--dark:#155e75;
}

.md-typeset h1,.md-typeset h2,.md-typeset h3{ font-family:"Space Grotesk","Inter",sans-serif; letter-spacing:-0.01em; }
.md-header__title{ font-family:"Space Grotesk",sans-serif; }
.md-typeset .headerlink{ color:var(--md-accent-fg-color); }
.md-typeset a{ color:var(--md-accent-fg-color); }
.md-nav__item--active>.md-nav__link{ color:var(--md-accent-fg-color); }

/* HUD-ish mono breadcrumbs / labels */
.md-path{ font-family:"IBM Plex Mono",monospace; font-size:.75rem; text-transform:uppercase; letter-spacing:.06em; }

/* Tables */
.md-typeset table{ border-collapse:collapse; border-radius:8px; overflow:hidden; }
.md-typeset th{ background:color-mix(in srgb,var(--md-accent-fg-color) 14%,transparent); color:var(--md-default-fg-color); font-family:"IBM Plex Mono",monospace; font-weight:500; letter-spacing:.02em; }
.md-typeset td{ padding:10px 16px; }

/* Code */
.md-typeset code,.md-typeset pre{ font-family:"IBM Plex Mono",monospace; border-radius:6px; }
.md-typeset :not(pre)>code{ background:color-mix(in srgb,var(--md-accent-fg-color) 12%,transparent); }

/* Admonitions: single left rule, accent */
.md-admonition{ border-left:3px solid var(--md-accent-fg-color)!important; box-shadow:none; }
```

- [ ] **Step 3: Update `mkdocs.yml`**

In `mkdocs.yml`:
- `theme.palette` → two schemes (slate-derived dark default `primary: custom`, accent `custom`; light scheme). Material reads the custom colors from CSS variables, so set `primary: cyan`/`accent: cyan` is acceptable as a fallback; keep the toggle pair:
```yaml
theme:
  name: material
  custom_dir: docs/overrides
  palette:
    - scheme: slate
      primary: cyan
      accent: cyan
      toggle:
        icon: material/weather-night
        name: Switch to light mode
    - scheme: default
      primary: cyan
      accent: cyan
      toggle:
        icon: material/weather-sunny
        name: Switch to dark mode
  font:
    text: Inter
    code: JetBrains Mono
  features:
    - navigation.tabs
    - navigation.sections
    - navigation.top
    - navigation.tracking
    - navigation.instant
    - toc.follow
    - search.suggest
    - search.highlight
    - content.code.copy
    - content.code.annotate
    - content.tabs.link
  icon:
    repo: fontawesome/brands/github
```
- **Remove** the `plugins:` `gen-files` and `literate-nav` entries (and their `scripts:`/`nav_file:` children). Keep only `- search`.
- Remove the `markdown_extensions` `pymdownx` entries only if desired — keep them (they're harmless and useful). Leave `extra_css: - css/custom.css` as-is.

- [ ] **Step 4: Build + verify**

Run: `uv run --group dev mkdocs build --strict`
Expected: success, no warnings (the removed `gen_doc_pages.py` plugin reference is gone; no `SUMMARY.md` needed).
Then `mkdocs serve` and visually confirm: dark default, cyan accent, flat canvas with a faint top-right glow, Space Grotesk headings, no grid.

- [ ] **Step 5: Commit**

```bash
git add docs/overrides/main.html docs/css/custom.css mkdocs.yml
git commit -m "feat(docs): Orbital theme (flat dark + faint cyan glow, no grid) + fonts; drop gen-files/literate-nav"
```

---

## Task 13: Numbered content hierarchy + concept renumbering

**Files:**
- Modify: `mkdocs.yml` (`nav:` → numbered tree per spec §4; add Notebooks section)
- Modify: `docs/index.md`, `docs/getting-started.md`, `docs/lakehouse.md`, `docs/datasets.md` (add numbered subsections)
- Create: `docs/notebooks/index.md`

- [ ] **Step 1: Rewrite `mkdocs.yml` nav to the numbered tree**

Replace the `nav:` block with the hierarchy from spec §4:
```yaml
nav:
  - "1. Overview": index.md
  - "2. Getting Started": getting-started.md
  - "3. Lakehouse Architecture": lakehouse.md
  - "4. Datasets": datasets.md
  - "5. Scenarios":
      - "5.1 Catalog": scenarios/index.md
      - "5.2 batch_ingest-nyc_taxi-spark-iceberg": scenarios/batch_ingest-nyc_taxi-spark-iceberg.md
      # ... 5.3–5.20 grouped by category in the spec's order ...
  - "6. Notebooks":
      - "6.1 Index": notebooks/index.md
      # one entry per scenario notebook doc, generated docs/notebooks/<name>.md
  - "7. Spark Apps":
      - "7.1 Overview": spark-apps/index.md
      - "7.2 nyc-taxi-etl": spark-apps/nyc-taxi-etl.md
      - "7.3 nyc-taxi-medallion": spark-apps/nyc-taxi-medallion.md
  - "8. Atlas Operations":
      - "8.1 Atlas Expectations": atlas-expectations.md
      - "8.2 Atlas Go-Live Runbook": go-live.md
      - "8.3 Atlas Go-Live Results": go-live-results.md
      - "8.4 Atlas Feedback (A7/A9)": atlas-feedback-a7a9.md
      - "8.5 Atlas Go-Live Findings": atlas-feedback-go-live.md
      - "8.6 Atlas Enablement": atlas-enablement.md
  - "9. Changelog": CHANGELOG.md
```

- [ ] **Step 2: Renumber concept docs + add Notebooks index**

- Add numbered subsections (`## 3.1 …`, `## 3.2 …`) to `docs/lakehouse.md`, `docs/datasets.md`; `## 2.1 …` to `docs/getting-started.md`. (Keep existing prose; only promote existing `##` headers to numbered form in order.)
- Create `docs/notebooks/index.md`:
```markdown
# 6. Notebooks

Each scenario ships paired Zeppelin (Scala) and Jupyter (PySpark) notebooks with identical logic.
A comprehensive, auto-extracted notebook doc per scenario lives below — also reachable from each scenario's §4.

- [batch_ingest-nyc_taxi-spark-iceberg](batch_ingest-nyc_taxi-spark-iceberg.md)
<!-- ... one link per scenario ... -->
```
- Generate the 19 `docs/notebooks/<scenario>.md` files by running the extractor over the real notebooks:
```bash
uv run --group dev python -c "
from pathlib import Path
import sys; sys.path.insert(0,'scripts')
from docslib.notebooks import extract_notebook_doc
out=Path('docs/notebooks'); out.mkdir(exist_ok=True)
for d in sorted(Path('scenarios').iterdir()):
    j=d/'jupyter'/'notebook.ipynb'; z=d/'zeppelin'/'notebook.zpln'
    if j.exists() and z.exists():
        (out/f'{d.name}.md').write_text(extract_notebook_doc(d.name,j,z),encoding='utf-8')
        print('wrote', d.name)
"
```

- [ ] **Step 3: Build + verify**

Run: `uv run --group dev mkdocs build --strict`
Expected: success; nav shows the numbered tree; notebook pages resolve.

- [ ] **Step 4: Commit**

```bash
git add mkdocs.yml docs/index.md docs/getting-started.md docs/lakehouse.md docs/datasets.md docs/notebooks/
git commit -m "feat(docs): numbered content hierarchy (§4) + per-scenario notebook docs in docs/notebooks/"
```

---

## Task 14: CI — `docs-sync.yml`, update `ci.yml`, trim `docs-deploy.yml`

**Files:**
- Create: `.github/workflows/docs-sync.yml`
- Modify: `.github/workflows/ci.yml` (add to `docs-build` job)
- Modify: `.github/workflows/docs-deploy.yml` (remove `render_readme.py` step)

- [ ] **Step 1: Write `docs-sync.yml`**

```yaml
name: docs-sync
on:
  push:
    branches: [main]
    paths:
      - "docs/**"
      - "mkdocs.yml"
      - "scenarios/**"
      - "spark-apps/**"
      - "scripts/build_docs.py"
      - "scripts/docslib/**"
      - "scripts/check_surfaces.py"
      - ".github/workflows/docs-sync.yml"
  workflow_dispatch:
permissions:
  contents: write
jobs:
  sync:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5  # v4
        with: { submodules: false }
      - uses: astral-sh/setup-uv@d4b2f3b6ecc6e67c4457f6d3e41ec42d3d0fcb86  # v5
        with: { enable-cache: true }
      - run: uv run --group dev python scripts/build_docs.py --root .
      - run: uv run --group dev python scripts/check_surfaces.py --root .
      - name: Commit regenerated READMEs
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
          git add README.md scenarios spark-apps
          if git diff --cached --quiet; then echo "no readme changes"; else
            git commit -m "docs: regenerate READMEs (docs-sync, ${{ github.sha }})"
            git push
          fi
      - name: Push wiki
        run: |
          git clone "https://x-access-token:${{ secrets.GITHUB_TOKEN }}@github.com/${{ github.repository }}.wiki.git" wiki-repo
          rsync -a --delete --exclude='.git' wiki/ wiki-repo/
          cd wiki-repo
          git config user.name "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
          git add -A
          if git diff --cached --quiet; then echo "no wiki changes"; else
            git commit -m "docs: sync wiki from docs/ (${{ github.sha }})"
            git push
          fi
```

- [ ] **Step 2: Extend the `docs-build` job in `ci.yml`**

In `.github/workflows/ci.yml`, after the existing `- run: uv run --group dev mkdocs build --strict` step in the `docs-build` job, add:
```yaml
      - run: uv run --group dev python scripts/build_docs.py --root . --check
      - run: uv run --group dev python scripts/check_surfaces.py --root .
      - run: uv run --group dev python scripts/check_diagrams.py --root .
```

- [ ] **Step 3: Trim `docs-deploy.yml`**

Remove these two lines from the `build` job in `.github/workflows/docs-deploy.yml`:
```yaml
        - run: uv run --group dev python scripts/render_readme.py
```
and remove `"scripts/render_readme.py"`, `"scripts/build_wiki.py"`, `"scripts/gen_doc_pages.py"` from the `paths:` trigger (they no longer exist). Keep the `mkdocs build --strict` + Pages deploy steps.

- [ ] **Step 4: Verify YAML parses**

Run: `uv run --group dev python -c "import yaml; [yaml.safe_load(open(f)) for f in ['.github/workflows/docs-sync.yml','.github/workflows/ci.yml','.github/workflows/docs-deploy.yml']]; print('ok')" `
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/docs-sync.yml .github/workflows/ci.yml .github/workflows/docs-deploy.yml
git commit -m "ci(docs): docs-sync.yml (regen+commit READMEs, push wiki); gate build with check_surfaces/diagrams/--check"
```

---

## Task 15: Cleanup — remove superseded scripts

**Files:**
- Delete: `scripts/gen_doc_pages.py`, `scripts/render_readme.py`, `scripts/build_wiki.py`

- [ ] **Step 1: Delete (only after Tasks 12–14 confirm nothing references them)**

Run a final reference check first:
```bash
grep -RInE 'gen_doc_pages|render_readme|build_wiki' .github mkdocs.yml docs --include='*.yml' --include='*.yaml' --include='*.md' || echo "no references"
```
Expected: `no references`. Then:
```bash
git rm scripts/gen_doc_pages.py scripts/render_readme.py scripts/build_wiki.py
```

- [ ] **Step 2: Verify the full test + build suite still passes**

Run: `uv run ruff check .`
Run: `uv run pytest -m "not infra and not network" -q`
Run: `uv run python scripts/verify_repo.py --root .`
Run: `uv run --group dev mkdocs build --strict`
Expected: all green.

- [ ] **Step 3: Commit**

```bash
git commit -m "chore(docs): remove superseded gen_doc_pages/render_readme/build_wiki (subsumed by docslib)"
```

---

## Task 16: Final end-to-end regeneration + verification

- [ ] **Step 1: Regenerate everything and commit**

```bash
uv run --group dev python scripts/build_docs.py --root .
uv run --group dev python scripts/check_surfaces.py --root .
uv run --group dev python scripts/build_docs.py --root . --check
uv run --group dev python scripts/check_diagrams.py --root .
git add README.md scenarios spark-apps docs/architectures
git commit -m "docs: regenerate all three surfaces from docs/ SOT (self-contained, embedded SVGs)"
```

- [ ] **Step 2: Full verification (spec §10)**

```bash
uv run ruff check .
uv run pytest -m "not infra and not network" -q
uv run python scripts/verify_repo.py --root .
uv run --group dev mkdocs build --strict
uv run --group dev python scripts/check_surfaces.py --root .
uv run --group dev python scripts/build_docs.py --root . --check
```
Expected: every command exits 0.

- [ ] **Step 3: Manual visual confirmation**

`uv run --group dev mkdocs serve` → confirm Orbital theme (dark default, cyan accent, flat canvas + faint corner glow, Space Grotesk headings, no grid), light toggle works, every scenario page embeds its landscape SVG, nav shows the numbered tree, every notebook page renders.

- [ ] **Step 4: Commit any final state + push the branch**

```bash
git push origin docs/overhaul-2026-07-06
```

---

## Notes for the implementer

- **Run order matters.** Tasks 1–8 build and unit-test the engine against fixtures; Task 9 fixes the CI gate; Tasks 10–13 produce assets + content; Task 14 wires CI; Task 15 deletes old scripts only after nothing references them; Task 16 is the end-to-end regen.
- **Commit per task** (each task ends with a commit step). Keep commits focused.
- **The deferred item #10** (per-scenario docs-vs-notebook content accuracy, e.g. `batch_ingest` "no transform" vs the cleaning notebook) is intentionally **out of scope** for this plan — it is content work tracked separately in the spec (§9).
- **`infra/` is never touched.** If a command would descend into `infra/`, scope it to the top-level paths only.
