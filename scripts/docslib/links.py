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
    """Resolve a relative markdown target to a docs/-posix path.

    Bare names are concept pages (docs-root-relative, e.g. ``datasets.md``) when listed in
    ``_CONCEPTS``; otherwise they are siblings in the current doc's directory.
    """
    if "/" not in target and not target.startswith("."):
        if target in _CONCEPTS:
            return target  # already a docs-root-relative posix like "datasets.md"
        # bare sibling in the same docs/ directory as the current doc
        cur_dir = "/".join(current_doc_posix.split("/")[:-1])
        return f"{cur_dir}/{target}" if cur_dir else target
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


# Conventional README/wiki destinations for scenario & spark-app docs absent from the DocMap.
_WIKI_PREFIX = {"scenarios": "Scenario", "spark-apps": "App"}


def _sibling_readme(doc_posix: str) -> str | None:
    """Convention README path for a scenario/spark-app doc (``scenarios/foo.md`` → ``scenarios/foo/README.md``)."""
    if "/" in doc_posix:
        parent, name = doc_posix.rsplit("/", 1)
        if parent in ("scenarios", "spark-apps") and name.endswith(".md"):
            return f"{doc_posix[:-3]}/README.md"
    return None


def _sibling_wiki_slug(doc_posix: str) -> str | None:
    """Convention wiki slug for a scenario/spark-app doc (``scenarios/foo.md`` → ``Scenario-foo``)."""
    if "/" in doc_posix:
        parent, name = doc_posix.rsplit("/", 1)
        if parent in _WIKI_PREFIX and name.endswith(".md"):
            return f"{_WIKI_PREFIX[parent]}-{name[:-3]}"
    return None


def rewrite_for_readme(target: str, current_doc_posix: str, doc_map: DocMap) -> str:
    kind = classify(target)
    if kind in (LinkKind.ANCHOR, LinkKind.MAILTO):
        return target
    if kind == LinkKind.EXTERNAL_SITE:
        return ""  # cross-surface link — drop
    if kind == LinkKind.EXTERNAL_OTHER:
        return target
    if kind == LinkKind.DIAGRAM:
        return target  # preserve local diagram path (e.g. architectures/foo.svg) for the in-repo README
    # DOC_RELATIVE
    doc_posix = _resolve_doc_relative(target, current_doc_posix)
    if doc_posix in doc_map.readme:
        return _relpath_between_docs(current_doc_posix, doc_map.readme[doc_posix])
    if doc_posix in _CONCEPTS:
        anchor = _CONCEPTS[doc_posix][0]
        return _relpath_to_root_readme(current_doc_posix, anchor)
    sibling = _sibling_readme(doc_posix)
    if sibling:
        return _relpath_between_docs(current_doc_posix, sibling)
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
    slug = _sibling_wiki_slug(doc_posix)
    if slug:
        return f"[[{slug}]]"
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
    """Relative path from the directory ``src_dir`` to ``dst`` (common-prefix aware)."""
    src_parts = src_dir.split("/") if src_dir else []
    dst_parts = dst.split("/")
    i = 0
    while i < len(src_parts) and i < len(dst_parts) and src_parts[i] == dst_parts[i]:
        i += 1
    up = [".."] * (len(src_parts) - i)
    down = dst_parts[i:]
    result = up + down
    return "/".join(result) if result else "."
