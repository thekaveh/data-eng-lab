"""Copy canonical SVG diagrams into each derived surface."""
from __future__ import annotations

import re
import shutil
from pathlib import Path

_IMG = re.compile(r"!\[[^\]]*\]\(([^)]+)\)")


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
    # Concept-page diagrams: scan every top-level docs/*.md concept page (index,
    # lakehouse, getting-started, ...) for architectures/<name>.svg refs and mirror each
    # canonical SVG to BOTH the in-repo README surface (<repo_root>/architectures/, so the
    # root README's "architectures/<name>.svg" path resolves in-repo) AND the wiki surface
    # (wiki_dir/<name>.svg, so the bare "<name>.svg" refs produced by render_wiki resolve).
    # Scenario/spark-app subdirs are excluded — their diagrams are handled by the loops above.
    written.extend(_copy_concept_diagrams(repo_root, wiki_dir, canon))
    return written


def _copy_concept_diagrams(repo_root: Path, wiki_dir: Path, canon: Path) -> list[Path]:
    """Mirror concept-page SVGs (referenced by top-level docs/*.md) to both surfaces."""
    docs = repo_root / "docs"
    if not docs.exists():
        return []
    root_arch = repo_root / "architectures"
    written: list[Path] = []
    seen: set[str] = set()
    for doc in sorted(docs.glob("*.md")):
        text = doc.read_text(encoding="utf-8")
        for m in _IMG.finditer(text):
            target = m.group(1).strip()
            # Match "architectures/x.svg", "../architectures/x.svg", etc.
            if "architectures/" not in target:
                continue
            name = target.split("/")[-1]
            if not name.endswith(".svg") or name in seen:
                continue
            svg = canon / name
            if not svg.exists():
                continue
            seen.add(name)
            root_arch.mkdir(parents=True, exist_ok=True)
            rdest = root_arch / name
            shutil.copy2(svg, rdest)
            written.append(rdest)
            wiki_dir.mkdir(parents=True, exist_ok=True)
            wdest = wiki_dir / name
            shutil.copy2(svg, wdest)
            written.append(wdest)
    return written
