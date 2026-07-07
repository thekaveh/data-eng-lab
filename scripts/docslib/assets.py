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
    # Root README's diagrams: scan docs/index.md (the source of the root README) for
    # architectures/<name>.svg refs and mirror them to <repo_root>/architectures/ so the
    # root README's normalized "architectures/<name>.svg" path resolves in-repo.
    written.extend(_copy_root_readme_diagrams(repo_root, canon))
    return written


def _copy_root_readme_diagrams(repo_root: Path, canon: Path) -> list[Path]:
    index = repo_root / "docs" / "index.md"
    if not index.exists():
        return []
    root_arch = repo_root / "architectures"
    written: list[Path] = []
    for m in _IMG.finditer(index.read_text(encoding="utf-8")):
        target = m.group(1).strip()
        # Match both "architectures/x.svg" and "../architectures/x.svg" forms.
        if "architectures/" not in target:
            continue
        name = target.split("/")[-1]
        if not name.endswith(".svg"):
            continue
        svg = canon / name
        if not svg.exists():
            continue
        root_arch.mkdir(parents=True, exist_ok=True)
        dest = root_arch / name
        shutil.copy2(svg, dest)
        written.append(dest)
    return written
