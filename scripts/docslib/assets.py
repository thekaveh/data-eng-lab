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
