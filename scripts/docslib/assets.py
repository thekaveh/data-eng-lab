"""Copy committed diagram PNGs into the legacy wiki surface."""
from __future__ import annotations

import shutil
from pathlib import Path


def _canonical(repo_root: Path) -> Path:
    return repo_root / "docs" / "diagrams" / "img"


def copy_assets(model, repo_root: Path, wiki_dir: Path) -> list[Path]:
    """Copy committed PNG projections into the wiki root used by rewritten links."""
    canon = _canonical(repo_root)
    written: list[Path] = []
    if not canon.exists():
        return written
    wiki_dir.mkdir(parents=True, exist_ok=True)
    for source in sorted(canon.glob("*.png")):
        destination = wiki_dir / source.name
        shutil.copy2(source, destination)
        written.append(destination)
    return written
