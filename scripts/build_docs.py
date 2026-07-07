#!/usr/bin/env python3
"""Build all three doc surfaces from docs/ (SOT).

    uv run --group dev python scripts/build_docs.py [--check] [--stage STAGE] [--root ROOT] [--wiki-dir DIR]
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

# Bootstrap: docslib is a top-level package living in scripts/. Ensure scripts/ is on sys.path
# so this works whether run as `python scripts/build_docs.py` or `uv run ... python scripts/build_docs.py`.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from docslib.assets import copy_assets  # noqa: E402
from docslib.parse import build_doc_map, parse_site  # noqa: E402
from docslib.render_readme import render_readme_surface  # noqa: E402
from docslib.render_wiki import render_wiki_surface  # noqa: E402


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


def _expected_outputs(model) -> list[str]:
    out = ["README.md"]
    for s in model.scenarios:
        out += [f"scenarios/{s.name}/README.md", f"scenarios/{s.name}/notebooks.md"]
    for a in model.apps:
        out.append(f"spark-apps/{a.src.stem}/README.md")
    return out


def _check(repo_root: Path, wiki_dir: Path) -> int:
    """Re-build into a temp dir; diff README-surface outputs against committed. 0 = clean.

    Wiki output and asset copies are gitignored/transient, so only the in-repo README surface
    (README.md, scenarios/*/README.md, scenarios/*/notebooks.md, spark-apps/*/README.md) is diffed.
    """
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


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Build README + Wiki + asset surfaces from docs/.")
    ap.add_argument("--root", default=".", type=Path)
    ap.add_argument("--wiki-dir", default="wiki", type=Path)
    ap.add_argument("--stage", default="all", choices=["all", "readme", "wiki", "assets"])
    ap.add_argument("--check", action="store_true",
                    help="rebuild into a temp dir and diff README-surface outputs; exit 0 if clean")
    args = ap.parse_args(argv)
    repo_root = args.root.resolve()
    wiki_dir = args.wiki_dir.resolve() if args.wiki_dir.is_absolute() else (repo_root / args.wiki_dir).resolve()
    if args.check:
        return _check(repo_root, wiki_dir)
    _build(repo_root, wiki_dir, args.stage)
    print(f"built {args.stage} → {repo_root}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
