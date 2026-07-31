#!/usr/bin/env python3
"""Assert the three doc surfaces are self-contained and their image targets resolve."""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

SITE = "thekaveh.github.io"
BANNER = "Full docs site"
MARKDOWN_LINK = re.compile(r"(!?)\[[^\]]*\]\(([^)]+)\)")
URI_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")


def _resolve_local_target(source: Path, target: str) -> tuple[str, Path] | None:
    """Strip URL decorations and resolve a local target without requiring it to exist."""
    clean = re.split(r"[?#]", target, maxsplit=1)[0]
    if not clean or clean.startswith("//") or URI_SCHEME.match(clean):
        return None
    return clean, (source.parent / clean).resolve()


def _escapes_root(root: Path, source: Path, target: str) -> bool:
    """Return whether lexical traversal leaves root before reaching its final target."""
    path = Path(target)
    if path.is_absolute():
        return True
    depth = len(source.parent.resolve().relative_to(root).parts)
    for part in path.parts:
        if part in ("", "."):
            continue
        if part == "..":
            if depth == 0:
                return True
            depth -= 1
        else:
            depth += 1
    return False


def _allowed_docs_image(
    root: Path,
    source: Path,
    bang: str,
    target: str,
    resolved: Path,
) -> bool:
    """Return whether a resolved docs target is the committed PNG exception."""
    if bang != "!" or Path(target).suffix != ".png" or _escapes_root(root, source, target):
        return False
    canonical = (root / "docs" / "diagrams" / "img").resolve()
    return resolved.parent == canonical and resolved.is_file()


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
        for match in MARKDOWN_LINK.finditer(text):
            bang, target = match.groups()
            local = _resolve_local_target(f, target)
            if local is None:
                continue
            clean, resolved = local
            docs = (root / "docs").resolve()
            if resolved.is_relative_to(docs) and not _allowed_docs_image(
                root, f, bang, clean, resolved
            ):
                findings.append(f"{f.relative_to(root)}: forbidden docs/-relative target {target}")
            if bang == "!" and Path(clean).suffix in (".png", ".svg") and not resolved.is_file():
                findings.append(f"{f.relative_to(root)}: missing local image {target}")
    # wiki surface
    wdir = root / "wiki"
    if wdir.exists():
        for f in wdir.glob("*.md"):
            text = f.read_text(encoding="utf-8")
            if SITE in text:
                findings.append(f"wiki/{f.name}: links to .io site")
            if BANNER in text:
                findings.append(f"wiki/{f.name}: contains mirror banner")
            for match in MARKDOWN_LINK.finditer(text):
                bang, target = match.groups()
                local = _resolve_local_target(f, target)
                if local is None:
                    continue
                clean, resolved = local
                if bang == "!" and Path(clean).suffix in (".png", ".svg") and not resolved.is_file():
                    findings.append(f"wiki/{f.name}: missing local image {target}")
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
