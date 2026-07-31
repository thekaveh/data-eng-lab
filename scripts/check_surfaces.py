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
DOCS_TARGET = re.compile(r"^(?:\.\./)*docs/")


def _allowed_docs_image(root: Path, source: Path, bang: str, target: str) -> bool:
    """Return whether a docs/-relative target is the committed PNG exception."""
    if bang != "!" or not target.endswith(".png"):
        return False
    canonical = (root / "docs" / "diagrams" / "img").resolve()
    resolved = (source.parent / target).resolve()
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
            if DOCS_TARGET.match(target) and not _allowed_docs_image(root, f, bang, target):
                findings.append(f"{f.relative_to(root)}: forbidden docs/-relative target {target}")
            if bang == "!" and target.endswith((".png", ".svg")) and not (f.parent / target).is_file():
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
                if bang == "!" and target.endswith((".png", ".svg")) and not (f.parent / target).is_file():
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
