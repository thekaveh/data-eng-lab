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
            # mirror the README-surface SVG check: every embedded SVG must exist locally
            # in the wiki dir (render_wiki rewrites diagrams to bare "<name>.svg" refs).
            for m in SVG_REF.finditer(text):
                svg = f.parent / m.group(1)
                if not svg.exists():
                    findings.append(f"wiki/{f.name}: missing local SVG {m.group(1)}")
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
