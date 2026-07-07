#!/usr/bin/env python3
"""Assert every canonical diagram in docs/architectures/ is a landscape, script-free SVG.

The manifest (scripts/diagrams_manifest.yaml) is the declarative input for
generation (Task 11); this checker validates the *output*: every `*.svg` under
``docs/architectures/`` must (a) carry explicit width/height, (b) be landscape
(width > height), and (c) contain no ``<script>`` (GitHub refuses to render
inline JS, so such an SVG would render as a broken image).

Exit code 0 = clean, 1 = one or more findings (printed to stderr).
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# width="..." / height="..." — anchored on the attribute so we don't trip on
# stray "width" substrings inside styling or class names.
_W = re.compile(r'\bwidth="([\d.]+)"')
_H = re.compile(r'\bheight="([\d.]+)"')


def _scan(root: Path) -> list[str]:
    """Return a list of human-readable findings for SVGs under root/docs/architectures/."""
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
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", default=".", type=Path)
    args = ap.parse_args(argv)
    findings = _scan(args.root.resolve())
    for f in findings:
        print(f"DIAGRAM: {f}", file=sys.stderr)
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
