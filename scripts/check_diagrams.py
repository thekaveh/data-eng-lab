#!/usr/bin/env python3
"""Validate canonical diagram manifest entries, HTML masters, and PNG projections."""

from __future__ import annotations

import argparse
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.docs.manifest import ManifestError, parse_manifest  # noqa: E402
from scripts.docs.render_diagrams import DiagramError, extract_svg  # noqa: E402

_SVG_OPEN = re.compile(r"<svg\b")
_PNG_MAGIC = b"\x89PNG\r\n\x1a\n"


def _set_findings(label: str, expected: set[str], actual: set[str]) -> list[str]:
    findings: list[str] = []
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    if missing:
        findings.append(f"{label} missing ids: {', '.join(missing)}")
    if extra:
        findings.append(f"{label} unexpected ids: {', '.join(extra)}")
    return findings


def _svg_dimensions(svg: str) -> tuple[float, float] | None:
    try:
        root = ET.fromstring(svg)
    except ET.ParseError:
        return None
    view_box = root.attrib.get("viewBox")
    if view_box:
        parts = re.split(r"[\s,]+", view_box.strip())
        if len(parts) == 4:
            try:
                return float(parts[2]), float(parts[3])
            except ValueError:
                return None
    try:
        return float(root.attrib["width"]), float(root.attrib["height"])
    except (KeyError, ValueError):
        return None


def _check_master(path: Path, identifier: str) -> list[str]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return [f"{identifier}: missing master {path}"]
    if len(_SVG_OPEN.findall(text)) != 1:
        return [f"{identifier}: master must contain exactly one inline SVG"]
    try:
        svg = extract_svg(text)
    except DiagramError as error:
        return [f"{identifier}: {error}"]
    dimensions = _svg_dimensions(svg)
    if dimensions is None:
        return [f"{identifier}: SVG has invalid or missing dimensions"]
    width, height = dimensions
    if width <= 0 or height <= 0 or width <= height:
        return [f"{identifier}: SVG is not landscape ({width}x{height})"]
    return []


def _check_png(path: Path, identifier: str) -> list[str]:
    try:
        data = path.read_bytes()
    except OSError:
        return [f"{identifier}: missing PNG {path}"]
    if not data.startswith(_PNG_MAGIC):
        return [f"{identifier}: invalid PNG magic"]
    if len(data) < 24 or data[12:16] != b"IHDR":
        return [f"{identifier}: invalid PNG header"]
    width = int.from_bytes(data[16:20], "big")
    height = int.from_bytes(data[20:24], "big")
    if width == 0 or height == 0:
        return [f"{identifier}: PNG dimensions must be nonzero"]
    return []


def _scan(root: Path) -> list[str]:
    manifest_path = root / "docs" / "manifest.yaml"
    try:
        manifest = parse_manifest(manifest_path.read_text(encoding="utf-8"))
    except OSError:
        return ["docs/manifest.yaml missing"]
    except ManifestError as error:
        return [f"docs/manifest.yaml invalid: {error}"]

    entries = {entry.id: entry for entry in manifest.diagrams}
    expected = set(entries)
    if not expected:
        return ["docs/manifest.yaml must declare at least one diagram"]
    masters_dir = root / "docs" / "diagrams"
    png_dir = masters_dir / "img"
    master_ids = {path.stem for path in masters_dir.glob("*.html")} if masters_dir.exists() else set()
    png_ids = {path.stem for path in png_dir.glob("*.png")} if png_dir.exists() else set()

    findings = _set_findings("HTML masters", expected, master_ids)
    findings.extend(_set_findings("PNG projections", expected, png_ids))
    for identifier, entry in entries.items():
        expected_master = Path(f"docs/diagrams/{identifier}.html")
        if entry.master != expected_master:
            findings.append(f"{identifier}: manifest master must be {expected_master}, got {entry.master}")
        master = root / expected_master
        png = png_dir / f"{identifier}.png"
        if identifier in master_ids:
            findings.extend(_check_master(master, identifier))
        if identifier in png_ids:
            findings.extend(_check_png(png, identifier))
    return findings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".", type=Path)
    args = parser.parse_args(argv)
    findings = _scan(args.root.resolve())
    for finding in findings:
        print(f"DIAGRAM: {finding}", file=sys.stderr)
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
