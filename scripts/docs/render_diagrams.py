"""Render canonical HTML diagram masters into site SVGs and repository PNGs."""

from __future__ import annotations

import argparse
import html
import re
import shutil
import xml.etree.ElementTree as ET
from pathlib import Path

from scripts.docs.manifest import Manifest, load_manifest


class DiagramError(ValueError):
    """Raised when a diagram master or projection violates its contract."""


_SVG_ROOT = re.compile(r"<svg\b[^>]*>.*?</svg>", re.DOTALL)
_HTML_NAMED_ENTITY = re.compile(r"&([A-Za-z][A-Za-z0-9]+);")
_XML_NAMED_ENTITIES = {"amp", "apos", "gt", "lt", "quot"}
_FORBIDDEN_ELEMENTS = {
    "animate",
    "animatemotion",
    "animatetransform",
    "embed",
    "foreignobject",
    "iframe",
    "object",
    "script",
    "set",
    "style",
}
_FORBIDDEN_VALUE_FRAGMENTS = (
    "javascript:",
    "vbscript:",
    "data:",
    "@import",
    "expression(",
    "-moz-binding",
)
_URI_OR_STYLE_ATTRIBUTES = {
    "clip-path",
    "cursor",
    "fill",
    "filter",
    "href",
    "mask",
    "src",
    "stroke",
    "style",
}


def import_svg_master(svg_text: str, *, title: str, evidence: str) -> str:
    """Wrap one standalone legacy SVG in the canonical HTML master shape."""
    svg = svg_text.strip()
    if not svg.startswith("<svg") or not svg.endswith("</svg>") or len(_SVG_ROOT.findall(svg)) != 1:
        raise DiagramError("legacy diagram must contain one standalone svg root")
    open_end = svg.find(">")
    accessible_svg = (
        svg[:open_end]
        + ' role="img" aria-labelledby="diagram-title diagram-description">\n'
        + f'  <title id="diagram-title">{html.escape(title)}</title>\n'
        + f'  <desc id="diagram-description">{html.escape(evidence)}</desc>\n'
        + svg[open_end + 1 :]
    )
    return (
        "<!doctype html>\n"
        '<html lang="en">\n'
        '<head><meta charset="utf-8">'
        f"<title>{html.escape(title)}</title></head>\n"
        "<body>\n"
        f"  <!-- {html.escape(evidence)} -->\n"
        f"{accessible_svg}\n"
        "</body>\n"
        "</html>\n"
    )


def extract_svg(html_text: str) -> str:
    """Extract one complete inline SVG and make HTML entities XML-safe."""
    roots = _SVG_ROOT.findall(html_text)
    if len(roots) != 1:
        raise DiagramError("diagram master must contain exactly one inline svg")

    def sanitize(match: re.Match[str]) -> str:
        name = match.group(1)
        return match.group(0) if name in _XML_NAMED_ENTITIES else html.unescape(match.group(0))

    svg = _HTML_NAMED_ENTITY.sub(sanitize, roots[0])
    _validate_safe_svg(svg)
    return svg


def _validate_safe_svg(svg: str) -> None:
    if "<!doctype" in svg.lower() or "<!entity" in svg.lower():
        raise DiagramError("unsafe SVG declaration")
    try:
        root = ET.fromstring(svg)
    except ET.ParseError as error:
        raise DiagramError(f"invalid SVG XML: {error}") from error

    for element in root.iter():
        tag = _local_name(element.tag).lower()
        if tag in _FORBIDDEN_ELEMENTS:
            raise DiagramError(f"unsafe SVG element: {tag}")
        for raw_name, value in element.attrib.items():
            name = _local_name(raw_name).lower()
            if name.startswith("on"):
                raise DiagramError(f"unsafe SVG event attribute: {name}")
            if name == "style":
                raise DiagramError("unsafe SVG style attribute")
            if name in _URI_OR_STYLE_ATTRIBUTES and _contains_executable_value(value):
                raise DiagramError(f"unsafe SVG attribute value: {name}")


def _local_name(name: str) -> str:
    return name.rsplit("}", 1)[-1]


def _contains_executable_value(value: str) -> bool:
    normalized = "".join(
        character
        for character in html.unescape(value).lower()
        if not character.isspace() and ord(character) >= 0x20
    )
    return any(fragment in normalized for fragment in _FORBIDDEN_VALUE_FRAGMENTS)


def svg_to_png(svg: str, destination: Path, *, width: int = 1600) -> None:
    """Render an SVG string to a PNG at a deterministic output width."""
    import cairosvg

    destination.parent.mkdir(parents=True, exist_ok=True)
    cairosvg.svg2png(bytestring=svg.encode("utf-8"), write_to=str(destination), output_width=width)


def render_all(manifest: Manifest, repo_root: Path, site_img_dir: Path, png_dir: Path) -> None:
    """Render every manifest diagram to its site SVG and committed PNG projections."""
    site_img_dir.mkdir(parents=True, exist_ok=True)
    png_dir.mkdir(parents=True, exist_ok=True)
    for diagram in manifest.diagrams:
        master = (repo_root / diagram.master).read_text(encoding="utf-8")
        svg = extract_svg(master)
        (site_img_dir / f"{diagram.id}.svg").write_text(f"{svg}\n", encoding="utf-8")
        svg_to_png(svg, png_dir / f"{diagram.id}.png")


def copy_assets(png_dir: Path, wiki_img_dir: Path) -> None:
    """Copy committed PNG projections into the generated wiki image directory."""
    wiki_img_dir.mkdir(parents=True, exist_ok=True)
    for source in sorted(png_dir.glob("*.png")):
        shutil.copyfile(source, wiki_img_dir / source.name)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    repo_root = args.root.resolve()
    manifest = load_manifest(repo_root / "docs/manifest.yaml", repo_root)
    render_all(
        manifest,
        repo_root,
        repo_root / "generated/site/assets/img",
        repo_root / "docs/diagrams/img",
    )


if __name__ == "__main__":
    main()
