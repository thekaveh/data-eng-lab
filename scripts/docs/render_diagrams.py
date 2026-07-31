"""Render canonical HTML diagram masters into site SVGs and repository PNGs."""

from __future__ import annotations

import argparse
import html
import re
import shutil
from pathlib import Path

from scripts.docs.manifest import Manifest, load_manifest


class DiagramError(ValueError):
    """Raised when a diagram master or projection violates its contract."""


_SVG_ROOT = re.compile(r"<svg\b[^>]*>.*?</svg>", re.DOTALL)
_HTML_NAMED_ENTITY = re.compile(r"&([A-Za-z][A-Za-z0-9]+);")
_XML_NAMED_ENTITIES = {"amp", "apos", "gt", "lt", "quot"}


def import_svg_master(svg_text: str, *, title: str, evidence: str) -> str:
    """Wrap one standalone legacy SVG in the canonical HTML master shape."""
    svg = svg_text.strip()
    if not svg.startswith("<svg") or not svg.endswith("</svg>") or len(_SVG_ROOT.findall(svg)) != 1:
        raise DiagramError("legacy diagram must contain one standalone svg root")
    return (
        "<!doctype html>\n"
        '<html lang="en">\n'
        '<head><meta charset="utf-8">'
        f"<title>{html.escape(title)}</title></head>\n"
        "<body>\n"
        f"  <!-- {html.escape(evidence)} -->\n"
        f"{svg}\n"
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

    return _HTML_NAMED_ENTITY.sub(sanitize, roots[0])


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
