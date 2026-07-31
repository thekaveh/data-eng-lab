"""Run the canonical three-surface documentation gate."""

from __future__ import annotations

import argparse
import re
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from scripts.docs.build_docs import DocumentationDrift, build
from scripts.docs.links import (
    MARKDOWN_LINK_RE,
    PAGES_ORIGIN,
    REPOSITORY_ORIGIN,
    WIKI_ORIGIN,
    find_links,
    is_forbidden,
)
from scripts.docs.manifest import Manifest, ManifestError, iter_leaf_sections, load_manifest
from scripts.docs.render_diagrams import DiagramError, extract_svg

_INTERNAL_ROOT = Path("docs/superpowers")
_UNFINISHED_MARKERS = ("TO" + "DO", "TB" + "D", "FIX" + "ME", "X" + "XX")
_H1 = re.compile(r"^# .+$", re.MULTILINE)
_SVG_OPEN = re.compile(r"<svg\b")
_PNG_MAGIC = b"\x89PNG\r\n\x1a\n"
_MIRROR_BANNER = "Full docs site"


@dataclass(frozen=True)
class Finding:
    """One documentation-gate result."""

    severity: str
    message: str


def check_completeness(repo_root: Path) -> tuple[Finding, ...]:
    """Reject public Markdown under ``docs/`` that the manifest does not own."""
    root = repo_root.resolve()
    manifest, findings = _load(root)
    if manifest is None:
        return findings
    declared = {
        section.source
        for section in iter_leaf_sections(manifest.sections)
        if section.source is not None
    }
    for path in sorted((root / "docs").rglob("*.md")):
        relative = path.relative_to(root)
        if _is_internal(relative):
            continue
        if relative not in declared:
            findings += (_error(f"public Markdown is absent from manifest: {relative.as_posix()}"),)
    return _sorted(findings)


def check_numbering(repo_root: Path) -> tuple[Finding, ...]:
    """Cross-check every manifest number and title against its source's first H1."""
    root = repo_root.resolve()
    manifest, findings = _load(root)
    if manifest is None:
        return findings
    for section in iter_leaf_sections(manifest.sections):
        assert section.source is not None
        text = (root / section.source).read_text(encoding="utf-8")
        heading = _H1.search(text)
        expected = f"# {section.number}. {section.title}"
        if heading is None or not heading.group(0).startswith(expected):
            findings += (
                _error(
                    f"{section.source.as_posix()} heading must start with {expected!r}"
                ),
            )
    return _sorted(findings)


def check_placeholders(repo_root: Path) -> tuple[Finding, ...]:
    """Reject standard unfinished-work markers in canonical public Markdown."""
    root = repo_root.resolve()
    findings: tuple[Finding, ...] = ()
    for path in _canonical_markdown(root):
        text = path.read_text(encoding="utf-8")
        relative = path.relative_to(root).as_posix()
        for marker in _UNFINISHED_MARKERS:
            if re.search(rf"\b{re.escape(marker)}\b", text):
                findings += (
                    _error(f"unfinished marker {marker} in public documentation: {relative}"),
                )
    return _sorted(findings)


def check_empty_artifacts(repo_root: Path) -> tuple[Finding, ...]:
    """Reject empty files and directories in the public canonical docs tree."""
    root = repo_root.resolve()
    docs = root / "docs"
    findings: tuple[Finding, ...] = ()
    if not docs.exists():
        return (_error("public documentation directory missing: docs"),)
    for path in sorted(docs.rglob("*")):
        relative = path.relative_to(root)
        if _is_internal(relative):
            continue
        if path.is_file() and path.stat().st_size == 0:
            findings += (
                _error(f"empty public documentation file: {relative.as_posix()}"),
            )
        elif path.is_dir() and not any(path.iterdir()):
            findings += (
                _error(f"empty public documentation directory: {relative.as_posix()}"),
            )
    return _sorted(findings)


def check_self_containment(repo_root: Path) -> tuple[Finding, ...]:
    """Reject cross-surface links and missing surface-local image targets."""
    root = repo_root.resolve()
    findings: tuple[Finding, ...] = ()
    for surface, path, restrict_docs in _surface_files(root):
        text = path.read_text(encoding="utf-8")
        relative = path.relative_to(root).as_posix()
        findings += _origin_findings(
            relative,
            text,
            surface,
            scan_plain_urls=path.suffix != ".md",
        )
        if surface == "wiki" and _MIRROR_BANNER in text:
            findings += (_error(f"{relative}: contains mirror banner"),)
        if path.suffix != ".md":
            continue
        for match in MARKDOWN_LINK_RE.finditer(text):
            target = match.group("target")
            # Exercise the shared classifier as the authoritative origin matrix.
            if is_forbidden(target, surface):
                continue
            local = _resolve_local_target(path, target)
            if local is None:
                continue
            clean, resolved = local
            if restrict_docs and resolved.is_relative_to((root / "docs").resolve()):
                if not _allowed_docs_image(root, path, match.group(0), clean, resolved):
                    findings += (
                        _error(f"{relative}: forbidden docs/-relative target {target}"),
                    )
            if match.group(0).startswith("!") and Path(clean).suffix in {".png", ".svg"}:
                if not resolved.is_file():
                    findings += (_error(f"{relative}: missing local image {target}"),)
    return _sorted(findings)


def check_diagrams(repo_root: Path) -> tuple[Finding, ...]:
    """Validate manifest inventory plus HTML, PNG, and generated SVG projections."""
    root = repo_root.resolve()
    manifest, findings = _load(root)
    if manifest is None:
        return findings
    entries = {entry.id: entry for entry in manifest.diagrams}
    expected = set(entries)
    if not expected:
        return (_error("docs/manifest.yaml must declare at least one diagram"),)

    masters_dir = root / "docs/diagrams"
    png_dir = masters_dir / "img"
    svg_dir = root / "generated/site/assets/img"
    master_ids = {path.stem for path in masters_dir.glob("*.html")}
    png_ids = {path.stem for path in png_dir.glob("*.png")}
    svg_ids = {path.stem for path in svg_dir.glob("*.svg")}
    findings += _set_findings("HTML masters", expected, master_ids)
    findings += _set_findings("PNG projections", expected, png_ids)
    findings += _set_findings("site SVG projections", expected, svg_ids)

    for identifier, entry in entries.items():
        expected_master = Path(f"docs/diagrams/{identifier}.html")
        if entry.master != expected_master:
            findings += (
                _error(
                    f"{identifier}: manifest master must be {expected_master}, got {entry.master}"
                ),
            )
        if identifier in master_ids:
            findings += _check_master(root / expected_master, identifier)
        if identifier in png_ids:
            findings += _check_png(png_dir / f"{identifier}.png", identifier)
        if identifier in svg_ids:
            svg_path = svg_dir / f"{identifier}.svg"
            try:
                svg = svg_path.read_text(encoding="utf-8")
            except OSError:
                findings += (_error(f"{identifier}: missing site SVG {svg_path}"),)
            else:
                if not _SVG_OPEN.search(svg):
                    findings += (_error(f"{identifier}: invalid site SVG projection"),)
    return _sorted(findings)


def check(repo_root: Path) -> tuple[Finding, ...]:
    """Render both generated surfaces and return every aggregate-gate finding."""
    root = repo_root.resolve()
    findings: tuple[Finding, ...] = ()
    try:
        build(root, site=True, wiki=True, check=True)
    except (DiagramError, DocumentationDrift, ManifestError, OSError, ValueError) as error:
        findings += (_error(f"documentation render failed: {error}"),)

    for probe in (
        check_completeness,
        check_numbering,
        check_self_containment,
        check_placeholders,
        check_empty_artifacts,
        check_diagrams,
    ):
        try:
            findings += probe(root)
        except (DiagramError, ManifestError, OSError, ValueError) as error:
            findings += (_error(f"{probe.__name__} failed: {error}"),)
    return _sorted(tuple(set(findings)))


def _load(root: Path) -> tuple[Manifest | None, tuple[Finding, ...]]:
    try:
        return load_manifest(root / "docs/manifest.yaml", root), ()
    except ManifestError as error:
        return None, (_error(f"docs/manifest.yaml invalid: {error}"),)


def _canonical_markdown(root: Path) -> tuple[Path, ...]:
    candidates: set[Path] = set()
    readme = root / "README.md"
    if readme.is_file():
        candidates.add(readme)
    docs = root / "docs"
    if docs.exists():
        candidates.update(
            path
            for path in docs.rglob("*.md")
            if not _is_internal(path.relative_to(root))
        )
    for directory in (root / "scenarios", root / "spark-apps"):
        if directory.exists():
            candidates.update(directory.rglob("*.md"))
    return tuple(sorted(candidates))


def _surface_files(root: Path) -> tuple[tuple[str, Path, bool], ...]:
    values: list[tuple[str, Path, bool]] = []
    for path in _canonical_markdown(root):
        restrict_docs = path == root / "README.md" or path.is_relative_to(root / "scenarios") or path.is_relative_to(
            root / "spark-apps"
        )
        values.append(("repo", path, restrict_docs))
    site = root / "generated/site"
    if site.exists():
        values.extend(("site", path, False) for path in sorted(site.rglob("*.md")))
    config = root / "mkdocs.yml"
    if config.is_file():
        values.append(("site", config, False))
    wiki = root / "generated/wiki"
    if wiki.exists():
        values.extend(("wiki", path, False) for path in sorted(wiki.rglob("*.md")))
    return tuple(values)


def _origin_findings(
    relative: str,
    text: str,
    surface: str,
    *,
    scan_plain_urls: bool,
) -> tuple[Finding, ...]:
    findings: tuple[Finding, ...] = ()
    label = "repository" if surface == "repo" else surface
    # Calling find_links keeps link parsing centralized; plain origins remain checked
    # to preserve the interim gate's protection against bare URLs and config values.
    targets = tuple(link.target for link in find_links(text))
    searchable = "\n".join((text if scan_plain_urls else "", *targets))
    if surface != "wiki" and WIKI_ORIGIN in searchable:
        findings += (_error(f"{relative}: {label} surface links to the wiki surface"),)
    without_wiki = searchable.replace(WIKI_ORIGIN, "")
    if surface != "repo" and REPOSITORY_ORIGIN in without_wiki:
        findings += (_error(f"{relative}: {label} surface links to the repository surface"),)
    pages_host = urlsplit(PAGES_ORIGIN).netloc
    # The interim repository gate also rejected a bare Pages URL, not only a
    # Markdown link. Preserve that behavior for canonical repository files.
    pages_searchable = text if surface != "site" else searchable
    if surface != "site" and pages_host in pages_searchable:
        findings += (_error(f"{relative}: {label} surface links to the site surface"),)
    return findings


def _resolve_local_target(source: Path, target: str) -> tuple[str, Path] | None:
    parsed = urlsplit(target)
    if parsed.scheme or parsed.netloc or target.startswith("//"):
        return None
    clean = parsed.path
    if not clean:
        return None
    return clean, (source.parent / clean).resolve()


def _escapes_root(root: Path, source: Path, target: str) -> bool:
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
    markdown: str,
    target: str,
    resolved: Path,
) -> bool:
    if not markdown.startswith("!") or Path(target).suffix != ".png":
        return False
    if _escapes_root(root, source, target):
        return False
    canonical = (root / "docs/diagrams/img").resolve()
    return resolved.parent == canonical and resolved.is_file()


def _set_findings(label: str, expected: set[str], actual: set[str]) -> tuple[Finding, ...]:
    findings: tuple[Finding, ...] = ()
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    if missing:
        findings += (_error(f"{label} missing ids: {', '.join(missing)}"),)
    if extra:
        findings += (_error(f"{label} unexpected ids: {', '.join(extra)}"),)
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


def _check_master(path: Path, identifier: str) -> tuple[Finding, ...]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return (_error(f"{identifier}: missing master {path}"),)
    if len(_SVG_OPEN.findall(text)) != 1:
        return (_error(f"{identifier}: master must contain exactly one inline SVG"),)
    try:
        svg = extract_svg(text)
    except DiagramError as error:
        return (_error(f"{identifier}: {error}"),)
    dimensions = _svg_dimensions(svg)
    if dimensions is None:
        return (_error(f"{identifier}: SVG has invalid or missing dimensions"),)
    width, height = dimensions
    if width <= 0 or height <= 0 or width <= height:
        return (_error(f"{identifier}: SVG is not landscape ({width}x{height})"),)
    return ()


def _check_png(path: Path, identifier: str) -> tuple[Finding, ...]:
    try:
        data = path.read_bytes()
    except OSError:
        return (_error(f"{identifier}: missing PNG {path}"),)
    if not data.startswith(_PNG_MAGIC):
        return (_error(f"PNG projection {identifier} has invalid PNG magic"),)
    if len(data) < 24 or data[12:16] != b"IHDR":
        return (_error(f"PNG projection {identifier} has invalid PNG header"),)
    width = int.from_bytes(data[16:20], "big")
    height = int.from_bytes(data[20:24], "big")
    if width == 0 or height == 0:
        return (_error(f"PNG projection {identifier} dimensions must be nonzero"),)
    return ()


def _is_internal(relative: Path) -> bool:
    return relative == _INTERNAL_ROOT or relative.is_relative_to(_INTERNAL_ROOT)


def _error(message: str) -> Finding:
    return Finding("error", message)


def _sorted(findings: tuple[Finding, ...]) -> tuple[Finding, ...]:
    return tuple(sorted(findings, key=lambda finding: (finding.message, finding.severity)))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=Path.cwd(), type=Path)
    args = parser.parse_args(argv)
    findings = check(args.root)
    for finding in findings:
        print(f"{finding.severity.upper()}: {finding.message}", file=sys.stderr)
    return 1 if any(finding.severity == "error" for finding in findings) else 0


if __name__ == "__main__":
    raise SystemExit(main())
