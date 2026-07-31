"""Run the canonical three-surface documentation gate."""

from __future__ import annotations

import argparse
import re
import sys
import unicodedata
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlsplit

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
from scripts.docs.render_diagrams import (
    DiagramError,
    diagram_fingerprint,
    extract_svg,
    projection_fingerprint_path,
)

_UNFINISHED_MARKERS = ("TO" + "DO", "TB" + "D", "FIX" + "ME", "X" + "XX")
_H1 = re.compile(r"^ {0,3}(# [^\r\n]+)$", re.MULTILINE)
_SVG_OPEN = re.compile(r"<svg\b")
_PNG_MAGIC = b"\x89PNG\r\n\x1a\n"
_MIRROR_BANNER = "Full docs site"
_BRACKETED_INLINE_MARKDOWN_PATH = re.compile(
    r"(?<!!)\[`(?P<path>[^`\r\n]+\.md(?:#[^`\r\n]+)?)`\](?!\s*(?:\(|\[|:))"
)


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
        if _is_internal(relative, manifest.internal_roots):
            continue
        if relative not in declared:
            findings += (_error(f"public Markdown is absent from manifest: {relative.as_posix()}"),)
    for path in sorted((root / "scenarios").glob("*/notebooks.md")):
        findings += (
            _error(
                "legacy scenario notebook documentation is not manifest-owned: "
                f"{path.relative_to(root).as_posix()}"
            ),
        )

    scenarios_root = root / "scenarios"
    scenario_ids = (
        {
            path.name
            for path in scenarios_root.iterdir()
            if path.is_dir()
            and (path / "jupyter/notebook.ipynb").is_file()
            and (path / "zeppelin/notebook.zpln").is_file()
        }
        if scenarios_root.is_dir()
        else set()
    )
    notebook_sources = {
        path
        for path in declared
        if path.parent == Path("docs/notebooks") and path.name != "index.md"
    }
    documented_ids = {path.stem for path in notebook_sources}
    for identifier in sorted(scenario_ids - documented_ids):
        findings += (
            _error(
                "scenario notebook documentation absent from manifest: "
                f"docs/notebooks/{identifier}.md"
            ),
        )
    for identifier in sorted(documented_ids - scenario_ids):
        findings += (
            _error(
                "manifest notebook documentation has no paired scenario notebooks: "
                f"docs/notebooks/{identifier}.md"
            ),
        )
    return _sorted(findings)


def check_numbering(repo_root: Path) -> tuple[Finding, ...]:
    """Cross-check every manifest number and title against its source's first H1."""
    root = repo_root.resolve()
    manifest, findings = _load(root)
    if manifest is None:
        return findings
    for section in iter_leaf_sections(manifest.sections):
        assert section.source is not None
        text = _without_fenced_code((root / section.source).read_text(encoding="utf-8"))
        heading = _H1.search(text)
        expected = f"# {section.number}. {section.title}"
        if heading is None or not heading.group(1).startswith(expected):
            findings += (
                _error(
                    f"{section.source.as_posix()} heading must start with {expected!r}"
                ),
            )
    return _sorted(findings)


def check_placeholders(repo_root: Path) -> tuple[Finding, ...]:
    """Reject unfinished-work markers in canonical and generated public artifacts."""
    root = repo_root.resolve()
    manifest, findings = _load(root)
    if manifest is None:
        return findings
    for path in _placeholder_files(root, manifest.internal_roots):
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
    manifest, findings = _load(root)
    if manifest is None:
        return findings
    docs = root / "docs"
    if not docs.exists():
        return (_error("public documentation directory missing: docs"),)
    for path in sorted(docs.rglob("*")):
        relative = path.relative_to(root)
        if _is_internal(relative, manifest.internal_roots):
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
    """Reject cross-surface links and invalid surface-local targets/fragments."""
    root = repo_root.resolve()
    manifest, findings = _load(root)
    if manifest is None:
        return findings
    for surface, path in _surface_files(root, manifest.internal_roots):
        text = path.read_text(encoding="utf-8")
        relative = path.relative_to(root).as_posix()
        findings += _origin_findings(relative, text, surface)
        if surface == "wiki" and _MIRROR_BANNER in text:
            findings += (_error(f"{relative}: contains mirror banner"),)
        if path.suffix != ".md":
            continue
        for malformed in _malformed_inline_path_links(text):
            findings += (
                _error(
                    f"{relative}: malformed Markdown link around inline path "
                    f"{malformed}"
                ),
            )
        for match in MARKDOWN_LINK_RE.finditer(_without_fenced_code(text)):
            target = match.group("target")
            # Exercise the shared classifier as the authoritative origin matrix.
            if is_forbidden(target, surface):
                continue
            local = _resolve_local_target(path, target)
            if local is None:
                continue
            clean, fragment, resolved = local
            is_image = match.group(0).startswith("!")
            surface_root = {
                "repo": root,
                "site": root / "generated/site",
                "wiki": root / "generated/wiki",
            }[surface].resolve()
            if _escapes_root(surface_root, path, clean) or not resolved.is_relative_to(surface_root):
                label = "repository" if surface == "repo" else surface
                kind = "image" if is_image else "target"
                findings += (
                    _error(f"{relative}: local {kind} escapes {label} surface: {target}"),
                )
                continue
            if surface == "repo":
                target_relative = resolved.relative_to(root)
                if _is_internal(target_relative, manifest.internal_roots):
                    findings += (
                        _error(
                            f"{relative}: local target enters internal documentation: "
                            f"{target_relative.as_posix()}"
                        ),
                    )
                    continue
            if not resolved.is_file():
                kind = "image" if is_image else "target"
                findings += (_error(f"{relative}: missing local {kind} {target}"),)
                continue
            if fragment and not is_image and resolved.suffix.casefold() in {".md", ".markdown"}:
                anchors = _markdown_anchors(resolved.read_text(encoding="utf-8"))
                if fragment not in anchors and fragment.removeprefix("user-content-") not in anchors:
                    target_name = resolved.relative_to(surface_root).as_posix()
                    findings += (
                        _error(
                            f"{relative}: missing local fragment #{fragment} in {target_name}"
                        ),
                    )
    return _sorted(findings)


def _malformed_inline_path_links(markdown: str) -> tuple[str, ...]:
    """Return bracketed inline-code paths that have no link target or definition."""
    visible = _without_fenced_code(markdown)
    findings: list[str] = []
    for match in _BRACKETED_INLINE_MARKDOWN_PATH.finditer(visible):
        label = re.escape(match.group(0))
        if re.search(rf"^\s*{label}\s*:", visible, flags=re.MULTILINE):
            continue
        findings.append(match.group("path"))
    return tuple(findings)


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
    fingerprint_ids = {path.stem for path in png_dir.glob("*.sha256")}
    svg_ids = {path.stem for path in svg_dir.glob("*.svg")}
    findings += _set_findings("HTML masters", expected, master_ids)
    findings += _set_findings("PNG projections", expected, png_ids)
    findings += _set_findings("PNG source fingerprints", expected, fingerprint_ids)
    findings += _set_findings("site SVG projections", expected, svg_ids)

    for identifier, entry in entries.items():
        expected_master = Path(f"docs/diagrams/{identifier}.html")
        if identifier in png_ids:
            findings += _check_png(png_dir / f"{identifier}.png", identifier)
        if entry.master != expected_master:
            findings += (
                _error(
                    f"{identifier}: manifest master must be {expected_master}, got {entry.master}"
                ),
            )
        if identifier in master_ids:
            master_path = root / expected_master
            master_findings = _check_master(master_path, identifier)
            findings += master_findings
            if not master_findings and identifier in fingerprint_ids:
                svg = extract_svg(master_path.read_text(encoding="utf-8"))
                expected_fingerprint = f"sha256:{diagram_fingerprint(svg)}\n"
                fingerprint_path = projection_fingerprint_path(
                    png_dir / f"{identifier}.png"
                )
                try:
                    actual_fingerprint = fingerprint_path.read_text(encoding="utf-8")
                except OSError as error:
                    findings += (
                        _error(f"{identifier}: unable to read PNG source fingerprint: {error}"),
                    )
                else:
                    if actual_fingerprint != expected_fingerprint:
                        findings += (
                            _error(
                                f"{identifier}: PNG source fingerprint differs from "
                                "master/render contract"
                            ),
                        )
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


def _canonical_markdown(root: Path, internal_roots: tuple[Path, ...]) -> tuple[Path, ...]:
    candidates: set[Path] = set()
    readme = root / "README.md"
    if readme.is_file():
        candidates.add(readme)
    docs = root / "docs"
    if docs.exists():
        candidates.update(
            path
            for path in docs.rglob("*.md")
            if not _is_internal(path.relative_to(root), internal_roots)
        )
    for directory in (root / "scenarios", root / "spark-apps"):
        if directory.exists():
            candidates.update(directory.rglob("*.md"))
    return tuple(sorted(candidates))


def _placeholder_files(root: Path, internal_roots: tuple[Path, ...]) -> tuple[Path, ...]:
    candidates = set(_canonical_markdown(root, internal_roots))
    for directory in (root / "generated/site", root / "generated/wiki"):
        if directory.exists():
            candidates.update(directory.rglob("*.md"))
    config = root / "mkdocs.yml"
    if config.is_file():
        candidates.add(config)
    return tuple(sorted(candidates))


def _surface_files(
    root: Path, internal_roots: tuple[Path, ...]
) -> tuple[tuple[str, Path], ...]:
    values: list[tuple[str, Path]] = []
    for path in _canonical_markdown(root, internal_roots):
        values.append(("repo", path))
    site = root / "generated/site"
    if site.exists():
        values.extend(("site", path) for path in sorted(site.rglob("*.md")))
    config = root / "mkdocs.yml"
    if config.is_file():
        values.append(("site", config))
    wiki = root / "generated/wiki"
    if wiki.exists():
        values.extend(("wiki", path) for path in sorted(wiki.rglob("*.md")))
    return tuple(values)


def _origin_findings(
    relative: str,
    text: str,
    surface: str,
) -> tuple[Finding, ...]:
    findings: tuple[Finding, ...] = ()
    label = "repository" if surface == "repo" else surface
    visible_text = _without_fenced_code(text)
    # Calling find_links keeps link parsing centralized; plain origins remain checked
    # to preserve the interim gate's protection against bare URLs and config values.
    targets = tuple(link.target for link in find_links(visible_text))
    searchable = "\n".join((visible_text, *targets))
    if surface != "wiki" and WIKI_ORIGIN in searchable:
        findings += (_error(f"{relative}: {label} surface links to the wiki surface"),)
    without_wiki = searchable.replace(WIKI_ORIGIN, "")
    if surface != "repo" and REPOSITORY_ORIGIN in without_wiki:
        findings += (_error(f"{relative}: {label} surface links to the repository surface"),)
    pages_host = urlsplit(PAGES_ORIGIN).netloc
    # The interim repository gate also rejected a bare Pages URL, not only a
    # Markdown link. Preserve that behavior for canonical repository files.
    if surface != "site" and pages_host in searchable:
        findings += (_error(f"{relative}: {label} surface links to the site surface"),)
    return findings


def _without_fenced_code(markdown: str) -> str:
    lines: list[str] = []
    fence_character = ""
    fence_length = 0
    for line in markdown.splitlines(keepends=True):
        marker = _fence_marker(line)
        if fence_character:
            if (
                marker is not None
                and marker[0] == fence_character
                and marker[1] >= fence_length
                and not marker[2].strip()
            ):
                fence_character = ""
                fence_length = 0
            lines.append("\n" if line.endswith("\n") else "")
            continue
        if marker is not None and not (marker[0] == "`" and "`" in marker[2]):
            fence_character = marker[0]
            fence_length = marker[1]
            lines.append("\n" if line.endswith("\n") else "")
            continue
        lines.append(line)
    return "".join(lines)


def _fence_marker(line: str) -> tuple[str, int, str] | None:
    content = line.rstrip("\r\n")
    indentation = len(content) - len(content.lstrip(" "))
    if indentation > 3:
        return None
    content = content[indentation:]
    if not content or content[0] not in {"`", "~"}:
        return None
    character = content[0]
    length = len(content) - len(content.lstrip(character))
    if length < 3:
        return None
    return character, length, content[length:]


def _resolve_local_target(source: Path, target: str) -> tuple[str, str, Path] | None:
    parsed = urlsplit(target)
    if parsed.scheme or parsed.netloc or target.startswith("//"):
        return None
    clean = unquote(parsed.path)
    fragment = unquote(parsed.fragment)
    if not clean and not fragment:
        return None
    resolved = (source.parent / clean).resolve() if clean else source.resolve()
    return clean, fragment, resolved


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


def _markdown_anchors(markdown: str) -> set[str]:
    """Return GitHub-compatible automatic and explicit anchors for Markdown."""
    visible = _without_fenced_code(markdown)
    anchors = {
        unquote(value)
        for value in re.findall(
            r"<(?:a|h[1-6])\b[^>]*(?:id|name)=[\"']([^\"']+)[\"']",
            visible,
            flags=re.IGNORECASE,
        )
    }
    generated: set[str] = set()
    for match in re.finditer(r"^ {0,3}#{1,6}[ \t]+(.+?)[ \t]*#*[ \t]*$", visible, re.MULTILINE):
        base = _github_slug(match.group(1))
        if not base:
            continue
        candidate = base
        suffix = 1
        while candidate in generated:
            candidate = f"{base}-{suffix}"
            suffix += 1
        generated.add(candidate)
        anchors.add(candidate)
    return anchors


def _github_slug(heading: str) -> str:
    """Approximate GitHub's heading slugger for repository documentation."""
    value = re.sub(r"<[^>]+>", "", heading)
    value = re.sub(r"!?\[([^\]]+)\]\([^)]*\)", r"\1", value)
    value = value.replace("`", "").strip().casefold()
    value = "".join(
        character
        for character in value
        if character in {"-", "_", " "}
        or not unicodedata.category(character).startswith(("P", "S"))
    )
    return re.sub(r"\s", "-", value)


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
    try:
        root = ET.fromstring(svg)
    except ET.ParseError:
        return (_error(f"{identifier}: SVG is not well-formed XML"),)
    if root.attrib.get("role") != "img":
        return (_error(f'{identifier}: SVG must have role="img"'),)
    labelled_by = root.attrib.get("aria-labelledby", "").split()
    children = {
        child.tag.rsplit("}", 1)[-1]: child
        for child in root
        if child.tag.rsplit("}", 1)[-1] in {"title", "desc"}
    }
    title = children.get("title")
    description = children.get("desc")
    if (
        title is None
        or description is None
        or not title.attrib.get("id")
        or not description.attrib.get("id")
        or title.attrib["id"] not in labelled_by
        or description.attrib["id"] not in labelled_by
        or not (title.text or "").strip()
        or not (description.text or "").strip()
    ):
        return (
            _error(
                f"{identifier}: SVG must label non-empty title and desc elements with aria-labelledby"
            ),
        )
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


def _is_internal(relative: Path, internal_roots: tuple[Path, ...]) -> bool:
    return any(relative == root or relative.is_relative_to(root) for root in internal_roots)


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
