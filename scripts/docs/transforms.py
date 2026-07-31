"""Deterministic canonical-document transforms for generated surfaces."""

from __future__ import annotations

import posixpath
import re
from collections.abc import Iterable, Iterator, Mapping
from pathlib import Path
from urllib.parse import urlsplit

from scripts.docs.links import MARKDOWN_LINK_RE, is_forbidden
from scripts.docs.manifest import Manifest, iter_leaf_sections


class SourceMap(Mapping[Path, Path]):
    """Immutable source-path mapping with the manifest diagram IDs."""

    def __init__(self, paths: Mapping[Path, Path], *, diagram_ids: Iterable[str]) -> None:
        self._paths = dict(paths)
        self.diagram_ids = frozenset(diagram_ids)

    def __getitem__(self, source: Path) -> Path:
        return self._paths[source]

    def __iter__(self) -> Iterator[Path]:
        return iter(self._paths)

    def __len__(self) -> int:
        return len(self._paths)


def build_source_map(manifest: Manifest, surface: str) -> SourceMap:
    """Map each manifest source page to its destination on *surface*."""
    mapping: dict[Path, Path] = {}
    for section in iter_leaf_sections(manifest.sections):
        assert section.source is not None
        mapping[section.source] = _destination(section.source, section.id, surface)
    return SourceMap(mapping, diagram_ids=(entry.id for entry in manifest.diagrams))


def rewrite_for_surface(
    markdown: str,
    surface: str,
    source: Path,
    source_map: SourceMap,
) -> str:
    """Rewrite canonical Markdown links to local targets for *surface*."""
    if not isinstance(source_map, SourceMap):
        raise TypeError("source_map must be a SourceMap with diagram metadata")
    source_destination = source_map.get(source, source)

    def replace(match: re.Match[str]) -> str:
        label = match.group("label")
        target = match.group("target")
        replacement = _rewrite_target(
            target,
            surface,
            source,
            source_destination,
            source_map,
            source_map.diagram_ids,
        )
        if replacement is None:
            return label
        start = match.start("target") - match.start()
        end = match.end("target") - match.start()
        return match.group(0)[:start] + replacement + match.group(0)[end:]

    return MARKDOWN_LINK_RE.sub(replace, markdown)


def _destination(source: Path, identifier: str, surface: str) -> Path:
    if surface == "repo":
        return source
    if surface == "site":
        return Path(*source.parts[1:]) if source.parts[0] == "docs" else source
    if source == Path("docs/index.md"):
        return Path("Home.md")
    if source.name == "index.md":
        return Path(f"{_title(source.parent.name)}.md")
    return Path(f"{_title(identifier)}.md")


def _rewrite_target(
    target: str,
    surface: str,
    source: Path,
    source_destination: Path,
    source_map: SourceMap,
    diagram_ids: frozenset[str],
) -> str | None:
    parsed = urlsplit(target)
    if parsed.scheme or parsed.netloc:
        return None if is_forbidden(target, surface) else target
    if target.startswith("#"):
        return target

    path_text, suffix = _path_and_suffix(target)
    resolved = _resolve(source.parent, path_text)
    if resolved.suffix == ".ipynb":
        return None
    if resolved in source_map:
        return _relative_target(source_destination, source_map[resolved]) + suffix
    if resolved.stem in diagram_ids and resolved.suffix in {".html", ".png", ".svg"}:
        asset = _diagram_asset(resolved.stem, surface)
        return _relative_target(source_destination, asset) + suffix if asset else target
    if resolved.suffix == ".md":
        return None
    return target


def _path_and_suffix(target: str) -> tuple[str, str]:
    marker_positions = [position for position in (target.find("?"), target.find("#")) if position >= 0]
    if not marker_positions:
        return target, ""
    marker = min(marker_positions)
    return target[:marker], target[marker:]


def _resolve(parent: Path, target: str) -> Path:
    return Path(posixpath.normpath((parent / target).as_posix()))


def _relative_target(source_destination: Path, target_destination: Path) -> str:
    return posixpath.relpath(target_destination.as_posix(), source_destination.parent.as_posix())


def _diagram_asset(identifier: str, surface: str) -> Path | None:
    if surface == "site":
        return Path("assets/img") / f"{identifier}.svg"
    if surface == "wiki":
        return Path("img") / f"{identifier}.png"
    return None


def _title(value: str) -> str:
    return value.replace("_", "-").title()
