"""Deterministic canonical-document transforms for generated surfaces."""

from __future__ import annotations

import posixpath
import re
from collections.abc import Mapping
from pathlib import Path
from urllib.parse import urlsplit

from scripts.docs.links import MARKDOWN_LINK_RE, find_html_image_sources, is_forbidden
from scripts.docs.manifest import (
    Manifest,
    ManifestError,
    Section,
    iter_leaf_sections,
)

_WIKI_HOME = Path("Home.md")
_WIKI_HOME_OWNER = ("overview", Path("docs/index.md"))
_WIKI_STRUCTURAL_DESTINATIONS = {
    "_sidebar": Path("_Sidebar.md"),
    "_footer": Path("_Footer.md"),
}


def build_source_map(manifest: Manifest, surface: str) -> dict[Path, Path]:
    """Map each manifest source page to its destination on *surface*."""
    mapping: dict[Path, Path] = {}
    destination_owners: dict[str, str] = {}
    for section in iter_leaf_sections(manifest.sections):
        assert section.source is not None
        destination = _destination(section.source, section.id, surface)
        _validate_surface_destination(destination, surface)
        if surface == "wiki":
            _validate_wiki_destination(section, destination)
        _register_destination(
            surface,
            destination,
            f"{section.id} ({section.source})",
            destination_owners,
        )
        mapping[section.source] = destination
    if surface == "site":
        asset_dir = Path("assets/img")
        extension = ".svg"
    elif surface == "wiki":
        asset_dir = Path("img")
        extension = ".png"
    else:
        return mapping
    for diagram in manifest.diagrams:
        destination = asset_dir / f"{diagram.id}{extension}"
        _validate_surface_destination(destination, surface)
        _register_destination(
            surface,
            destination,
            f"diagram {diagram.id} ({diagram.master})",
            destination_owners,
        )
        mapping[Path("docs/architectures") / f"{diagram.id}.svg"] = destination
        mapping[Path("docs/diagrams/img") / f"{diagram.id}.png"] = destination
    return mapping


def _validate_surface_destination(destination: Path, surface: str) -> None:
    if surface not in {"site", "wiki"}:
        return
    surface_root = Path("/__docs_surface__")
    resolved = (surface_root / destination).resolve()
    if destination.is_absolute() or not resolved.is_relative_to(surface_root):
        raise ManifestError(f"{surface} destination escapes its surface root: {destination}")
    canonical = resolved.relative_to(surface_root)
    if destination != canonical:
        raise ManifestError(f"{surface} destination must be canonical: {destination} (use {canonical})")


def _register_destination(
    surface: str,
    destination: Path,
    owner: str,
    owners: dict[str, str],
) -> None:
    key = destination.as_posix().casefold()
    existing = owners.get(key)
    if existing is not None and existing != owner:
        raise ManifestError(f"{surface} destination collision at {destination}: {existing} and {owner}")
    owners[key] = owner


def _validate_wiki_destination(
    section: Section,
    destination: Path,
) -> None:
    assert section.source is not None
    structural_destination = _WIKI_STRUCTURAL_DESTINATIONS.get(section.id.casefold())
    if structural_destination is not None:
        raise ManifestError(
            f"wiki destination {structural_destination} is reserved for a structural page; "
            f"{section.id} ({section.source}) cannot use its identifier"
        )
    if destination in _WIKI_STRUCTURAL_DESTINATIONS.values():
        raise ManifestError(
            f"wiki destination {destination} is reserved for a structural page; "
            f"{section.id} ({section.source}) cannot map to it"
        )
    if destination == _WIKI_HOME and (section.id, section.source) != _WIKI_HOME_OWNER:
        owner_id, owner_source = _WIKI_HOME_OWNER
        raise ManifestError(
            f"wiki destination {_WIKI_HOME} is reserved for {owner_id} ({owner_source}); "
            f"{section.id} ({section.source}) cannot use it"
        )


def rewrite_for_surface(
    markdown: str,
    surface: str,
    source: Path,
    source_map: Mapping[Path, Path],
) -> str:
    """Rewrite canonical Markdown links to local targets for *surface*."""
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
        )
        if replacement is None:
            return label
        start = match.start("target") - match.start()
        end = match.end("target") - match.start()
        return match.group(0)[:start] + replacement + match.group(0)[end:]

    rewritten = MARKDOWN_LINK_RE.sub(replace, markdown)

    for image in reversed(find_html_image_sources(rewritten)):
        replacement = _rewrite_target(
            image.target,
            surface,
            source,
            source_destination,
            source_map,
        )
        if replacement is not None:
            rewritten = rewritten[: image.start] + replacement + rewritten[image.end :]

    return rewritten


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
    source_map: Mapping[Path, Path],
) -> str | None:
    parsed = urlsplit(target)
    if parsed.scheme or parsed.netloc:
        return None if is_forbidden(target, surface) else target
    if target.startswith("#"):
        return target

    path_text, suffix = _path_and_suffix(target)
    resolved = _resolve(source.parent, path_text)
    if resolved in source_map:
        return _relative_target(source_destination, source_map[resolved]) + suffix
    return None


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


def _title(value: str) -> str:
    return value.replace("_", "-").title()
