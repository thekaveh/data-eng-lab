"""Canonical documentation manifest parsing and repository-boundary validation."""

from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

_PATH_SAFE_SLUG = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_-]*$")
_AUXILIARY_PUBLIC_INPUTS = (
    Path("docs/stylesheets/extra.css"),
    Path("docs/overrides/main.html"),
    Path("docs/diagrams/img"),
)


class ManifestError(ValueError):
    """Raised when a documentation manifest violates its contract."""


@dataclass(frozen=True)
class Section:
    id: str
    number: str
    title: str
    source: Path | None = None
    children: tuple["Section", ...] = ()
    diagrams: tuple[str, ...] = ()


@dataclass(frozen=True)
class DiagramEntry:
    id: str
    master: Path


@dataclass(frozen=True)
class Manifest:
    surfaces: tuple[str, ...]
    numbering: str
    internal_roots: tuple[Path, ...]
    sections: tuple[Section, ...]
    diagrams: tuple[DiagramEntry, ...]


def parse_manifest(text: str) -> Manifest:
    """Parse manifest YAML into immutable values without accessing the filesystem."""
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as error:
        raise ManifestError(f"invalid manifest YAML: {error}") from error

    root = _mapping(data, "manifest")
    if "surfaces" not in root:
        raise ManifestError("manifest missing keys: surfaces")
    surfaces = _string_tuple(root["surfaces"], "surfaces")
    if surfaces != ("repo", "site", "wiki"):
        raise ManifestError("surfaces must be repo, site, wiki")

    _keys(root, {"surfaces", "numbering", "internal_roots", "sections", "diagrams"}, set(), "manifest")

    numbering = _string(root["numbering"], "numbering")
    if numbering != "baked":
        raise ManifestError("numbering must be baked")

    internal_roots = tuple(Path(value) for value in _string_tuple(root["internal_roots"], "internal_roots"))
    section_ids: set[str] = set()
    section_numbers: set[str] = set()
    section_values = _list(root["sections"], "sections")
    sections = tuple(_parse_section(value, section_ids, section_numbers) for value in section_values)

    diagram_ids: set[str] = set()
    diagrams = tuple(_parse_diagram(value, diagram_ids) for value in _list(root["diagrams"], "diagrams"))
    return Manifest(surfaces, numbering, internal_roots, sections, diagrams)


def load_manifest(path: Path, repo_root: Path) -> Manifest:
    """Load a manifest and ensure every referenced source and master is in the repository."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as error:
        raise ManifestError(f"unable to read manifest: {path}") from error

    manifest = parse_manifest(text)
    root = repo_root.resolve()
    sources = [leaf.source for leaf in iter_leaf_sections(manifest.sections)]
    masters = [entry.master for entry in manifest.diagrams]
    for manifest_path in (*sources, *masters):
        assert manifest_path is not None
        resolved = (root / manifest_path).resolve()
        if not resolved.is_relative_to(root):
            raise ManifestError(f"manifest path outside repository: {manifest_path}")
        if not resolved.exists():
            raise ManifestError(f"missing manifest path: {manifest_path}")
    docs_root = (root / "docs").resolve()
    resolved_internal_roots: list[tuple[Path, Path]] = []
    for internal_root in manifest.internal_roots:
        resolved = (root / internal_root).resolve()
        if not resolved.is_relative_to(root):
            raise ManifestError(f"manifest path outside repository: {internal_root}")
        canonical = resolved.relative_to(root)
        if internal_root != canonical:
            raise ManifestError(
                f"internal root must be canonical repo-relative path: "
                f"{internal_root} (use {canonical})"
            )
        if resolved == docs_root or not resolved.is_relative_to(docs_root):
            raise ManifestError(
                f"internal root must be a proper docs subtree: {internal_root}"
            )
        if not resolved.exists():
            raise ManifestError(f"missing manifest path: {internal_root}")
        if not resolved.is_dir():
            raise ManifestError(f"internal root must be a directory: {internal_root}")
        resolved_internal_roots.append((internal_root, resolved))

    for source in sources:
        assert source is not None
        resolved = (root / source).resolve()
        for internal_root, internal_resolved in resolved_internal_roots:
            if resolved == internal_resolved or resolved.is_relative_to(internal_resolved):
                raise ManifestError(
                    f"published source is inside internal root: {source} ({internal_root})"
                )
    for master in masters:
        resolved = (root / master).resolve()
        for internal_root, internal_resolved in resolved_internal_roots:
            if resolved == internal_resolved or resolved.is_relative_to(internal_resolved):
                raise ManifestError(
                    f"diagram master is inside internal root: {master} ({internal_root})"
                )
    for internal_root, internal_resolved in resolved_internal_roots:
        for auxiliary in _AUXILIARY_PUBLIC_INPUTS:
            auxiliary_resolved = (root / auxiliary).resolve()
            if (
                auxiliary_resolved == internal_resolved
                or auxiliary_resolved.is_relative_to(internal_resolved)
                or internal_resolved.is_relative_to(auxiliary_resolved)
            ):
                raise ManifestError(
                    "internal root overlaps published auxiliary input: "
                    f"{internal_root} ({auxiliary})"
                )
    return manifest


def iter_leaf_sections(sections: tuple[Section, ...]) -> Iterator[Section]:
    """Yield source-bearing sections in manifest order."""
    for section in sections:
        if section.source is not None:
            yield section
        else:
            yield from iter_leaf_sections(section.children)


def _parse_section(value: Any, ids: set[str], numbers: set[str]) -> Section:
    section = _mapping(value, "section")
    _keys(section, {"id", "number", "title"}, {"source", "children", "diagrams"}, "section")
    identifier = _string(section["id"], "section id")
    _path_safe_slug(identifier, "section id")
    number = _string(section["number"], "section number")
    title = _string(section["title"], "section title")
    if identifier in ids:
        raise ManifestError(f"duplicate section id: {identifier}")
    if number in numbers:
        raise ManifestError(f"duplicate section number: {number}")
    ids.add(identifier)
    numbers.add(number)

    has_source = "source" in section
    has_children = "children" in section
    if has_source and has_children:
        raise ManifestError("section cannot define both source and children")
    if not has_source and not has_children:
        raise ManifestError("section must define exactly one of source or children")
    if has_source and section["source"] is None:
        raise ManifestError("section source must be a string")

    diagrams = _string_tuple(section.get("diagrams", []), "section diagrams")
    if has_source:
        source = Path(_string(section["source"], "section source"))
        return Section(identifier, number, title, source=source, diagrams=diagrams)
    return Section(
        identifier,
        number,
        title,
        children=tuple(_parse_section(child, ids, numbers) for child in _list(section["children"], "section children")),
        diagrams=diagrams,
    )


def _parse_diagram(value: Any, ids: set[str]) -> DiagramEntry:
    diagram = _mapping(value, "diagram")
    _keys(diagram, {"id", "master"}, set(), "diagram")
    identifier = _string(diagram["id"], "diagram id")
    _path_safe_slug(identifier, "diagram id")
    if identifier in ids:
        raise ManifestError(f"duplicate diagram id: {identifier}")
    ids.add(identifier)
    return DiagramEntry(identifier, Path(_string(diagram["master"], "diagram master")))


def _mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ManifestError(f"{name} must be a mapping")
    if not all(isinstance(key, str) for key in value):
        raise ManifestError(f"{name} keys must be strings")
    return value


def _keys(value: dict[str, Any], required: set[str], optional: set[str], name: str) -> None:
    missing = required - set(value)
    extra = set(value) - required - optional
    if missing:
        raise ManifestError(f"{name} missing keys: {', '.join(sorted(missing))}")
    if extra:
        raise ManifestError(f"{name} has unknown keys: {', '.join(sorted(extra))}")


def _list(value: Any, name: str) -> list[Any]:
    if not isinstance(value, list):
        raise ManifestError(f"{name} must be a list")
    return value


def _string_tuple(value: Any, name: str) -> tuple[str, ...]:
    return tuple(_string(item, name) for item in _list(value, name))


def _string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ManifestError(f"{name} must be a non-empty string")
    return value


def _path_safe_slug(value: str, name: str) -> None:
    if not _PATH_SAFE_SLUG.fullmatch(value):
        raise ManifestError(f"{name} must be a path-safe slug: {value}")
