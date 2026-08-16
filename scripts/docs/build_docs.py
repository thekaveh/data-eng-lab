"""Build deterministic MkDocs and GitHub Wiki documentation surfaces."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tempfile
from pathlib import Path

from scripts.docs.manifest import (
    Manifest,
    ManifestError,
    Section,
    iter_leaf_sections,
    load_manifest,
)
from scripts.docs.render_diagrams import copy_assets, extract_svg
from scripts.docs.transforms import build_source_map, rewrite_for_surface


class DocumentationDrift(RuntimeError):
    """Raised when two generated documentation trees differ."""


_MKDOCS_TEMPLATE = """\
site_name: data-eng-lab
site_url: https://thekaveh.github.io/data-eng-lab/
site_description: >-
  An Apache Iceberg lakehouse data engineering lab —
  19 paired scenarios, 17 Scala/PySpark parity pairs, 6 CI-built Maven apps,
  Trino BI, Redpanda streaming,
  medallion architecture on Docker Compose.
docs_dir: generated/site
site_dir: site

theme:
  name: material
  custom_dir: generated/site/overrides
  palette:
    - scheme: slate
      primary: cyan
      accent: cyan
      toggle:
        icon: material/weather-night
        name: Switch to light mode
    - scheme: default
      primary: cyan
      accent: cyan
      toggle:
        icon: material/weather-sunny
        name: Switch to dark mode
  font:
    text: Inter
    code: IBM Plex Mono
  features:
    - navigation.tabs
    - navigation.sections
    - navigation.top
    - navigation.tracking
    - navigation.instant
    - toc.follow
    - search.suggest
    - search.highlight
    - content.code.copy
    - content.code.annotate
    - content.tabs.link

markdown_extensions:
  - admonition
  - attr_list
  - md_in_html
  - tables
  - toc:
      permalink: true
  - pymdownx.highlight:
      anchor_linenums: true
  - pymdownx.inlinehilite
  - pymdownx.superfences
  - pymdownx.details
  - pymdownx.tabbed:
      alternate_style: true
  - pymdownx.emoji:
      emoji_index: !!python/name:material.extensions.emoji.twemoji
      emoji_generator: !!python/name:material.extensions.emoji.to_svg
  - pymdownx.critic
  - pymdownx.caret
  - pymdownx.keys
  - pymdownx.mark
  - pymdownx.tilde

plugins:
  - search

extra_css:
  - stylesheets/extra.css

nav:
{nav}
"""


def render_mkdocs_yml(manifest: Manifest) -> str:
    """Render the root MkDocs configuration from the manifest section tree."""
    source_map = build_source_map(manifest, "site")
    return _MKDOCS_TEMPLATE.format(nav="\n".join(_nav_lines(manifest.sections, source_map)))


def render_site(manifest: Manifest, repo_root: Path, output: Path) -> None:
    """Project every public manifest leaf and SVG diagram into the MkDocs input tree."""
    _reset(output)
    source_map = build_source_map(manifest, "site")
    _render_pages(manifest, repo_root, output, "site", source_map)

    image_dir = output / "assets/img"
    image_dir.mkdir(parents=True, exist_ok=True)
    for diagram in manifest.diagrams:
        master = (repo_root / diagram.master).read_text(encoding="utf-8")
        destination = _surface_destination(
            output, Path("assets/img") / f"{diagram.id}.svg", "site"
        )
        destination.write_text(f"{extract_svg(master)}\n", encoding="utf-8")

    _copy_file(
        repo_root / "docs/stylesheets/extra.css",
        _surface_destination(output, Path("stylesheets/extra.css"), "site"),
    )
    _copy_file(
        repo_root / "docs/overrides/main.html",
        _surface_destination(output, Path("overrides/main.html"), "site"),
    )


def render_wiki(manifest: Manifest, repo_root: Path, output: Path) -> None:
    """Project every public manifest leaf and PNG diagram into a self-contained wiki tree."""
    _reset(output)
    source_map = build_source_map(manifest, "wiki")
    _render_pages(manifest, repo_root, output, "wiki", source_map)
    _surface_destination(output, Path("_Sidebar.md"), "wiki").write_text(
        _render_sidebar(manifest), encoding="utf-8"
    )
    _surface_destination(output, Path("_Footer.md"), "wiki").write_text(
        "data-eng-lab documentation · Generated from the canonical documentation manifest.\n",
        encoding="utf-8",
    )
    copy_assets(
        repo_root / "docs/diagrams/img",
        _surface_destination(output, Path("img"), "wiki"),
    )


def hash_tree(root: Path) -> dict[Path, str]:
    """Return the SHA-256 digest of every file, keyed by sorted relative path."""
    files = sorted(
        (path for path in root.rglob("*") if path.is_file()),
        key=lambda path: path.relative_to(root).as_posix(),
    )
    return {
        path.relative_to(root): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in files
    }


def assert_dirs_equal(actual: Path, expected: Path) -> None:
    """Raise when generated trees differ by path or file content."""
    actual_hashes = hash_tree(actual)
    expected_hashes = hash_tree(expected)
    if actual_hashes == expected_hashes:
        return

    actual_paths = set(actual_hashes)
    expected_paths = set(expected_hashes)
    missing = sorted(expected_paths - actual_paths)
    unexpected = sorted(actual_paths - expected_paths)
    changed = sorted(
        path for path in actual_paths & expected_paths if actual_hashes[path] != expected_hashes[path]
    )
    details = []
    details.extend(f"missing {path.as_posix()}" for path in missing)
    details.extend(f"unexpected {path.as_posix()}" for path in unexpected)
    details.extend(f"changed {path.as_posix()}" for path in changed)
    raise DocumentationDrift("documentation trees differ: " + ", ".join(details))


def build(repo_root: Path, *, site: bool, wiki: bool, check: bool) -> None:
    """Build requested surfaces and optionally prove a byte-identical second render."""
    repo_root = repo_root.resolve()
    manifest = load_manifest(repo_root / "docs/manifest.yaml", repo_root)
    site_output = repo_root / "generated/site"
    wiki_output = repo_root / "generated/wiki"

    if site:
        render_site(manifest, repo_root, site_output)
        (repo_root / "mkdocs.yml").write_text(render_mkdocs_yml(manifest), encoding="utf-8")
    if wiki:
        render_wiki(manifest, repo_root, wiki_output)

    if check:
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory)
            if site:
                expected_site = temporary / "site"
                render_site(manifest, repo_root, expected_site)
                assert_dirs_equal(site_output, expected_site)
            if wiki:
                expected_wiki = temporary / "wiki"
                render_wiki(manifest, repo_root, expected_wiki)
                assert_dirs_equal(wiki_output, expected_wiki)


def _nav_lines(
    sections: tuple[Section, ...],
    source_map: dict[Path, Path],
    indent: int = 3,
) -> list[str]:
    lines: list[str] = []
    prefix = " " * indent
    for section in sections:
        label = json.dumps(f"{section.number}. {section.title}", ensure_ascii=False)
        if section.source is not None:
            lines.append(f"{prefix}- {label}: {source_map[section.source].as_posix()}")
        else:
            lines.append(f"{prefix}- {label}:")
            lines.extend(_nav_lines(section.children, source_map, indent + 2))
    return lines


def _render_pages(
    manifest: Manifest,
    repo_root: Path,
    output: Path,
    surface: str,
    source_map: dict[Path, Path],
) -> None:
    for section in iter_leaf_sections(manifest.sections):
        assert section.source is not None
        source = repo_root / section.source
        destination = _surface_destination(
            output, source_map[section.source], surface
        )
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            rewrite_for_surface(source.read_text(encoding="utf-8"), surface, section.source, source_map),
            encoding="utf-8",
        )


def _surface_destination(output: Path, relative: Path, surface: str) -> Path:
    output_root = output.resolve()
    destination = (output_root / relative).resolve()
    if relative.is_absolute() or not destination.is_relative_to(output_root):
        raise ManifestError(
            f"{surface} destination escapes its surface root: {relative}"
        )
    return destination


def _render_sidebar(manifest: Manifest) -> str:
    source_map = build_source_map(manifest, "wiki")
    lines: list[str] = []

    def append(sections: tuple[Section, ...], depth: int) -> None:
        for section in sections:
            label = f"{section.number}. {section.title}"
            prefix = "  " * depth
            if section.source is not None:
                lines.append(f"{prefix}- [{label}]({source_map[section.source].as_posix()})\n")
            else:
                lines.append(f"{prefix}- **{label}**\n")
                append(section.children, depth + 1)

    append(manifest.sections, 0)
    return "".join(lines)


def _reset(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)


def _copy_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--site", action="store_true")
    parser.add_argument("--wiki", action="store_true")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    if not args.site and not args.wiki:
        parser.error("at least one of --site or --wiki is required")
    build(args.root, site=args.site, wiki=args.wiki, check=args.check)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
