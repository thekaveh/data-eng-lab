from pathlib import Path

import pytest

from scripts.docs.manifest import (
    ManifestError,
    iter_leaf_sections,
    load_manifest,
    parse_manifest,
)

VALID = """\
surfaces: [repo, site, wiki]
numbering: baked
internal_roots: [docs/superpowers]
sections:
  - {id: overview, number: '1', title: Overview, source: docs/index.md}
  - id: scenarios
    number: '5'
    title: Scenarios
    children:
      - {id: catalog, number: '5.1', title: Catalog, source: docs/scenarios/index.md}
diagrams:
  - {id: overview, master: docs/diagrams/overview.html}
"""


def test_parse_manifest_exposes_leaf_order_and_internal_boundary():
    manifest = parse_manifest(VALID)
    assert manifest.surfaces == ("repo", "site", "wiki")
    assert manifest.internal_roots == (Path("docs/superpowers"),)
    assert [(leaf.number, leaf.source) for leaf in iter_leaf_sections(manifest.sections)] == [
        ("1", Path("docs/index.md")),
        ("5.1", Path("docs/scenarios/index.md")),
    ]


@pytest.mark.parametrize(
    "text,message",
    [
        ("surfaces: [repo]\nnumbering: baked\nsections: []\ndiagrams: []\n", "surfaces must be repo, site, wiki"),
        (VALID.replace("numbering: baked", "numbering: generated"), "numbering must be baked"),
        (
            VALID.replace(
                "- {id: catalog, number: '5.1', title: Catalog, source: docs/scenarios/index.md}",
                "- id: catalog\n        number: '5.1'\n        title: Catalog\n"
                "        source: docs/scenarios/index.md\n        children: []",
            ),
            "cannot define both source and children",
        ),
        (VALID.replace("number: '5.1'", "number: '1'"), "duplicate section number: 1"),
    ],
)
def test_parse_manifest_rejects_invalid_contract(text, message):
    with pytest.raises(ManifestError, match=message):
        parse_manifest(text)


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        ("id: overview", "id: ../overview", "section id must be a path-safe slug"),
        (
            "id: overview, master: docs/diagrams/overview.html",
            "id: diagrams/overview, master: docs/diagrams/overview.html",
            "diagram id must be a path-safe slug",
        ),
    ],
)
def test_parse_manifest_requires_path_safe_slug_ids(old, new, message):
    with pytest.raises(ManifestError, match=message):
        parse_manifest(VALID.replace(old, new))


def test_load_manifest_rejects_missing_source_and_master(tmp_path):
    path = tmp_path / "docs" / "manifest.yaml"
    path.parent.mkdir()
    path.write_text(VALID, encoding="utf-8")
    with pytest.raises(ManifestError, match="missing manifest path: docs/index.md"):
        load_manifest(path, tmp_path)


@pytest.mark.parametrize(
    ("internal_root", "message"),
    [
        ("../private", "manifest path outside repository: ../private"),
        ("docs/private", "missing manifest path: docs/private"),
    ],
)
def test_load_manifest_validates_internal_root_paths(tmp_path, internal_root, message):
    (tmp_path / "docs/diagrams").mkdir(parents=True)
    (tmp_path / "docs/index.md").write_text("# 1. Overview\n", encoding="utf-8")
    (tmp_path / "docs/scenarios").mkdir()
    (tmp_path / "docs/scenarios/index.md").write_text("# 5.1. Catalog\n", encoding="utf-8")
    (tmp_path / "docs/diagrams/overview.html").write_text("<svg/>", encoding="utf-8")
    path = tmp_path / "docs/manifest.yaml"
    path.write_text(VALID.replace("docs/superpowers", internal_root), encoding="utf-8")

    with pytest.raises(ManifestError, match=message):
        load_manifest(path, tmp_path)


def test_load_manifest_requires_internal_roots_to_be_directories(tmp_path):
    (tmp_path / "docs/diagrams").mkdir(parents=True)
    (tmp_path / "docs/index.md").write_text("# 1. Overview\n", encoding="utf-8")
    (tmp_path / "docs/scenarios").mkdir()
    (tmp_path / "docs/scenarios/index.md").write_text("# 5.1. Catalog\n", encoding="utf-8")
    (tmp_path / "docs/diagrams/overview.html").write_text("<svg/>", encoding="utf-8")
    (tmp_path / "docs/internal.md").write_text("# Internal\n", encoding="utf-8")
    path = tmp_path / "docs/manifest.yaml"
    path.write_text(VALID.replace("docs/superpowers", "docs/internal.md"), encoding="utf-8")

    with pytest.raises(ManifestError, match="internal root must be a directory: docs/internal.md"):
        load_manifest(path, tmp_path)


@pytest.mark.parametrize(
    ("internal_root", "message"),
    [
        (".", "internal root must be a proper docs subtree"),
        ("docs", "internal root must be a proper docs subtree"),
        ("scenarios", "internal root must be a proper docs subtree"),
        ("docs/private/..", "internal root must be canonical"),
    ],
)
def test_load_manifest_requires_internal_roots_to_be_proper_docs_subtrees(
    tmp_path, internal_root, message
):
    _write_valid_manifest_tree(tmp_path)
    (tmp_path / "docs/private").mkdir()
    path = tmp_path / "docs/manifest.yaml"
    path.write_text(VALID.replace("docs/superpowers", internal_root), encoding="utf-8")

    with pytest.raises(ManifestError, match=message):
        load_manifest(path, tmp_path)


@pytest.mark.parametrize(
    ("internal_root", "message"),
    [
        ("docs/scenarios", "published source is inside internal root"),
        ("docs/diagrams", "diagram master is inside internal root"),
    ],
)
def test_load_manifest_rejects_internal_roots_that_overlap_public_inputs(
    tmp_path, internal_root, message
):
    _write_valid_manifest_tree(tmp_path)
    path = tmp_path / "docs/manifest.yaml"
    path.write_text(VALID.replace("docs/superpowers", internal_root), encoding="utf-8")

    with pytest.raises(ManifestError, match=message):
        load_manifest(path, tmp_path)


def test_load_manifest_rejects_noncanonical_internal_root_alias(tmp_path):
    _write_valid_manifest_tree(tmp_path)
    (tmp_path / "docs/private").mkdir()
    path = tmp_path / "docs/manifest.yaml"
    path.write_text(
        VALID.replace("docs/superpowers", "docs/private/../superpowers"),
        encoding="utf-8",
    )

    with pytest.raises(ManifestError, match="internal root must be canonical"):
        load_manifest(path, tmp_path)


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        (
            "source: docs/index.md",
            "source: docs/scenarios/../index.md",
            "section source must be canonical repo-relative path",
        ),
        (
            "master: docs/diagrams/overview.html",
            "master: docs/diagrams/nested/../overview.html",
            "diagram master must be canonical repo-relative path",
        ),
    ],
)
def test_load_manifest_rejects_source_and_master_path_aliases(tmp_path, old, new, message):
    _write_valid_manifest_tree(tmp_path)
    (tmp_path / "docs/diagrams/nested").mkdir()
    path = tmp_path / "docs/manifest.yaml"
    path.write_text(VALID.replace(old, new), encoding="utf-8")

    with pytest.raises(ManifestError, match=message):
        load_manifest(path, tmp_path)


def test_load_manifest_rejects_duplicate_published_sources(tmp_path):
    _write_valid_manifest_tree(tmp_path)
    path = tmp_path / "docs/manifest.yaml"
    path.write_text(
        VALID.replace(
            "diagrams:",
            "  - {id: duplicate, number: '5.2', title: Duplicate, "
            "source: docs/index.md}\n"
            "diagrams:",
        ),
        encoding="utf-8",
    )

    with pytest.raises(ManifestError, match="duplicate published source: docs/index.md"):
        load_manifest(path, tmp_path)


@pytest.mark.parametrize(
    "internal_root",
    ["docs/stylesheets", "docs/overrides", "docs/diagrams/img"],
)
def test_load_manifest_rejects_internal_roots_that_overlap_auxiliary_public_inputs(
    tmp_path, internal_root
):
    _write_valid_manifest_tree(tmp_path)
    path = tmp_path / "docs/manifest.yaml"
    path.write_text(VALID.replace("docs/superpowers", internal_root), encoding="utf-8")

    with pytest.raises(ManifestError, match="internal root overlaps published auxiliary input"):
        load_manifest(path, tmp_path)


def test_load_manifest_rejects_explicit_public_source_inside_internal_root(tmp_path):
    _write_valid_manifest_tree(tmp_path)
    internal_source = tmp_path / "docs/superpowers/private.md"
    internal_source.write_text("# Private\n", encoding="utf-8")
    path = tmp_path / "docs/manifest.yaml"
    path.write_text(
        VALID.replace("source: docs/index.md", "source: docs/superpowers/private.md"),
        encoding="utf-8",
    )

    with pytest.raises(ManifestError, match="published source is inside internal root"):
        load_manifest(path, tmp_path)


def _write_valid_manifest_tree(root: Path) -> None:
    (root / "docs/diagrams/img").mkdir(parents=True)
    (root / "docs/superpowers").mkdir()
    (root / "docs/stylesheets").mkdir()
    (root / "docs/overrides").mkdir()
    (root / "docs/index.md").write_text("# 1. Overview\n", encoding="utf-8")
    (root / "docs/scenarios").mkdir()
    (root / "docs/scenarios/index.md").write_text("# 5.1. Catalog\n", encoding="utf-8")
    (root / "docs/diagrams/overview.html").write_text("<svg/>\n", encoding="utf-8")
    (root / "docs/diagrams/img/overview.png").write_bytes(b"png")
    (root / "docs/stylesheets/extra.css").write_text("body {}\n", encoding="utf-8")
    (root / "docs/overrides/main.html").write_text("{% extends 'base.html' %}\n", encoding="utf-8")
