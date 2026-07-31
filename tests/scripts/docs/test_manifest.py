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


def test_load_manifest_rejects_missing_source_and_master(tmp_path):
    path = tmp_path / "docs" / "manifest.yaml"
    path.parent.mkdir()
    path.write_text(VALID, encoding="utf-8")
    with pytest.raises(ManifestError, match="missing manifest path: docs/index.md"):
        load_manifest(path, tmp_path)
