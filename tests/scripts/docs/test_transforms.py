from pathlib import Path

import pytest

from scripts.docs.manifest import parse_manifest
from scripts.docs.transforms import build_source_map, rewrite_for_surface


@pytest.fixture
def manifest():
    return parse_manifest(
        """\
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
  - {id: example, number: '6', title: Example, source: docs/notebooks/example.md}
diagrams:
  - {id: overview, master: docs/diagrams/overview.html}
"""
    )


def test_build_source_map_uses_home_for_wiki(manifest):
    mapping = build_source_map(manifest, "wiki")
    assert type(mapping) is dict
    assert mapping[Path("docs/index.md")] == Path("Home.md")
    assert mapping[Path("docs/scenarios/index.md")] == Path("Scenarios.md")


@pytest.mark.parametrize(
    "target",
    [
        "../architectures/overview.svg",
        "../diagrams/img/overview.png",
    ],
)
def test_plain_dict_source_map_rewrites_legacy_and_canonical_diagrams(manifest, target):
    source = Path("docs/notebooks/example.md")
    mapping = dict(build_source_map(manifest, "site"))
    assert mapping[Path("docs/architectures/overview.svg")] == Path("assets/img/overview.svg")
    assert mapping[Path("docs/diagrams/img/overview.png")] == Path("assets/img/overview.svg")
    assert rewrite_for_surface(
        f"![Flow]({target})",
        "site",
        source,
        mapping,
    ) == "![Flow](../assets/img/overview.svg)"


def test_rewrite_for_site_preserves_subdirectory_image_prefix(manifest):
    mapping = build_source_map(manifest, "site")
    result = rewrite_for_surface(
        "[Catalog](../scenarios/index.md) ![Flow](../architectures/overview.svg)",
        "site",
        Path("docs/notebooks/example.md"),
        mapping,
    )
    assert "[Catalog](../scenarios/index.md)" in result
    assert "![Flow](../assets/img/overview.svg)" in result


def test_rewrite_drops_forbidden_and_non_manifest_targets(manifest):
    mapping = build_source_map(manifest, "wiki")
    result = rewrite_for_surface(
        "[Source](https://github.com/thekaveh/data-eng-lab/blob/main/docs/index.md) "
        "[Draft](superpowers/specs/internal.md) [Notebook](../../scenarios/x/jupyter/notebook.ipynb)",
        "wiki",
        Path("docs/index.md"),
        mapping,
    )
    assert result == "Source Draft Notebook"


def test_rewrite_changes_only_the_target_when_it_matches_the_label(manifest):
    mapping = build_source_map(manifest, "wiki")
    result = rewrite_for_surface(
        "[../scenarios/index.md](../scenarios/index.md)",
        "wiki",
        Path("docs/notebooks/example.md"),
        mapping,
    )
    assert result == "[../scenarios/index.md](Scenarios.md)"
