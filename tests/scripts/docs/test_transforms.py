from pathlib import Path

import pytest

from scripts.docs.manifest import ManifestError, parse_manifest
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
  - {id: data-eng-lab-hero, master: docs/diagrams/data-eng-lab-hero.html}
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
    assert (
        rewrite_for_surface(
            f"![Flow]({target})",
            "site",
            source,
            mapping,
        )
        == "![Flow](../assets/img/overview.svg)"
    )


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


@pytest.mark.parametrize(
    ("surface", "expected"),
    [
        ("site", '<img src="../assets/img/data-eng-lab-hero.svg" alt="Lakehouse hero">'),
        ("wiki", '<img src="img/data-eng-lab-hero.png" alt="Lakehouse hero">'),
    ],
)
def test_rewrite_maps_local_html_image_sources(manifest, surface, expected):
    source = Path("docs/notebooks/example.md")
    mapping = build_source_map(manifest, surface)
    markdown = '<img src="../diagrams/img/data-eng-lab-hero.png" alt="Lakehouse hero">'

    assert rewrite_for_surface(markdown, surface, source, mapping) == expected


def test_rewrite_maps_html_image_with_whitespace_around_src_equals(manifest):
    source = Path("docs/notebooks/example.md")
    mapping = build_source_map(manifest, "site")
    markdown = '<img alt="Lakehouse hero" src  =  "../diagrams/img/data-eng-lab-hero.png">'

    assert rewrite_for_surface(markdown, "site", source, mapping) == (
        '<img alt="Lakehouse hero" src  =  "../assets/img/data-eng-lab-hero.svg">'
    )


def test_rewrite_changes_only_the_real_html_src_attribute(manifest):
    source = Path("docs/notebooks/example.md")
    mapping = build_source_map(manifest, "site")
    markdown = (
        '<img data-src="../diagrams/img/data-eng-lab-hero.png" '
        'srcset="small.png 1x" '
        "alt=\"literal src='../diagrams/img/data-eng-lab-hero.png'\" "
        'src = "../diagrams/img/data-eng-lab-hero.png">'
    )

    assert rewrite_for_surface(markdown, "site", source, mapping) == (
        '<img data-src="../diagrams/img/data-eng-lab-hero.png" '
        'srcset="small.png 1x" '
        "alt=\"literal src='../diagrams/img/data-eng-lab-hero.png'\" "
        'src = "../assets/img/data-eng-lab-hero.svg">'
    )


def test_rewrite_preserves_remote_badge_image_sources(manifest):
    source = Path("docs/index.md")
    mapping = build_source_map(manifest, "site")
    badge = (
        '<img data-src="lazy.svg" alt="Apache Spark" '
        'src = "https://img.shields.io/badge/Apache%20Spark-compute-E25A1C">'
    )

    assert rewrite_for_surface(badge, "site", source, mapping) == badge


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


@pytest.mark.parametrize(
    "target",
    [
        "../datasets/registry.yaml",
        "../assets/unpublished.png",
        "../../scripts/helper.py?raw=1#example",
    ],
)
def test_rewrite_drops_every_unmapped_local_target(manifest, target):
    mapping = build_source_map(manifest, "site")

    assert (
        rewrite_for_surface(
            f"[Unpublished]({target})",
            "site",
            Path("docs/notebooks/example.md"),
            mapping,
        )
        == "Unpublished"
    )


def test_rewrite_changes_only_the_target_when_it_matches_the_label(manifest):
    mapping = build_source_map(manifest, "wiki")
    result = rewrite_for_surface(
        "[../scenarios/index.md](../scenarios/index.md)",
        "wiki",
        Path("docs/notebooks/example.md"),
        mapping,
    )
    assert result == "[../scenarios/index.md](Scenarios.md)"


def test_wiki_source_map_rejects_normalized_destination_collisions():
    manifest = parse_manifest(
        """\
surfaces: [repo, site, wiki]
numbering: baked
internal_roots: [docs/superpowers]
sections:
  - {id: foo_bar, number: '1', title: Foo Bar, source: docs/foo.md}
  - {id: foo-bar, number: '2', title: Foo-Bar, source: docs/bar.md}
diagrams: []
"""
    )

    with pytest.raises(ManifestError, match="wiki destination collision at Foo-Bar.md") as error:
        build_source_map(manifest, "wiki")

    assert "foo_bar (docs/foo.md)" in str(error.value)
    assert "foo-bar (docs/bar.md)" in str(error.value)


def test_wiki_source_map_reserves_home_for_the_overview_leaf():
    manifest = parse_manifest(
        """\
surfaces: [repo, site, wiki]
numbering: baked
internal_roots: [docs/superpowers]
sections:
  - {id: overview, number: '1', title: Overview, source: docs/index.md}
  - {id: home, number: '2', title: Home, source: docs/home.md}
diagrams: []
"""
    )

    with pytest.raises(ManifestError, match="wiki destination Home.md is reserved") as error:
        build_source_map(manifest, "wiki")

    assert "overview (docs/index.md)" in str(error.value)
    assert "home (docs/home.md)" in str(error.value)


@pytest.mark.parametrize(
    ("identifier", "source", "destination"),
    [
        ("_Sidebar", "docs/sidebar.md", "_Sidebar.md"),
        ("_Footer", "docs/footer.md", "_Footer.md"),
    ],
)
def test_wiki_source_map_reserves_structural_page_identifiers(identifier, source, destination):
    manifest = parse_manifest(
        f"""\
surfaces: [repo, site, wiki]
numbering: baked
internal_roots: [docs/superpowers]
sections:
  - {{id: {identifier}, number: '1', title: Reserved, source: {source}}}
diagrams: []
"""
    )

    with pytest.raises(ManifestError, match=rf"wiki destination {destination} is reserved") as error:
        build_source_map(manifest, "wiki")

    assert f"{identifier} ({source})" in str(error.value)


def test_site_source_map_rejects_destination_that_escapes_surface_root():
    manifest = parse_manifest(
        """\
surfaces: [repo, site, wiki]
numbering: baked
internal_roots: [docs/superpowers]
sections:
  - {id: overview, number: '1', title: Overview, source: docs/../README.md}
diagrams: []
"""
    )

    with pytest.raises(ManifestError, match="site destination escapes its surface root"):
        build_source_map(manifest, "site")


def test_site_source_map_independently_rejects_destination_collisions():
    manifest = parse_manifest(
        """\
surfaces: [repo, site, wiki]
numbering: baked
internal_roots: [docs/superpowers]
sections:
  - {id: overview, number: '1', title: Overview, source: docs/index.md}
  - {id: duplicate, number: '2', title: Duplicate, source: index.md}
diagrams: []
"""
    )

    with pytest.raises(ManifestError, match="site destination collision at index.md") as error:
        build_source_map(manifest, "site")

    assert "overview (docs/index.md)" in str(error.value)
    assert "duplicate (index.md)" in str(error.value)
