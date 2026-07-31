"""Tests for the canonical aggregate documentation gate."""

from pathlib import Path

import pytest

from scripts.docs.check_docs import (
    check,
    check_completeness,
    check_diagrams,
    check_empty_artifacts,
    check_numbering,
    check_placeholders,
    check_self_containment,
)


def _master(*, width: int = 800, height: int = 400, svg_count: int = 1) -> str:
    svg = f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}"></svg>'
    return f"<html><body>{svg * svg_count}</body></html>"


def _png(*, width: int = 800, height: int = 400) -> bytes:
    return (
        b"\x89PNG\r\n\x1a\n"
        + b"\x00\x00\x00\rIHDR"
        + width.to_bytes(4, "big")
        + height.to_bytes(4, "big")
    )


@pytest.fixture
def repo_fixture(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    (repo / "docs/diagrams/img").mkdir(parents=True)
    (repo / "docs/stylesheets").mkdir(parents=True)
    (repo / "docs/overrides").mkdir(parents=True)
    (repo / "docs/index.md").write_text("# 1. Overview\n", encoding="utf-8")
    (repo / "docs/diagrams/overview.html").write_text(_master(), encoding="utf-8")
    (repo / "docs/diagrams/img/overview.png").write_bytes(_png())
    (repo / "docs/stylesheets/extra.css").write_text("body { color: cyan; }\n", encoding="utf-8")
    (repo / "docs/overrides/main.html").write_text('{% extends "base.html" %}\n', encoding="utf-8")
    (repo / "docs/manifest.yaml").write_text(
        """\
surfaces: [repo, site, wiki]
numbering: baked
internal_roots: [docs/superpowers]
sections:
  - {id: overview, number: '1', title: Overview, source: docs/index.md}
diagrams:
  - {id: overview, master: docs/diagrams/overview.html}
""",
        encoding="utf-8",
    )
    return repo


def messages(findings):
    return [finding.message for finding in findings]


def test_completeness_ignores_only_explicit_internal_root(repo_fixture: Path):
    (repo_fixture / "docs/unmanifested.md").write_text("# Unmanifested\n", encoding="utf-8")
    (repo_fixture / "docs/superpowers/internal.md").parent.mkdir(parents=True)
    (repo_fixture / "docs/superpowers/internal.md").write_text("# Internal\n", encoding="utf-8")

    assert messages(check_completeness(repo_fixture)) == [
        "public Markdown is absent from manifest: docs/unmanifested.md"
    ]


def test_numbering_matches_manifest_heading(repo_fixture: Path):
    (repo_fixture / "docs/index.md").write_text("preface\n# Overview\n", encoding="utf-8")

    assert messages(check_numbering(repo_fixture)) == [
        "docs/index.md heading must start with '# 1. Overview'"
    ]


def test_empty_public_artifacts_are_errors_but_generated_dirs_are_not(repo_fixture: Path):
    (repo_fixture / "docs/empty.md").touch()
    (repo_fixture / "generated/empty").mkdir(parents=True)

    assert messages(check_empty_artifacts(repo_fixture)) == [
        "empty public documentation file: docs/empty.md"
    ]


def test_empty_public_directories_are_errors(repo_fixture: Path):
    (repo_fixture / "docs/empty-dir").mkdir()

    assert messages(check_empty_artifacts(repo_fixture)) == [
        "empty public documentation directory: docs/empty-dir"
    ]


def test_placeholders_ignore_only_internal_documentation(repo_fixture: Path):
    marker = "TO" + "DO"
    (repo_fixture / "docs/index.md").write_text(f"# 1. Overview\n\n{marker}: finish\n", encoding="utf-8")
    internal = repo_fixture / "docs/superpowers/internal.md"
    internal.parent.mkdir(parents=True)
    internal.write_text(f"# Internal\n\n{marker}: allowed\n", encoding="utf-8")

    assert messages(check_placeholders(repo_fixture)) == [
        "unfinished marker TODO in public documentation: docs/index.md"
    ]


def test_repository_cross_surface_link_fails(repo_fixture: Path):
    (repo_fixture / "README.md").write_text(
        "see https://thekaveh.github.io/data-eng-lab/\n", encoding="utf-8"
    )

    assert messages(check_self_containment(repo_fixture)) == [
        "README.md: repository surface links to the site surface"
    ]


@pytest.mark.parametrize("target", ["./docs/index.md", "x/../docs/index.md"])
def test_repository_docs_page_link_fails_after_normalization(repo_fixture: Path, target: str):
    (repo_fixture / "x").mkdir()
    (repo_fixture / "README.md").write_text(f"[docs]({target})\n", encoding="utf-8")

    assert messages(check_self_containment(repo_fixture)) == [
        f"README.md: forbidden docs/-relative target {target}"
    ]


def test_committed_diagram_png_is_the_only_docs_exception(repo_fixture: Path):
    (repo_fixture / "README.md").write_text(
        "![diagram](docs/diagrams/img/overview.png?raw=1#overview)\n", encoding="utf-8"
    )
    assert check_self_containment(repo_fixture) == ()

    (repo_fixture / "docs/diagrams/img/overview.svg").write_text("<svg/>", encoding="utf-8")
    (repo_fixture / "README.md").write_text(
        "![diagram](docs/diagrams/img/overview.svg?raw=1)\n", encoding="utf-8"
    )
    assert messages(check_self_containment(repo_fixture)) == [
        "README.md: forbidden docs/-relative target docs/diagrams/img/overview.svg?raw=1"
    ]


def test_nested_committed_diagram_png_passes(repo_fixture: Path):
    readme = repo_fixture / "scenarios/example/README.md"
    readme.parent.mkdir(parents=True)
    (readme.parent / "assets").mkdir()
    readme.write_text(
        "![diagram](assets/../../../docs/diagrams/img/overview.png#diagram)\n",
        encoding="utf-8",
    )

    assert check_self_containment(repo_fixture) == ()


def test_docs_exception_cannot_escape_root_then_reenter(repo_fixture: Path):
    target = f"../{repo_fixture.name}/docs/diagrams/img/overview.png"
    (repo_fixture / "README.md").write_text(f"![diagram]({target})\n", encoding="utf-8")

    assert messages(check_self_containment(repo_fixture)) == [
        f"README.md: forbidden docs/-relative target {target}"
    ]


def test_missing_repository_image_fails(repo_fixture: Path):
    readme = repo_fixture / "scenarios/example/README.md"
    readme.parent.mkdir(parents=True)
    readme.write_text("![diagram](architectures/missing.svg)\n", encoding="utf-8")

    assert messages(check_self_containment(repo_fixture)) == [
        "scenarios/example/README.md: missing local image architectures/missing.svg"
    ]


def test_missing_committed_diagram_png_fails(repo_fixture: Path):
    (repo_fixture / "README.md").write_text(
        "![diagram](docs/diagrams/img/missing.png)\n", encoding="utf-8"
    )

    assert messages(check_self_containment(repo_fixture)) == [
        "README.md: forbidden docs/-relative target docs/diagrams/img/missing.png",
        "README.md: missing local image docs/diagrams/img/missing.png",
    ]


def test_wiki_banner_and_missing_image_fail(repo_fixture: Path):
    page = repo_fixture / "generated/wiki/Home.md"
    page.parent.mkdir(parents=True)
    page.write_text(
        "> Full docs site: https://thekaveh.github.io/data-eng-lab/\n\n![lead](overview.svg)\n",
        encoding="utf-8",
    )

    assert messages(check_self_containment(repo_fixture)) == [
        "generated/wiki/Home.md: contains mirror banner",
        "generated/wiki/Home.md: missing local image overview.svg",
        "generated/wiki/Home.md: wiki surface links to the site surface",
    ]


def test_surface_local_wiki_image_passes(repo_fixture: Path):
    page = repo_fixture / "generated/wiki/Home.md"
    page.parent.mkdir(parents=True)
    page.write_text("![lead](overview.svg)\n", encoding="utf-8")
    (page.parent / "overview.svg").write_text("<svg/>", encoding="utf-8")

    assert check_self_containment(repo_fixture) == ()


def test_diagram_inventory_and_file_validation(repo_fixture: Path):
    site_images = repo_fixture / "generated/site/assets/img"
    site_images.mkdir(parents=True)
    (site_images / "overview.svg").write_text("<svg/>", encoding="utf-8")
    assert check_diagrams(repo_fixture) == ()

    (repo_fixture / "docs/diagrams/extra.html").write_text(_master(), encoding="utf-8")
    (repo_fixture / "docs/diagrams/img/overview.png").write_bytes(b"not png")
    (site_images / "overview.svg").unlink()

    assert messages(check_diagrams(repo_fixture)) == [
        "HTML masters unexpected ids: extra",
        "PNG projection overview has invalid PNG magic",
        "site SVG projections missing ids: overview",
    ]


def test_empty_diagram_manifest_cannot_make_gate_vacuous(repo_fixture: Path):
    (repo_fixture / "docs/manifest.yaml").write_text(
        """\
surfaces: [repo, site, wiki]
numbering: baked
internal_roots: [docs/superpowers]
sections:
  - {id: overview, number: '1', title: Overview, source: docs/index.md}
diagrams: []
""",
        encoding="utf-8",
    )

    assert messages(check_diagrams(repo_fixture)) == [
        "docs/manifest.yaml must declare at least one diagram"
    ]


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b"not png", "PNG projection overview has invalid PNG magic"),
        (_png(width=0), "PNG projection overview dimensions must be nonzero"),
    ],
)
def test_invalid_or_zero_dimension_png_fails(
    repo_fixture: Path, payload: bytes, message: str
):
    site_image = repo_fixture / "generated/site/assets/img/overview.svg"
    site_image.parent.mkdir(parents=True)
    site_image.write_text("<svg/>", encoding="utf-8")
    (repo_fixture / "docs/diagrams/img/overview.png").write_bytes(payload)

    assert message in messages(check_diagrams(repo_fixture))


def test_non_landscape_and_multiple_svg_master_fail(repo_fixture: Path):
    site_image = repo_fixture / "generated/site/assets/img/overview.svg"
    site_image.parent.mkdir(parents=True)
    site_image.write_text("<svg/>", encoding="utf-8")
    master = repo_fixture / "docs/diagrams/overview.html"
    master.write_text(_master(width=400, height=800), encoding="utf-8")
    assert messages(check_diagrams(repo_fixture)) == [
        "overview: SVG is not landscape (400.0x800.0)"
    ]

    master.write_text(_master(svg_count=2), encoding="utf-8")
    assert messages(check_diagrams(repo_fixture)) == [
        "overview: master must contain exactly one inline SVG"
    ]


def test_aggregate_gate_renders_both_surfaces_and_checks_determinism(repo_fixture: Path):
    assert check(repo_fixture) == ()
    assert (repo_fixture / "generated/site/index.md").is_file()
    assert (repo_fixture / "generated/wiki/Home.md").is_file()
    assert (repo_fixture / "mkdocs.yml").is_file()


def test_findings_are_sorted_by_message(repo_fixture: Path):
    (repo_fixture / "docs/z-last.md").touch()
    (repo_fixture / "docs/a-first.md").touch()

    result = check(repo_fixture)
    assert list(result) == sorted(result, key=lambda finding: finding.message)
