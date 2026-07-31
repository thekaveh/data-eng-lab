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
from scripts.docs.render_diagrams import extract_svg, svg_to_png


def _master(
    *, width: int = 800, height: int = 400, svg_count: int = 1, accessible: bool = True
) -> str:
    metadata = (
        ' role="img" aria-labelledby="diagram-title diagram-description">'
        '<title id="diagram-title">Overview</title>'
        '<desc id="diagram-description">Overview architecture.</desc>'
        if accessible
        else ">"
    )
    svg = (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}"'
        f"{metadata}</svg>"
    )
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
    (repo / "docs/superpowers").mkdir()
    (repo / "docs/stylesheets").mkdir(parents=True)
    (repo / "docs/overrides").mkdir(parents=True)
    (repo / "docs/index.md").write_text("# 1. Overview\n", encoding="utf-8")
    (repo / "docs/diagrams/overview.html").write_text(_master(), encoding="utf-8")
    svg_to_png(
        extract_svg((repo / "docs/diagrams/overview.html").read_text(encoding="utf-8")),
        repo / "docs/diagrams/img/overview.png",
    )
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


@pytest.fixture
def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def messages(findings):
    return [finding.message for finding in findings]


def test_atlas_acceptance_record_is_consistent(repo_root):
    required = {
        "docs/atlas-expectations.md": [
            "2026-07-31",
            "TaskFlow",
            "SparkSubmitHook",
            "succeeded",
            "#66",
            "#67",
            "#68",
        ],
        "docs/atlas-enablement.md": ["2026-07-31", "SparkSubmitOperator", "succeeded"],
        "docs/atlas-feedback-go-live.md": ["2026-07-31", "resolved", "FINISHED"],
        "docs/go-live-results.md": ["8,991,502", "passenger_count", "double", "success=true"],
        "docs/CHANGELOG.md": ["985918ce8c805081947d53b1c48bb80610237a5b", "2026-07-31"],
    }
    for relative, phrases in required.items():
        text = (repo_root / relative).read_text(encoding="utf-8")
        for phrase in phrases:
            assert phrase in text, f"{relative} is missing {phrase!r}"

    current_status_pages = (
        "docs/atlas-expectations.md",
        "docs/atlas-enablement.md",
        "docs/go-live.md",
        "docs/go-live-results.md",
        "docs/CHANGELOG.md",
    )
    stale_phrases = (
        "not yet claimed",
        "pending fresh retest",
        "awaits a fresh representative DAG run",
        "pending rerun",
    )
    for relative in current_status_pages:
        text = (repo_root / relative).read_text(encoding="utf-8")
        for phrase in stale_phrases:
            assert phrase not in text, f"{relative} retains stale wording {phrase!r}"


def test_completeness_ignores_only_explicit_internal_root(repo_fixture: Path):
    (repo_fixture / "docs/unmanifested.md").write_text("# Unmanifested\n", encoding="utf-8")
    (repo_fixture / "docs/superpowers/internal.md").parent.mkdir(parents=True, exist_ok=True)
    (repo_fixture / "docs/superpowers/internal.md").write_text("# Internal\n", encoding="utf-8")

    assert messages(check_completeness(repo_fixture)) == [
        "public Markdown is absent from manifest: docs/unmanifested.md"
    ]


def test_all_canonical_scans_follow_manifest_internal_roots(repo_fixture: Path):
    manifest = repo_fixture / "docs/manifest.yaml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace("docs/superpowers", "docs/private"),
        encoding="utf-8",
    )
    private = repo_fixture / "docs/private/internal.md"
    private.parent.mkdir()
    private.write_text("TO" + "DO" + ": private\n", encoding="utf-8")
    legacy = repo_fixture / "docs/superpowers/legacy.md"
    legacy.parent.mkdir(exist_ok=True)
    legacy.write_text("TO" + "DO" + ": legacy\n", encoding="utf-8")

    assert messages(check_completeness(repo_fixture)) == [
        "public Markdown is absent from manifest: docs/superpowers/legacy.md"
    ]
    assert messages(check_placeholders(repo_fixture)) == [
        "unfinished marker TODO in public documentation: docs/superpowers/legacy.md"
    ]

    private.write_text("", encoding="utf-8")
    legacy.write_text("", encoding="utf-8")
    assert messages(check_empty_artifacts(repo_fixture)) == [
        "empty public documentation file: docs/superpowers/legacy.md"
    ]


def test_numbering_matches_manifest_heading(repo_fixture: Path):
    (repo_fixture / "docs/index.md").write_text("preface\n# Overview\n", encoding="utf-8")

    assert messages(check_numbering(repo_fixture)) == [
        "docs/index.md heading must start with '# 1. Overview'"
    ]


def test_numbering_ignores_correct_h1_inside_fenced_code(repo_fixture: Path):
    (repo_fixture / "docs/index.md").write_text(
        "````markdown\n# 1. Overview\n```\n````\n\n# Wrong real heading\n",
        encoding="utf-8",
    )

    assert messages(check_numbering(repo_fixture)) == [
        "docs/index.md heading must start with '# 1. Overview'"
    ]


def test_numbering_uses_correct_real_h1_after_fenced_example(repo_fixture: Path):
    (repo_fixture / "docs/index.md").write_text(
        "~~~markdown\n# Wrong fenced heading\n~~~\n\n# 1. Overview\n",
        encoding="utf-8",
    )

    assert check_numbering(repo_fixture) == ()


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
    internal.parent.mkdir(parents=True, exist_ok=True)
    internal.write_text(f"# Internal\n\n{marker}: allowed\n", encoding="utf-8")

    assert messages(check_placeholders(repo_fixture)) == [
        "unfinished marker TODO in public documentation: docs/index.md"
    ]


def test_placeholders_scan_renderer_only_pages_and_generated_config(repo_fixture: Path):
    site = repo_fixture / "generated/site/renderer-only.md"
    sidebar = repo_fixture / "generated/wiki/_Sidebar.md"
    footer = repo_fixture / "generated/wiki/_Footer.md"
    site.parent.mkdir(parents=True)
    sidebar.parent.mkdir(parents=True)
    site.write_text("TO" + "DO" + ": site projection\n", encoding="utf-8")
    sidebar.write_text("TB" + "D" + ": sidebar\n", encoding="utf-8")
    footer.write_text("FIX" + "ME" + ": footer\n", encoding="utf-8")
    (repo_fixture / "mkdocs.yml").write_text("X" + "XX" + ": config\n", encoding="utf-8")

    assert messages(check_placeholders(repo_fixture)) == [
        "unfinished marker FIXME in public documentation: generated/wiki/_Footer.md",
        "unfinished marker TBD in public documentation: generated/wiki/_Sidebar.md",
        "unfinished marker TODO in public documentation: generated/site/renderer-only.md",
        "unfinished marker XXX in public documentation: mkdocs.yml",
    ]


def test_repository_cross_surface_link_fails(repo_fixture: Path):
    (repo_fixture / "README.md").write_text(
        "see https://thekaveh.github.io/data-eng-lab/\n", encoding="utf-8"
    )

    assert messages(check_self_containment(repo_fixture)) == [
        "README.md: repository surface links to the site surface"
    ]


def test_bare_generated_url_fails_but_fenced_clone_command_is_ignored(repo_fixture: Path):
    page = repo_fixture / "generated/site/index.md"
    page.parent.mkdir(parents=True)
    page.write_text(
        "Repository: https://github.com/thekaveh/data-eng-lab\n\n"
        "```bash\n"
        "git clone https://github.com/thekaveh/data-eng-lab.git\n"
        "```\n",
        encoding="utf-8",
    )
    assert messages(check_self_containment(repo_fixture)) == [
        "generated/site/index.md: site surface links to the repository surface"
    ]

    page.write_text(
        "```bash\n"
        "git clone https://github.com/thekaveh/data-eng-lab.git\n"
        "```\n",
        encoding="utf-8",
    )
    assert check_self_containment(repo_fixture) == ()


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


def test_existing_image_outside_surface_fails(repo_fixture: Path):
    outside = repo_fixture.parent / "outside.png"
    outside.write_bytes(_png())
    (repo_fixture / "README.md").write_text("![outside](../outside.png)\n", encoding="utf-8")

    assert messages(check_self_containment(repo_fixture)) == [
        "README.md: local image escapes repository surface: ../outside.png"
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


@pytest.mark.parametrize("surface", ["site", "wiki"])
def test_generated_missing_local_link_fails(repo_fixture: Path, surface: str):
    page = repo_fixture / f"generated/{surface}/Home.md"
    page.parent.mkdir(parents=True)
    page.write_text("[Registry](assets/registry.yaml)\n", encoding="utf-8")

    assert messages(check_self_containment(repo_fixture)) == [
        f"generated/{surface}/Home.md: missing local target assets/registry.yaml"
    ]


def test_generated_existing_local_link_passes(repo_fixture: Path):
    page = repo_fixture / "generated/wiki/Home.md"
    target = repo_fixture / "generated/wiki/Runbook.md"
    page.parent.mkdir(parents=True)
    page.write_text("[Runbook](Runbook.md#setup)\n", encoding="utf-8")
    target.write_text("# Setup\n", encoding="utf-8")

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


def test_diagram_gate_rejects_png_that_is_not_a_fresh_master_render(repo_fixture: Path):
    pytest.importorskip("cairosvg")
    site_images = repo_fixture / "generated/site/assets/img"
    site_images.mkdir(parents=True)
    (site_images / "overview.svg").write_text(
        "<svg xmlns=\"http://www.w3.org/2000/svg\"/>", encoding="utf-8"
    )
    stale = repo_fixture / "docs/diagrams/img/overview.png"
    stale.write_bytes(_png())

    assert "overview: committed PNG differs from fresh master render" in messages(
        check_diagrams(repo_fixture)
    )


def test_aggregate_gate_reports_stale_committed_png_without_rewriting_it(repo_fixture: Path):
    stale = repo_fixture / "docs/diagrams/img/overview.png"
    stale.write_bytes(_png())
    before = stale.read_bytes()

    assert "overview: committed PNG differs from fresh master render" in messages(
        check(repo_fixture)
    )
    assert stale.read_bytes() == before


def test_diagram_gate_reports_fresh_render_failure(repo_fixture: Path, monkeypatch):
    site_images = repo_fixture / "generated/site/assets/img"
    site_images.mkdir(parents=True)
    (site_images / "overview.svg").write_text("<svg/>", encoding="utf-8")

    def fail_render(*_args, **_kwargs):
        raise ValueError("renderer unavailable")

    monkeypatch.setattr("scripts.docs.check_docs.svg_to_png", fail_render)

    assert "overview: fresh PNG render failed: renderer unavailable" in messages(
        check_diagrams(repo_fixture)
    )


def test_diagram_gate_requires_accessible_svg_metadata(repo_fixture: Path):
    site_images = repo_fixture / "generated/site/assets/img"
    site_images.mkdir(parents=True)
    (site_images / "overview.svg").write_text("<svg/>", encoding="utf-8")
    (repo_fixture / "docs/diagrams/overview.html").write_text(
        _master(accessible=False), encoding="utf-8"
    )

    assert "overview: SVG must have role=\"img\"" in messages(check_diagrams(repo_fixture))


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
