import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from scripts.docs.manifest import DiagramEntry, Manifest
from scripts.docs.render_diagrams import (
    DiagramError,
    copy_assets,
    diagram_fingerprint,
    extract_svg,
    import_svg_master,
    projection_fingerprint_path,
    render_all,
    svg_to_png,
)


def test_overview_diagram_includes_observability_and_no_authoring_metadata():
    root = Path(__file__).resolve().parents[3]
    svg = extract_svg((root / "docs/diagrams/overview.html").read_text(encoding="utf-8"))

    for label in ("iceberg-rest-probe", "Prometheus", "Grafana", "metrics", "dashboard"):
        assert label in svg
    for leaked in ("Orbital theme", "landscape"):
        assert leaked not in svg


def test_lakehouse_hero_is_wide_text_free_and_accessible():
    root = Path(__file__).resolve().parents[3]
    master = root / "docs/diagrams/data-eng-lab-hero.html"

    svg = extract_svg(master.read_text(encoding="utf-8"))
    element = ET.fromstring(svg)

    assert element.attrib["width"] == "1800"
    assert element.attrib["height"] == "560"
    assert element.attrib["viewBox"] == "0 0 1800 560"
    assert not any(child.tag.endswith("text") for child in element.iter())
    assert "lakehouse" in svg.casefold()
    assert "bronze, silver, and gold" in svg.casefold()


def test_lakehouse_hero_output_arrow_points_right():
    root = Path(__file__).resolve().parents[3]
    master = root / "docs/diagrams/data-eng-lab-hero.html"

    svg = extract_svg(master.read_text(encoding="utf-8"))
    element = ET.fromstring(svg)
    path_data = {child.attrib.get("d") for child in element.iter() if child.tag.endswith("path")}

    assert "M1138 270 l-18 -10 v20 z" in path_data


def test_extract_svg_sanitizes_html_named_entities_and_preserves_numeric_entities():
    master = (
        '<html><svg xmlns="http://www.w3.org/2000/svg">'
        "<text>&Sigma; &middot; &amp; &#931; &#x3A3;</text>"
        "</svg></html>"
    )

    assert extract_svg(master) == (
        '<svg xmlns="http://www.w3.org/2000/svg"><text>Σ · &amp; &#931; &#x3A3;</text></svg>'
    )


def test_extract_svg_rejects_missing_or_multiple_roots():
    with pytest.raises(DiagramError, match="exactly one inline svg"):
        extract_svg("<html><body>No diagram</body></html>")
    with pytest.raises(DiagramError, match="exactly one inline svg"):
        extract_svg("<html><svg></svg><svg></svg></html>")


@pytest.mark.parametrize(
    "payload",
    [
        "<script>alert(1)</script>",
        '<rect onload="alert(1)"/>',
        '<a href="javascript:alert(1)"><text>open</text></a>',
        '<image xmlns:xlink="http://www.w3.org/1999/xlink" '
        'xlink:href="data:image/svg+xml,&lt;svg onload=alert(1)&gt;"/>',
        '<rect style="fill:url(javascript:alert(1))"/>',
        '<foreignObject><div xmlns="http://www.w3.org/1999/xhtml">unsafe</div></foreignObject>',
    ],
)
def test_extract_svg_rejects_executable_content(payload):
    master = f'<html><svg xmlns="http://www.w3.org/2000/svg">{payload}</svg></html>'

    with pytest.raises(DiagramError, match="unsafe SVG"):
        extract_svg(master)


def test_extract_svg_rejects_obfuscated_executable_uri():
    master = (
        '<html><svg xmlns="http://www.w3.org/2000/svg">'
        '<a href="java&#x73;cript:alert(1)"><text>open</text></a>'
        "</svg></html>"
    )

    with pytest.raises(DiagramError, match="unsafe SVG"):
        extract_svg(master)


def test_import_svg_master_preserves_the_complete_svg():
    svg = '<svg xmlns="http://www.w3.org/2000/svg"><path d="M0 0h10v10z"/></svg>'

    master = import_svg_master(
        svg,
        title="Overview",
        evidence="Verified against atlas.consumer.yml on 2026-07-31.",
    )

    assert master.count("<svg") == 1
    assert '<path d="M0 0h10v10z"/>' in master
    assert "Verified against atlas.consumer.yml on 2026-07-31." in master
    assert 'role="img"' in master
    assert 'aria-labelledby="diagram-title diagram-description"' in master
    assert '<title id="diagram-title">Overview</title>' in master
    assert (
        '<desc id="diagram-description">Verified against atlas.consumer.yml on 2026-07-31.</desc>'
        in master
    )


def test_import_svg_master_rejects_non_standalone_input():
    with pytest.raises(DiagramError, match="one standalone svg root"):
        import_svg_master("<div><svg></svg></div>", title="Bad", evidence="None")


def test_svg_to_png_writes_png_magic(tmp_path):
    pytest.importorskip("cairosvg")
    destination = tmp_path / "nested" / "diagram.png"

    svg_to_png('<svg xmlns="http://www.w3.org/2000/svg" width="10" height="10"/>', destination, width=100)

    assert destination.read_bytes().startswith(b"\x89PNG\r\n\x1a\n")


def test_render_all_writes_site_svg_and_committed_png(tmp_path):
    pytest.importorskip("cairosvg")
    master_path = Path("docs/diagrams/overview.html")
    master = import_svg_master(
        '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="9"><text>&Sigma;</text></svg>',
        title="Overview",
        evidence="Verified against fixture.",
    )
    (tmp_path / master_path).parent.mkdir(parents=True)
    (tmp_path / master_path).write_text(master, encoding="utf-8")
    manifest = Manifest(
        surfaces=("repo", "site", "wiki"),
        numbering="baked",
        internal_roots=(),
        sections=(),
        diagrams=(DiagramEntry("overview", master_path),),
    )

    render_all(
        manifest,
        tmp_path,
        tmp_path / "generated/site/assets/img",
        tmp_path / "docs/diagrams/img",
    )

    svg_path = tmp_path / "generated/site/assets/img/overview.svg"
    png_path = tmp_path / "docs/diagrams/img/overview.png"
    first_svg = svg_path.read_bytes()
    first_png = png_path.read_bytes()
    assert first_svg.endswith(b"\n")
    assert first_png.startswith(b"\x89PNG")
    fingerprint_path = projection_fingerprint_path(png_path)
    assert fingerprint_path.read_text(encoding="utf-8") == (
        f"sha256:{diagram_fingerprint(extract_svg(master))}\n"
    )

    render_all(
        manifest,
        tmp_path,
        tmp_path / "generated/site/assets/img",
        tmp_path / "docs/diagrams/img",
    )

    assert svg_path.read_bytes() == first_svg
    assert png_path.read_bytes() == first_png


def test_matching_fingerprint_avoids_host_specific_png_rerender(tmp_path, monkeypatch):
    master_path = Path("docs/diagrams/overview.html")
    master = import_svg_master(
        '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="9"></svg>',
        title="Overview",
        evidence="Verified against fixture.",
    )
    (tmp_path / master_path).parent.mkdir(parents=True)
    (tmp_path / master_path).write_text(master, encoding="utf-8")
    manifest = Manifest(
        surfaces=("repo", "site", "wiki"),
        numbering="baked",
        internal_roots=(),
        sections=(),
        diagrams=(DiagramEntry("overview", master_path),),
    )
    png = tmp_path / "docs/diagrams/img/overview.png"
    png.parent.mkdir(parents=True)
    png.write_bytes(b"host-specific png bytes")
    projection_fingerprint_path(png).write_text(
        f"sha256:{diagram_fingerprint(extract_svg(master))}\n", encoding="utf-8"
    )

    def unexpected_render(*_args, **_kwargs):
        raise AssertionError("matching source fingerprint must not rerender")

    monkeypatch.setattr("scripts.docs.render_diagrams.svg_to_png", unexpected_render)
    render_all(
        manifest,
        tmp_path,
        tmp_path / "generated/site/assets/img",
        tmp_path / "docs/diagrams/img",
    )

    assert png.read_bytes() == b"host-specific png bytes"


def test_copy_assets_copies_only_png_files(tmp_path):
    png_dir = tmp_path / "png"
    wiki_dir = tmp_path / "wiki"
    png_dir.mkdir()
    (png_dir / "overview.png").write_bytes(b"\x89PNG fixture")
    (png_dir / "notes.txt").write_text("not an image", encoding="utf-8")

    copy_assets(png_dir, wiki_dir)

    assert (wiki_dir / "overview.png").read_bytes() == b"\x89PNG fixture"
    assert not (wiki_dir / "notes.txt").exists()


def test_every_committed_master_has_accessible_svg_metadata():
    root = Path(__file__).resolve().parents[3]

    for path in sorted((root / "docs/diagrams").glob("*.html")):
        svg = extract_svg(path.read_text(encoding="utf-8"))
        assert 'role="img"' in svg, path
        assert 'aria-labelledby="diagram-title diagram-description"' in svg, path
        assert '<title id="diagram-title">' in svg, path
        assert '<desc id="diagram-description">' in svg, path
