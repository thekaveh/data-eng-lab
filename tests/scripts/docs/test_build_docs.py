import subprocess
import sys
from pathlib import Path

import pytest

from scripts.docs.build_docs import (
    DocumentationDrift,
    assert_dirs_equal,
    build,
    hash_tree,
    render_mkdocs_yml,
    render_site,
    render_wiki,
)
from scripts.docs.manifest import ManifestError, iter_leaf_sections, load_manifest, parse_manifest


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
      - {id: scenario-catalog, number: '5.1', title: Catalog, source: docs/scenarios/index.md}
diagrams:
  - {id: overview, master: docs/diagrams/overview.html}
"""
    )


@pytest.fixture
def tmp_repo(tmp_path):
    repo = tmp_path / "repo"
    (repo / "docs/scenarios").mkdir(parents=True)
    (repo / "docs/diagrams/img").mkdir(parents=True)
    (repo / "docs/superpowers").mkdir()
    (repo / "docs/stylesheets").mkdir(parents=True)
    (repo / "docs/overrides").mkdir(parents=True)
    (repo / "docs/index.md").write_text(
        "# data-eng-lab\n\n[Catalog](scenarios/index.md)\n\n![Overview](diagrams/img/overview.png)\n",
        encoding="utf-8",
    )
    (repo / "docs/scenarios/index.md").write_text(
        "# 5.1. Catalog\n\n[Overview](../index.md)\n",
        encoding="utf-8",
    )
    (repo / "docs/diagrams/overview.html").write_text(
        '<html><svg xmlns="http://www.w3.org/2000/svg" width="16" height="9"></svg></html>',
        encoding="utf-8",
    )
    (repo / "docs/diagrams/img/overview.png").write_bytes(b"\x89PNG fixture")
    (repo / "docs/stylesheets/extra.css").write_text("body { color: cyan; }\n", encoding="utf-8")
    (repo / "docs/overrides/main.html").write_text('{% extends "base.html" %}\n', encoding="utf-8")
    return repo


def test_render_mkdocs_uses_generated_site_and_has_no_repo_controls(manifest):
    config = render_mkdocs_yml(manifest)

    assert "docs_dir: generated/site" in config
    assert "site_dir: site" in config
    assert "repo_url" not in config
    assert "repo_name" not in config
    assert "edit_uri" not in config
    assert '"1. Overview": index.md' in config
    assert "custom_dir: generated/site/overrides" in config
    assert "stylesheets/extra.css" in config
    assert "19 paired scenarios, 17 Scala/PySpark parity pairs" in config


def test_repository_mkdocs_description_matches_maven_app_inventory():
    repo_root = Path(__file__).resolve().parents[3]
    manifest = load_manifest(repo_root / "docs/manifest.yaml", repo_root)
    app_count = len(tuple((repo_root / "spark-apps").glob("*/pom.xml")))

    assert app_count == 6
    assert f"{app_count} CI-built Maven apps" in render_mkdocs_yml(manifest)


def test_render_site_and_wiki_are_complete(tmp_repo, manifest):
    render_site(manifest, tmp_repo, tmp_repo / "generated/site")
    render_wiki(manifest, tmp_repo, tmp_repo / "generated/wiki")

    assert (tmp_repo / "generated/site/index.md").is_file()
    assert (tmp_repo / "generated/site/scenarios/index.md").is_file()
    assert (tmp_repo / "generated/site/assets/img/overview.svg").is_file()
    assert (tmp_repo / "generated/site/stylesheets/extra.css").is_file()
    assert (tmp_repo / "generated/site/overrides/main.html").is_file()
    assert (tmp_repo / "generated/wiki/Home.md").is_file()
    assert (tmp_repo / "generated/wiki/Scenarios.md").is_file()
    assert (tmp_repo / "generated/wiki/_Sidebar.md").is_file()
    assert (tmp_repo / "generated/wiki/_Footer.md").is_file()
    assert (tmp_repo / "generated/wiki/img/overview.png").read_bytes() == b"\x89PNG fixture"
    assert "assets/img/overview.svg" in (tmp_repo / "generated/site/index.md").read_text(encoding="utf-8")
    assert "img/overview.png" in (tmp_repo / "generated/wiki/Home.md").read_text(encoding="utf-8")
    assert "http" not in (tmp_repo / "generated/wiki/_Footer.md").read_text(encoding="utf-8")


def test_renderer_independently_rejects_destination_outside_surface_root(tmp_repo, manifest, monkeypatch):
    monkeypatch.setattr(
        "scripts.docs.build_docs.build_source_map",
        lambda _manifest, _surface: {Path("docs/index.md"): Path("../escaped.md")},
    )

    with pytest.raises(ManifestError, match="site destination escapes its surface root"):
        render_site(manifest, tmp_repo, tmp_repo / "generated/site")

    assert not (tmp_repo / "generated/escaped.md").exists()


def test_repository_manifest_projects_all_public_pages_and_assets(tmp_path):
    repo_root = Path(__file__).resolve().parents[3]
    manifest = load_manifest(repo_root / "docs/manifest.yaml", repo_root)
    site = tmp_path / "site"
    wiki = tmp_path / "wiki"

    render_site(manifest, repo_root, site)
    render_wiki(manifest, repo_root, wiki)

    public_sources = {
        path.relative_to(repo_root / "docs")
        for path in (repo_root / "docs").rglob("*.md")
        if "superpowers" not in path.parts
    }
    assert len(tuple(iter_leaf_sections(manifest.sections))) == 64
    assert {path.relative_to(site) for path in site.rglob("*.md")} == public_sources
    assert len(tuple(wiki.glob("*.md"))) == 66
    assert len(tuple((site / "assets/img").glob("*.svg"))) == len(manifest.diagrams)
    assert len(tuple((wiki / "img").glob("*.png"))) == len(manifest.diagrams)
    assert "data-eng-lab-hero" in {diagram.id for diagram in manifest.diagrams}
    assert (site / "assets/img/data-eng-lab-hero.svg").is_file()
    assert (wiki / "img/data-eng-lab-hero.png").is_file()


def test_rendered_mkdocs_config_builds_strictly(tmp_repo, manifest):
    render_site(manifest, tmp_repo, tmp_repo / "generated/site")
    config = tmp_repo / "mkdocs.yml"
    config.write_text(render_mkdocs_yml(manifest), encoding="utf-8")

    result = subprocess.run(
        [sys.executable, "-m", "mkdocs", "build", "--strict", "--config-file", str(config)],
        cwd=tmp_repo,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_hash_tree_is_sorted_and_hashes_file_bytes(tmp_path):
    (tmp_path / "nested").mkdir()
    (tmp_path / "z.txt").write_bytes(b"last")
    (tmp_path / "nested/a.txt").write_bytes(b"first")

    hashes = hash_tree(tmp_path)

    assert list(hashes) == [Path("nested/a.txt"), Path("z.txt")]
    assert hashes[Path("nested/a.txt")] == "a7937b64b8caa58f03721bb6bacf5c78cb235febe0e70b1b84cd99541461a08e"


def test_assert_dirs_equal_detects_same_file_set_with_changed_content(tmp_path):
    actual = tmp_path / "actual"
    expected = tmp_path / "expected"
    actual.mkdir()
    expected.mkdir()
    (actual / "page.md").write_text("new", encoding="utf-8")
    (expected / "page.md").write_text("old", encoding="utf-8")

    with pytest.raises(DocumentationDrift, match="page.md"):
        assert_dirs_equal(actual, expected)


def test_build_check_rerenders_identical_trees(tmp_repo, manifest):
    (tmp_repo / "docs/manifest.yaml").write_text(
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
      - {id: scenario-catalog, number: '5.1', title: Catalog, source: docs/scenarios/index.md}
diagrams:
  - {id: overview, master: docs/diagrams/overview.html}
""",
        encoding="utf-8",
    )

    build(tmp_repo, site=True, wiki=True, check=True)

    assert (tmp_repo / "mkdocs.yml").read_text(encoding="utf-8") == render_mkdocs_yml(manifest)
    assert hash_tree(tmp_repo / "generated/site")
    assert hash_tree(tmp_repo / "generated/wiki")
