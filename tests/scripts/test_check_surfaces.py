import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("check_surfaces", ROOT / "scripts" / "check_surfaces.py")


def _load():
    m = importlib.util.module_from_spec(SPEC)
    SPEC.loader.exec_module(m)
    return m


def test_clean_repo_passes(tmp_path):
    cs = _load()
    (tmp_path / "README.md").write_text("ok no links\n")
    (tmp_path / "scenarios").mkdir()
    (tmp_path / "scenarios" / "a").mkdir()
    (tmp_path / "scenarios" / "a" / "README.md").write_text("# a\n")
    assert cs.main(["--root", str(tmp_path)]) == 0


def test_io_link_in_readme_fails(tmp_path):
    cs = _load()
    (tmp_path / "README.md").write_text("see https://thekaveh.github.io/data-eng-lab/\n")
    assert cs.main(["--root", str(tmp_path)]) == 1


def test_docs_relative_link_fails(tmp_path):
    cs = _load()
    (tmp_path / "scenarios").mkdir()
    (tmp_path / "scenarios" / "a").mkdir()
    (tmp_path / "scenarios" / "a" / "README.md").write_text("[x](../../docs/datasets.md)\n")
    assert cs.main(["--root", str(tmp_path)]) == 1


def test_root_readme_committed_diagram_png_passes(tmp_path):
    cs = _load()
    image = tmp_path / "docs" / "diagrams" / "img" / "overview.png"
    image.parent.mkdir(parents=True)
    image.write_bytes(b"\x89PNG fixture")
    (tmp_path / "README.md").write_text("![diagram](docs/diagrams/img/overview.png)\n")
    assert cs.main(["--root", str(tmp_path)]) == 0


def test_nested_readme_committed_diagram_png_passes(tmp_path):
    cs = _load()
    image = tmp_path / "docs" / "diagrams" / "img" / "scenario.png"
    image.parent.mkdir(parents=True)
    image.write_bytes(b"\x89PNG fixture")
    readme = tmp_path / "scenarios" / "scenario" / "README.md"
    readme.parent.mkdir(parents=True)
    readme.write_text("![diagram](../../docs/diagrams/img/scenario.png)\n")
    assert cs.main(["--root", str(tmp_path)]) == 0


def test_missing_committed_diagram_png_fails(tmp_path):
    cs = _load()
    (tmp_path / "README.md").write_text("![diagram](docs/diagrams/img/missing.png)\n")
    assert cs.main(["--root", str(tmp_path)]) == 1


def test_wrong_extension_in_committed_diagram_path_fails(tmp_path):
    cs = _load()
    image = tmp_path / "docs" / "diagrams" / "img" / "overview.svg"
    image.parent.mkdir(parents=True)
    image.write_text("<svg/>")
    (tmp_path / "README.md").write_text("![diagram](docs/diagrams/img/overview.svg)\n")
    assert cs.main(["--root", str(tmp_path)]) == 1


def test_wiki_banner_fails(tmp_path):
    cs = _load()
    (tmp_path / "wiki").mkdir()
    (tmp_path / "wiki" / "Home.md").write_text("> Full docs site: https://thekaveh.github.io/...\n")
    assert cs.main(["--root", str(tmp_path)]) == 1


def test_missing_local_svg_fails(tmp_path):
    cs = _load()
    (tmp_path / "scenarios").mkdir()
    (tmp_path / "scenarios" / "a").mkdir()
    (tmp_path / "scenarios" / "a" / "README.md").write_text("![d](architectures/a.svg)\n")
    assert cs.main(["--root", str(tmp_path)]) == 1


def test_wiki_missing_local_svg_fails(tmp_path):
    """A wiki page embedding a bare SVG ref with no local copy in wiki/ must fail."""
    cs = _load()
    (tmp_path / "README.md").write_text("ok no links\n")
    (tmp_path / "wiki").mkdir()
    (tmp_path / "wiki" / "Home.md").write_text("![lead](overview.svg)\n")
    assert cs.main(["--root", str(tmp_path)]) == 1


def test_wiki_svg_with_local_copy_passes(tmp_path):
    """A wiki page embedding a bare SVG ref whose local copy exists must pass."""
    cs = _load()
    (tmp_path / "README.md").write_text("ok no links\n")
    (tmp_path / "wiki").mkdir()
    (tmp_path / "wiki" / "Home.md").write_text("![lead](overview.svg)\n")
    (tmp_path / "wiki" / "overview.svg").write_text("<svg/>")
    assert cs.main(["--root", str(tmp_path)]) == 0
