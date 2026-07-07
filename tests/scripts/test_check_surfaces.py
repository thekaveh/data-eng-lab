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
