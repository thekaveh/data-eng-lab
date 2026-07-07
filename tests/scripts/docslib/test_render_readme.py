from pathlib import Path

from docslib.parse import build_doc_map, parse_site
from docslib.render_readme import AUTOGEN_HEADER, render_readme_surface

FIX = Path(__file__).resolve().parents[1] / "_fixtures" / "repo"


def test_root_readme_is_self_contained(tmp_path):
    sm = parse_site(FIX)
    dm = build_doc_map(sm)
    out = render_readme_surface(sm, dm, FIX)
    root = out[Path("README.md")]
    assert root.startswith(AUTOGEN_HEADER)
    assert "thekaveh.github.io" not in root          # no .io link
    assert "docs/" not in root.split("## ")[0]       # no docs/ path in head
    assert "README.md#datasets" in root              # concept → root anchor


def test_scenario_readme_internal_links_only(tmp_path):
    sm = parse_site(FIX)
    dm = build_doc_map(sm)
    out = render_readme_surface(sm, dm, FIX)
    sreadme = out[Path("scenarios/batch_ingest-nyc_taxi-spark-iceberg/README.md")]
    assert sreadme.startswith(AUTOGEN_HEADER)
    assert "thekaveh.github.io" not in sreadme
    assert "../../docs/" not in sreadme              # no docs/ cross-link
    assert "../README.md#datasets" in sreadme        # concept → root README
    # diagram localized
    assert "architectures/batch_ingest-nyc_taxi-spark-iceberg.svg" in sreadme


def test_notebooks_md_emitted():
    sm = parse_site(FIX)
    dm = build_doc_map(sm)
    out = render_readme_surface(sm, dm, FIX)
    assert Path("scenarios/batch_ingest-nyc_taxi-spark-iceberg/notebooks.md") in out
