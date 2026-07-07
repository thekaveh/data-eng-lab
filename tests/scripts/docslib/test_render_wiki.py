from pathlib import Path

from docslib.parse import build_doc_map, parse_site
from docslib.render_wiki import render_wiki_surface

FIX = Path(__file__).resolve().parents[1] / "_fixtures" / "repo"


def test_wiki_has_home_sidebar_scenarios_no_banner():
    sm = parse_site(FIX)
    dm = build_doc_map(sm)
    out = render_wiki_surface(sm, dm, FIX)
    names = {p.name for p in out}
    assert "Home.md" in names and "_Sidebar.md" in names
    scen = out[Path("Scenario-batch_ingest-nyc_taxi-spark-iceberg.md")]
    assert "Full docs site" not in scen            # no banner
    assert "thekaveh.github.io" not in scen
    assert "[[Datasets]]" in scen                  # concept → wiki link
    assert "batch_ingest-nyc_taxi-spark-iceberg.svg" in scen  # diagram localized


def test_sidebar_lists_sections():
    sm = parse_site(FIX)
    dm = build_doc_map(sm)
    out = render_wiki_surface(sm, dm, FIX)
    bar = out[Path("_Sidebar.md")]
    assert "[[Home]]" in bar
    assert "Scenario-batch_ingest" in bar or "batch_ingest" in bar
