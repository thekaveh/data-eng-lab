from pathlib import Path

from docslib.parse import build_doc_map, parse_site

FIX = Path(__file__).resolve().parents[1] / "_fixtures" / "repo"


def test_parse_builds_pages_and_scenarios():
    sm = parse_site(FIX)
    key = "scenarios/batch_ingest-nyc_taxi-spark-iceberg.md"
    assert key in sm.pages
    page = sm.pages[key]
    assert page.title == "batch_ingest-nyc_taxi-spark-iceberg"
    nums = {s.number for s in page.sections}
    assert "1" in nums and "8" in nums
    assert "medallion-nyc_taxi-spark-iceberg.md" in page.see_also
    assert len(sm.scenarios) == 1
    assert sm.scenarios[0].name == "batch_ingest-nyc_taxi-spark-iceberg"


def test_nav_numbers_assigned():
    sm = parse_site(FIX)
    titles = {n.title: n.number for n in sm.nav}
    assert titles["Datasets"] == "2"


def test_doc_map_readme_and_wiki():
    sm = parse_site(FIX)
    dm = build_doc_map(sm)
    k = "scenarios/batch_ingest-nyc_taxi-spark-iceberg.md"
    assert dm.readme[k].endswith("/README.md")
    assert dm.wiki[k].startswith("Scenario-")
