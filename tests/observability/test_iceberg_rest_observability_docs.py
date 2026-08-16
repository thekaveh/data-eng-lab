from pathlib import Path

import yaml

from scripts.docs.build_docs import render_site, render_wiki
from scripts.docs.manifest import load_manifest
from scripts.docs.transforms import build_source_map

ROOT = Path(__file__).resolve().parents[2]
SOURCE = Path("docs/iceberg-rest-observability.md")
METRICS = (
    "data_eng_lab_iceberg_rest_synthetic_probe_success",
    "data_eng_lab_iceberg_rest_synthetic_probe_duration_seconds",
    "data_eng_lab_iceberg_rest_synthetic_probe_http_status_code",
    "data_eng_lab_iceberg_rest_synthetic_probe_result",
)
RESULTS = ("success", "slow", "malformed", "timeout", "http_error", "unavailable")
ALERTS = (
    "IcebergRestSyntheticExporterMissing",
    "IcebergRestSyntheticUnavailable",
    "IcebergRestSyntheticSlow",
)


def _assert_contract(text: str) -> None:
    assert text.startswith("# 8.9. Iceberg REST Observability\n")
    assert "synthetic" in text.lower()
    assert "native request totals" in text
    assert "per-route request latency" in text
    assert "30 days" in text
    assert "99.5%" in text
    assert "p95" in text and "1 second" in text
    assert "no Alertmanager" in text
    assert "does not page" in text
    assert "consumer-owned" in text
    assert "does not modify Atlas" in text
    assert "upstream Iceberg" in text
    for value in (*METRICS, *RESULTS, *ALERTS):
        assert value in text
    assert "avg_over_time" in text
    assert "quantile_over_time(0.95" in text
    assert "retention_quarantine" not in text


def test_canonical_runbook_and_manifest_define_leaf_8_9() -> None:
    _assert_contract((ROOT / SOURCE).read_text(encoding="utf-8"))
    manifest = yaml.safe_load((ROOT / "docs/manifest.yaml").read_text(encoding="utf-8"))
    atlas = next(section for section in manifest["sections"] if section["id"] == "atlas-operations")
    assert atlas["children"][-1] == {
        "id": "iceberg-rest-observability",
        "number": "8.9",
        "title": "Iceberg REST Observability",
        "source": SOURCE.as_posix(),
    }


def test_site_and_wiki_project_the_same_runbook_contract(tmp_path: Path) -> None:
    manifest = load_manifest(ROOT / "docs/manifest.yaml", ROOT)
    site = tmp_path / "site"
    wiki = tmp_path / "wiki"
    render_site(manifest, ROOT, site)
    render_wiki(manifest, ROOT, wiki)
    mappings = {
        "site": (site, build_source_map(manifest, "site")),
        "wiki": (wiki, build_source_map(manifest, "wiki")),
    }
    for root, mapping in mappings.values():
        _assert_contract((root / mapping[SOURCE]).read_text(encoding="utf-8"))


def test_go_live_feedback_records_the_delivered_synthetic_boundary() -> None:
    text = (ROOT / "docs/atlas-feedback-go-live.md").read_text(encoding="utf-8")
    assert "Synthetic Iceberg REST availability and latency are delivered by #90" in text
    assert "authoritative native request totals remain unavailable" in text
    assert "Add observability metrics for the Iceberg REST catalog (query counts, latency)." not in text
