from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DASHBOARD_PATH = ROOT / "observability/grafana/iceberg-rest.json"
ALLOWED_METRICS = {
    "data_eng_lab_iceberg_rest_synthetic_probe_success",
    "data_eng_lab_iceberg_rest_synthetic_probe_duration_seconds",
    "data_eng_lab_iceberg_rest_synthetic_probe_result",
    "ALERTS",
    "up",
}


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        assert key not in value
        value[key] = item
    return value


def test_dashboard_is_strict_stable_and_has_six_nonoverlapping_panels() -> None:
    dashboard = json.loads(
        DASHBOARD_PATH.read_text(encoding="utf-8"),
        object_pairs_hook=_unique_object,
        parse_constant=lambda value: (_ for _ in ()).throw(AssertionError(value)),
    )
    assert dashboard["uid"] == "data-eng-lab-iceberg-rest-synthetic"
    assert dashboard["title"] == "Iceberg REST Synthetic Observability"
    assert dashboard["tags"] == ["data-eng-lab", "iceberg", "synthetic"]
    assert dashboard["refresh"] == "30s"
    assert dashboard["timezone"] == "utc"
    assert dashboard["links"] == []
    assert dashboard["templating"] == {"list": []}
    assert "annotations" not in dashboard

    panels = dashboard["panels"]
    assert [panel["title"] for panel in panels] == [
        "Current availability",
        "Current latency",
        "30-day availability SLO",
        "30-day p95 latency SLO",
        "Current synthetic outcome",
        "Active Iceberg REST alerts",
    ]
    ids = [panel["id"] for panel in panels]
    assert len(ids) == len(set(ids))
    occupied: set[tuple[int, int]] = set()
    for panel in panels:
        assert panel["datasource"] == {"type": "prometheus", "uid": "Prometheus"}
        grid = panel["gridPos"]
        cells = {
            (x, y) for x in range(grid["x"], grid["x"] + grid["w"]) for y in range(grid["y"], grid["y"] + grid["h"])
        }
        assert occupied.isdisjoint(cells)
        occupied.update(cells)
        for target in panel["targets"]:
            assert target["datasource"] == {
                "type": "prometheus",
                "uid": "Prometheus",
            }
            expression = target["expr"]
            assert 'job="iceberg-rest-synthetic"' in expression
            assert 'target="catalog"' in expression
            assert any(metric in expression for metric in ALLOWED_METRICS)
            assert "http://" not in expression and "https://" not in expression


def test_dashboard_queries_cover_current_slos_outcome_and_alerts() -> None:
    dashboard = json.loads(DASHBOARD_PATH.read_text(encoding="utf-8"))
    expressions = [panel["targets"][0]["expr"] for panel in dashboard["panels"]]
    assert expressions[0].startswith("data_eng_lab_iceberg_rest_synthetic_probe_success{")
    assert expressions[1].startswith("data_eng_lab_iceberg_rest_synthetic_probe_duration_seconds{")
    assert "sum_over_time" in expressions[2] and "count_over_time" in expressions[2]
    assert "up{" in expressions[2] and "[30d]" in expressions[2]
    assert "quantile_over_time(0.95" in expressions[3] and "[30d]" in expressions[3]
    assert "probe_result" in expressions[4] and "== 1" in expressions[4]
    assert expressions[5].startswith("ALERTS{")


def test_dashboard_query_contract_documents_prometheus_scrape_availability() -> None:
    design = (ROOT / "docs/superpowers/specs/2026-08-15-iceberg-rest-observability-design.md").read_text()
    plan = (ROOT / "docs/superpowers/plans/2026-08-15-iceberg-rest-observability.md").read_text()
    assert "Prometheus `up`" in design
    assert "Prometheus `up`" in plan
