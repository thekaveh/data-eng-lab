from __future__ import annotations

import copy
from pathlib import Path

import pytest
import yaml

from scripts.observability.prometheus_config import (
    BASE_CONFIG_SHA256,
    ConfigFailure,
    render_prometheus_config,
)

ROOT = Path(__file__).resolve().parents[2]
BASE_PATH = ROOT / "infra/services/prometheus/config/prometheus.yml"
OUTPUT_PATH = ROOT / "observability/prometheus/prometheus.yml"
RULES_PATH = ROOT / "observability/prometheus/rules/iceberg-rest.yml"


def _load(path: Path) -> dict[str, object]:
    value = yaml.safe_load(path.read_bytes())
    assert isinstance(value, dict)
    return value


def test_generated_prometheus_config_preserves_atlas_and_adds_one_final_job() -> None:
    base = _load(BASE_PATH)
    rendered = render_prometheus_config(base)
    assert OUTPUT_PATH.read_bytes() == rendered
    actual = yaml.safe_load(rendered)
    expected = copy.deepcopy(base)
    expected["rule_files"].append("/etc/data-eng-lab-prometheus/rules/*.yml")
    jobs = expected["scrape_configs"]
    assert isinstance(jobs, list)
    jobs.append(
        {
            "job_name": "iceberg-rest-synthetic",
            "scrape_interval": "30s",
            "scrape_timeout": "5s",
            "metrics_path": "/metrics",
            "honor_labels": True,
            "static_configs": [{"targets": ["iceberg-rest-probe:8080"], "labels": {"target": "catalog"}}],
        }
    )
    assert actual == expected


def test_generator_rejects_duplicate_jobs_and_changed_atlas_source() -> None:
    base = _load(BASE_PATH)
    jobs = base["scrape_configs"]
    assert isinstance(jobs, list)
    jobs.append(copy.deepcopy(jobs[0]))
    with pytest.raises(ConfigFailure, match="^prometheus_config_invalid$"):
        render_prometheus_config(base)
    assert BASE_CONFIG_SHA256 == "038325adcb2e12658e740416216968aa3509633a6ff907894bfddfdbd4c4325e"


def test_alert_rules_are_closed_and_operator_actionable() -> None:
    document = _load(RULES_PATH)
    assert set(document) == {"groups"}
    groups = document["groups"]
    assert isinstance(groups, list) and len(groups) == 1
    group = groups[0]
    assert group["name"] == "data-eng-lab-iceberg-rest-synthetic"
    rules = group["rules"]
    assert [rule["alert"] for rule in rules] == [
        "IcebergRestSyntheticExporterMissing",
        "IcebergRestSyntheticUnavailable",
        "IcebergRestSyntheticSlow",
    ]
    assert [rule["for"] for rule in rules] == ["2m", "2m", "10m"]
    assert [rule["labels"]["severity"] for rule in rules] == [
        "critical",
        "critical",
        "warning",
    ]
    for rule in rules:
        expression = rule["expr"]
        assert 'job="iceberg-rest-synthetic"' in expression
        assert 'target="catalog"' in expression
        assert "http://" not in str(rule) and "https://" not in str(rule)
        assert rule["annotations"]["runbook"] == "docs/iceberg-rest-observability.md"
        assert "{{" not in str(rule["annotations"])
    assert "absent(up{" in rules[0]["expr"]
    assert 'up{job="iceberg-rest-synthetic",target="catalog"} == 0' in rules[0]["expr"]
    assert "probe_success" in rules[1]["expr"]
    assert "max_over_time" not in rules[1]["expr"]
    assert "probe_duration_seconds" in rules[2]["expr"]
