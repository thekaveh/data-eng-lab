from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_probe_dockerfile_is_minimal_pinned_and_nonroot() -> None:
    text = (ROOT / "observability/iceberg-rest-probe.Dockerfile").read_text()
    assert (
        "FROM --platform=linux/amd64 "
        "python@sha256:cec9aa7aa96eea4fa036e9b82be1e6b325f2e3707f462d885868df51ec0a4b47" in text
    )
    assert "COPY scripts/observability /workspace/scripts/observability" in text
    assert "COPY . " not in text
    assert "find /workspace -type f -name '*.pyc' -delete" in text
    assert "USER 65532:65532" in text
    assert 'ENTRYPOINT ["python", "-m", "scripts.observability.iceberg_rest_probe"]' in text


def test_consumer_manifest_enables_observability_with_30_day_retention() -> None:
    manifest = yaml.safe_load((ROOT / "atlas.consumer.yml").read_text(encoding="utf-8"))
    values = manifest["env"]["values"]
    assert values["PROMETHEUS_SOURCE"] == "container"
    assert values["GRAFANA_SOURCE"] == "container"
    assert values["PROMETHEUS_RETENTION_DAYS"] == "30"


def test_probe_service_and_observability_mounts_are_hardened_and_internal() -> None:
    overlay = yaml.safe_load((ROOT / "compose/data-eng-lab.yml").read_text(encoding="utf-8"))
    services = overlay["services"]
    probe = services["iceberg-rest-probe"]
    assert probe["build"] == {
        "context": "..",
        "dockerfile": "observability/iceberg-rest-probe.Dockerfile",
    }
    assert probe["platform"] == "linux/amd64"
    assert probe["user"] == "65532:65532"
    assert probe["read_only"] is True
    assert probe["cap_drop"] == ["ALL"]
    assert probe["security_opt"] == ["no-new-privileges:true"]
    assert probe["tmpfs"] == ["/tmp:size=16m,mode=1777"]
    assert probe["environment"] == {
        "ICEBERG_REST_PROBE_ORIGIN": "http://iceberg-rest:8181",
        "ICEBERG_REST_PROBE_TIMEOUT_SECONDS": "2",
        "ICEBERG_REST_PROBE_MAX_BODY_BYTES": "65536",
    }
    assert probe["networks"] == ["backend-network"]
    assert "ports" not in probe and "expose" not in probe
    assert "iceberg-rest" not in probe.get("depends_on", {})
    assert probe["deploy"]["replicas"] == 1
    assert probe["deploy"]["resources"]["limits"] == {
        "cpus": "0.25",
        "memory": "64M",
        "pids": 32,
    }
    assert probe["healthcheck"]["timeout"] == "3s"

    expected_mounts = {
        "prometheus": {
            "../observability/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro",
            "../observability/prometheus/rules/iceberg-rest.yml:/etc/prometheus/rules/iceberg-rest.yml:ro",
        },
        "grafana": {
            "../observability/grafana/iceberg-rest.json:/etc/grafana/provisioning/dashboards/iceberg-rest.json:ro"
        },
    }
    for service, mounts in expected_mounts.items():
        assert mounts.issubset(set(services[service]["volumes"]))
