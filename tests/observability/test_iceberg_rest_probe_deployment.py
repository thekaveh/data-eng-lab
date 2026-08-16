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
            "../observability/prometheus/rules/iceberg-rest.yml:/etc/data-eng-lab-prometheus/rules/iceberg-rest.yml:ro",
        },
        "grafana": {
            "./services/grafana/config/provisioning/alerting:/etc/data-eng-lab-grafana/provisioning/alerting:ro",
            "./services/grafana/config/provisioning/datasources:/etc/data-eng-lab-grafana/provisioning/datasources:ro",
            "./services/grafana/config/provisioning/plugins:/etc/data-eng-lab-grafana/provisioning/plugins:ro",
            "./services/grafana/config/provisioning/dashboards/app-tier.json:/etc/data-eng-lab-grafana/atlas-dashboards/app-tier.json:ro",
            "./services/grafana/config/provisioning/dashboards/containers-and-host.json:/etc/data-eng-lab-grafana/atlas-dashboards/containers-and-host.json:ro",
            "./services/grafana/config/provisioning/dashboards/kong.json:/etc/data-eng-lab-grafana/atlas-dashboards/kong.json:ro",
            "./services/grafana/config/provisioning/dashboards/litellm.json:/etc/data-eng-lab-grafana/atlas-dashboards/litellm.json:ro",
            "./services/grafana/config/provisioning/dashboards/n8n.json:/etc/data-eng-lab-grafana/atlas-dashboards/n8n.json:ro",
            "./services/grafana/config/provisioning/dashboards/postgres-redis.json:/etc/data-eng-lab-grafana/atlas-dashboards/postgres-redis.json:ro",
            "./services/grafana/config/provisioning/dashboards/stack-overview.json:/etc/data-eng-lab-grafana/atlas-dashboards/stack-overview.json:ro",
            "../observability/grafana/provisioning:/etc/data-eng-lab-grafana/provisioning/dashboards:ro",
            "../observability/grafana/iceberg-rest.json:/etc/data-eng-lab-grafana/issue-dashboards/iceberg-rest.json:ro",
        },
    }
    for service, mounts in expected_mounts.items():
        assert mounts.issubset(set(services[service]["volumes"]))
    assert services["grafana"]["environment"]["GF_PATHS_PROVISIONING"] == ("/etc/data-eng-lab-grafana/provisioning")


def test_observability_mounts_never_nest_under_atlas_readonly_parents() -> None:
    overlay = yaml.safe_load((ROOT / "compose/data-eng-lab.yml").read_text(encoding="utf-8"))
    base_paths = {
        "prometheus": ROOT / "infra/services/prometheus/compose.yml",
        "grafana": ROOT / "infra/services/grafana/compose.yml",
    }
    for service, base_path in base_paths.items():
        base = yaml.safe_load(base_path.read_text(encoding="utf-8"))
        readonly_parents = {
            volume.split(":")[1] for volume in base["services"][service]["volumes"] if volume.endswith(":ro")
        }
        overlay_targets = {volume.split(":")[1] for volume in overlay["services"][service]["volumes"]}
        for parent in readonly_parents:
            assert all(target == parent or not target.startswith(parent + "/") for target in overlay_targets)


def test_grafana_consumer_provisioning_preserves_atlas_and_adds_issue_dashboard() -> None:
    path = ROOT / "observability/grafana/provisioning/dashboards.yml"
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert document["apiVersion"] == 1
    providers = document["providers"]
    assert [(item["name"], item["folder"], item["options"]["path"]) for item in providers] == [
        ("atlas", "Atlas", "/etc/data-eng-lab-grafana/atlas-dashboards"),
        ("data-eng-lab", "Data Engineering Lab", "/etc/data-eng-lab-grafana/issue-dashboards"),
    ]

    overlay = yaml.safe_load((ROOT / "compose/data-eng-lab.yml").read_text(encoding="utf-8"))
    mounted_names = {
        Path(volume.split(":", 1)[0]).name
        for volume in overlay["services"]["grafana"]["volumes"]
        if "/atlas-dashboards/" in volume
    }
    source_names = {
        path.name for path in (ROOT / "infra/services/grafana/config/provisioning/dashboards").glob("*.json")
    }
    assert mounted_names == source_names
