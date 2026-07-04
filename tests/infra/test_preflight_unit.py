import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _load(mod):
    spec = importlib.util.spec_from_file_location(mod, ROOT / "tests" / "infra" / f"{mod}.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


preflight = _load("preflight")
manifest = _load("manifest")


def test_disabled_service_is_skipped():
    services = [manifest.ServiceSpec("redpanda", enabled=False, init_check=lambda: (True, "ok"))]
    results = preflight.run_layer1(services, docker_ok=True)
    assert results[0].status == "skipped"


def test_docker_down_blocks_all():
    services = [manifest.ServiceSpec("minio", enabled=True, init_check=lambda: (True, "ok"))]
    results = preflight.run_layer1(services, docker_ok=False)
    assert results[0].status == "blocked"


def test_failed_init_check_reports_fail():
    services = [manifest.ServiceSpec("minio", enabled=True,
                                     init_check=lambda: (False, "buckets missing"))]
    results = preflight.run_layer1(services, docker_ok=True)
    assert results[0].status == "fail"
    assert "buckets missing" in results[0].detail


def test_passing_init_check_reports_pass():
    services = [manifest.ServiceSpec("minio", enabled=True, init_check=lambda: (True, "ok"))]
    results = preflight.run_layer1(services, docker_ok=True)
    assert results[0].status == "pass"
    assert results[0].detail == "ok"


def test_render_matrix_contains_status():
    results = [preflight.Result("minio", "pass", "ok")]
    out = preflight.render_matrix(results)
    assert "minio" in out
    assert "PASS" in out
    assert "✅" in out
    assert "1 service ·" in out


def test_docker_error_sentinel_reports_blocked():
    services = [manifest.ServiceSpec("minio", enabled=True, init_check=lambda: (False, "<docker-error>"))]
    results = preflight.run_layer1(services, docker_ok=True)
    assert results[0].status == "blocked"


def test_docker_timeout_sentinel_reports_blocked():
    services = [manifest.ServiceSpec("minio", enabled=True, init_check=lambda: (False, "<docker-timeout>"))]
    results = preflight.run_layer1(services, docker_ok=True)
    assert results[0].status == "blocked"


# ---------------------------------------------------------------------------
# Phase 13 — manifest presence for Trino (A7) + Redpanda (A9)
# ---------------------------------------------------------------------------


def test_trino_in_expected_services():
    """trino ServiceSpec is declared in EXPECTED_SERVICES."""
    names = [s.name for s in manifest.EXPECTED_SERVICES]
    assert "trino" in names


def test_redpanda_in_expected_services():
    """redpanda ServiceSpec is declared in EXPECTED_SERVICES."""
    names = [s.name for s in manifest.EXPECTED_SERVICES]
    assert "redpanda" in names


def test_trino_gated_by_env_flag(monkeypatch):
    """trino ServiceSpec is disabled when TRINO_ENABLED is absent."""
    monkeypatch.delenv("TRINO_ENABLED", raising=False)
    # Reload manifest so _truthy() is re-evaluated.
    fresh = _load("manifest")
    spec = next(s for s in fresh.EXPECTED_SERVICES if s.name == "trino")
    assert spec.enabled is False


def test_redpanda_gated_by_env_flag(monkeypatch):
    """redpanda ServiceSpec is disabled when REDPANDA_ENABLED is absent."""
    monkeypatch.delenv("REDPANDA_ENABLED", raising=False)
    fresh = _load("manifest")
    spec = next(s for s in fresh.EXPECTED_SERVICES if s.name == "redpanda")
    assert spec.enabled is False
