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


# ---------------------------------------------------------------------------
# Issue #42 — enablement derived from Atlas *_SOURCE (env > infra/.env),
# not the *_ENABLED flags that nothing sets since the consumer-manifest migration.
# ---------------------------------------------------------------------------


def test_source_enabled_reads_infra_env_container(tmp_path, monkeypatch):
    """A container source in the env file enables the service."""
    monkeypatch.delenv("TRINO_SOURCE", raising=False)
    env = tmp_path / ".env"
    env.write_text("TRINO_SOURCE=container\n")
    assert manifest._source_enabled("TRINO_SOURCE", env) is True


def test_source_enabled_reads_infra_env_disabled(tmp_path, monkeypatch):
    """An explicit 'disabled' source in the env file disables the service."""
    monkeypatch.delenv("TRINO_SOURCE", raising=False)
    env = tmp_path / ".env"
    env.write_text("TRINO_SOURCE=disabled\n")
    assert manifest._source_enabled("TRINO_SOURCE", env) is False


def test_source_enabled_env_var_overrides_file(tmp_path, monkeypatch):
    """The environment variable takes precedence over infra/.env (test override)."""
    env = tmp_path / ".env"
    env.write_text("TRINO_SOURCE=container\n")
    monkeypatch.setenv("TRINO_SOURCE", "disabled")
    assert manifest._source_enabled("TRINO_SOURCE", env) is False


def test_source_enabled_absent_defaults_enabled(tmp_path, monkeypatch):
    """Absent source (no env, no file) is treated as enabled — a live service is
    never silently skipped; a genuinely-down one surfaces as fail/blocked."""
    monkeypatch.delenv("TRINO_SOURCE", raising=False)
    env = tmp_path / "missing.env"  # does not exist
    assert manifest._source_enabled("TRINO_SOURCE", env) is True


def test_source_enabled_reads_last_value(tmp_path, monkeypatch):
    """Last KEY= line wins (matches the shell envval / catalog._envval convention)."""
    monkeypatch.delenv("TRINO_SOURCE", raising=False)
    env = tmp_path / ".env"
    env.write_text("TRINO_SOURCE=disabled\nTRINO_SOURCE=container\n")
    assert manifest._source_enabled("TRINO_SOURCE", env) is True


def _spec(services, name):
    return next(s for s in services if s.name == name)


def test_gated_services_enabled_when_source_container(monkeypatch):
    """trino/redpanda/jenkins/iceberg-rest are enabled when their *_SOURCE=container."""
    for key in ("TRINO_SOURCE", "REDPANDA_SOURCE", "JENKINS_SOURCE", "ICEBERG_REST_SOURCE"):
        monkeypatch.setenv(key, "container")
    fresh = _load("manifest")
    for name in ("trino", "redpanda", "jenkins", "iceberg-rest"):
        assert _spec(fresh.EXPECTED_SERVICES, name).enabled is True, name


def test_gated_service_disabled_when_source_disabled(monkeypatch):
    """A service is skipped only when its *_SOURCE is genuinely 'disabled'."""
    monkeypatch.setenv("TRINO_SOURCE", "disabled")
    fresh = _load("manifest")
    assert _spec(fresh.EXPECTED_SERVICES, "trino").enabled is False


def test_gated_service_env_override(monkeypatch):
    """Env var overrides whatever infra/.env holds (redpanda forced off)."""
    monkeypatch.setenv("REDPANDA_SOURCE", "disabled")
    fresh = _load("manifest")
    assert _spec(fresh.EXPECTED_SERVICES, "redpanda").enabled is False
