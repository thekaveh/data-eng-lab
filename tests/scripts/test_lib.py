import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LIB = ROOT / "scripts" / "lib.sh"


def _run(script: str, cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(["bash", "-c", f'set -e; source "{LIB}"; {script}'],
                          cwd=cwd, capture_output=True, text=True)


def test_envval_reads_last_value(tmp_path: Path):
    env = tmp_path / ".env"
    env.write_text("FOO=one\nBAR=two\nFOO=three\n")
    out = _run(f'envval FOO "{env}"', tmp_path)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "three"


def test_set_env_is_idempotent(tmp_path: Path):
    env = tmp_path / ".env"
    env.write_text("PROJECT_NAME=old\n")
    _run(f'set_env PROJECT_NAME data-eng-lab "{env}"', tmp_path)
    _run(f'set_env PROJECT_NAME data-eng-lab "{env}"', tmp_path)
    lines = [ln for ln in env.read_text().splitlines() if ln.startswith("PROJECT_NAME=")]
    assert lines == ["PROJECT_NAME=data-eng-lab"], env.read_text()


def test_set_env_default_does_not_overwrite(tmp_path: Path):
    env = tmp_path / ".env"
    env.write_text("KEEP=mine\n")
    _run(f'set_env_default KEEP theirs "{env}"', tmp_path)
    assert "KEEP=mine" in env.read_text()
    assert "KEEP=theirs" not in env.read_text()


def test_set_env_default_appends_when_absent(tmp_path: Path):
    env = tmp_path / ".env"
    env.write_text("")
    _run(f'set_env_default NEW_KEY value "{env}"', tmp_path)
    assert "NEW_KEY=value" in env.read_text()


def test_resolve_project_name_prefers_exported(tmp_path: Path):
    env = tmp_path / ".env"
    env.write_text("PROJECT_NAME=from-file\n")
    out = _run(f'export PROJECT_NAME=from-shell; resolve_project_name "{env}"', tmp_path)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "from-shell"


def test_resolve_project_name_from_env_file(tmp_path: Path):
    env = tmp_path / ".env"
    env.write_text("PROJECT_NAME=custom-proj\n")
    out = _run(f'unset PROJECT_NAME; resolve_project_name "{env}"', tmp_path)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "custom-proj"


def test_resolve_project_name_defaults(tmp_path: Path):
    out = _run('unset PROJECT_NAME; resolve_project_name ""', tmp_path)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "data-eng-lab"


def test_wait_healthy_succeeds_when_probe_reports_ready(tmp_path: Path):
    script = 'ready_probe() { echo "Up 3s (healthy)"; }; export HEALTH_PROBE=ready_probe; wait_healthy svc1'
    out = _run(script, tmp_path)
    assert out.returncode == 0, out.stderr


def test_wait_healthy_times_out_when_never_ready(tmp_path: Path):
    script = 'down_probe() { echo ""; }; export HEALTH_PROBE=down_probe; export HEALTH_TIMEOUT=0; wait_healthy svc1'
    out = _run(script, tmp_path)
    assert out.returncode == 1
