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
