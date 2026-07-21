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
