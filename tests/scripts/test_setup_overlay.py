import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "setup-overlay.sh"


def _run(tmp_infra: Path) -> subprocess.CompletedProcess:
    env = {**os.environ, "INFRA_DIR": str(tmp_infra)}
    return subprocess.run(["bash", str(SCRIPT)], cwd=ROOT, env=env,
                          capture_output=True, text=True)


def test_creates_symlink_and_env(tmp_path: Path):
    infra = tmp_path / "infra"
    (infra / "services").mkdir(parents=True)
    (infra / ".env").write_text("BASE_PORT=63000\n")

    out = _run(infra)
    assert out.returncode == 0, out.stderr

    slot = infra / "services" / "_user" / "data-eng-lab" / "compose.yml"
    assert slot.is_symlink(), "overlay symlink not created"
    # Relative, portable link (rag-showcase pattern): 4 levels up to <repo>/compose.
    assert os.readlink(slot) == "../../../../compose/data-eng-lab.yml"

    env_text = (infra / ".env").read_text()
    assert "PROJECT_NAME=data-eng-lab" in env_text
    assert "ICEBERG_REST_URI=http://iceberg-rest:8181" in env_text


def test_is_idempotent(tmp_path: Path):
    infra = tmp_path / "infra"
    (infra / "services").mkdir(parents=True)
    (infra / ".env").write_text("")
    _run(infra)
    _run(infra)
    env_text = (infra / ".env").read_text()
    assert env_text.count("PROJECT_NAME=data-eng-lab") == 1
