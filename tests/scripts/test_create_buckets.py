import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "create_buckets.sh"
# Atlas's minio-init creates landing/lakehouse/jars/checkpoints; we only provision lakehouse-test.
EXPECTED = ["lakehouse-test"]


def test_creates_all_expected_buckets(tmp_path: Path):
    infra = tmp_path / "infra"
    infra.mkdir()
    (infra / ".env").write_text("MINIO_ROOT_USER=minioadmin\nMINIO_ROOT_PASSWORD=secret\n")

    # Stub 'mc' that records each invocation's args to a log file.
    rec = tmp_path / "mc_calls.log"
    stub = tmp_path / "mc_stub.sh"
    stub.write_text(f'#!/usr/bin/env bash\necho "$@" >> "{rec}"\n')
    stub.chmod(0o755)

    env = {**os.environ, "INFRA_DIR": str(infra), "MC_CMD": str(stub), "PROJECT_NAME": "data-eng-lab"}
    out = subprocess.run(["bash", str(SCRIPT)], cwd=ROOT, env=env, capture_output=True, text=True)
    assert out.returncode == 0, out.stderr

    lines = rec.read_text().splitlines()
    for bucket in EXPECTED:
        assert any(f"local/{bucket}" in ln and "--ignore-existing" in ln for ln in lines), \
            f"bucket '{bucket}' not created with --ignore-existing; calls:\n{rec.read_text()}"
