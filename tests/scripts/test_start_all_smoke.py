import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
START = ROOT / "scripts" / "start-all.sh"
STOP = ROOT / "scripts" / "stop-all.sh"


def test_start_all_dry_run_lists_plan():
    out = subprocess.run(["bash", str(START), "--dry-run"], cwd=ROOT,
                         capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    text = out.stdout + out.stderr
    required = [
        "_user/data-eng-lab", "env backfill", "compose validate", "doctor",
        "--consumer", "--track data-eng", "--detach", "endpoints export",
        "atlas-consumer.env", "ATLAS_MINIO_HOST_ENDPOINT",
        "register_iceberg", "preflight", "layer2",
    ]
    for token in required:
        assert token in text, f"dry-run plan missing '{token}':\n{text}"


def test_start_all_asserts_only_supported_endpoint_contract():
    text = START.read_text(encoding="utf-8")
    assert "ATLAS_MINIO_HOST_ENDPOINT" in text
    for unsupported in (
        "ATLAS_ICEBERG_REST_HOST_ENDPOINT",
        "ATLAS_TRINO_HOST_ENDPOINT",
        "ATLAS_REDPANDA_HOST_ENDPOINT",
        "ATLAS_ZEPPELIN_HOST_ENDPOINT",
        "ATLAS_AIRFLOW_HOST_ENDPOINT",
    ):
        assert unsupported not in text


def test_start_all_uses_argument_safe_atlas_runner():
    script = START.read_text(encoding="utf-8")
    assert "eval" not in script
    atlas_steps = [
        line.strip() for line in script.splitlines()
        if line.strip().startswith("run_atlas ")
    ]
    assert atlas_steps
    assert atlas_steps[0] == 'run_atlas --consumer "$MANIFEST" env backfill'
    assert all('--consumer "$MANIFEST"' in step for step in atlas_steps)
    assert 'cd "$INFRA_DIR"' in script
    assert './start.sh "$@"' in script


def test_start_all_dry_run_quotes_an_untrusted_infra_path(tmp_path):
    marker = tmp_path / "should-not-exist"
    unsafe_path = f"{tmp_path}/infra; touch {marker}"
    out = subprocess.run(
        ["bash", str(START), "--dry-run"], cwd=ROOT, capture_output=True, text=True,
        env={**os.environ, "INFRA_DIR": unsafe_path},
    )
    assert out.returncode == 0, out.stderr
    assert not marker.exists()
    assert "env backfill" in out.stdout


def test_stop_all_dry_run():
    out = subprocess.run(["bash", str(STOP), "--dry-run"], cwd=ROOT,
                         capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert "stop.sh" in (out.stdout + out.stderr)
