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


def test_infra_cd_steps_are_subshelled():
    """The `cd $INFRA_DIR` steps run in subshells so the working directory does
    not leak across run() phases (issue #45 item 6)."""
    script = START.read_text(encoding="utf-8")
    # Every step that cd's into infra must open a subshell: `run "(cd ...`
    for line in script.splitlines():
        if "cd \\\"$INFRA_DIR\\\"" in line and line.strip().startswith("run "):
            assert "(cd" in line, f"cd-into-infra step not subshelled: {line.strip()}"


def test_stop_all_dry_run():
    out = subprocess.run(["bash", str(STOP), "--dry-run"], cwd=ROOT,
                         capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert "stop.sh" in (out.stdout + out.stderr)
