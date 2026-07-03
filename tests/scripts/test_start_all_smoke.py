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
    for token in ["setup-overlay", "--track data-eng", "create_buckets", "preflight"]:
        assert token in text, f"dry-run plan missing '{token}':\n{text}"


def test_stop_all_dry_run():
    out = subprocess.run(["bash", str(STOP), "--dry-run"], cwd=ROOT,
                         capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert "stop.sh" in (out.stdout + out.stderr)
