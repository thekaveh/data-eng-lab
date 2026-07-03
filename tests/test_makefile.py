import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TARGETS = ["setup", "up", "down", "datasets", "verify", "test", "preflight", "lint", "fmt"]


def test_all_targets_defined():
    out = subprocess.run(["make", "-npq"], cwd=ROOT, capture_output=True, text=True)
    text = out.stdout
    missing = [t for t in TARGETS if f"\n{t}:" not in text]
    assert not missing, f"Makefile missing targets: {missing}"


def test_datasets_target_runs_downloader():
    text = subprocess.run(["make", "-npq"], cwd=ROOT, capture_output=True, text=True).stdout
    assert "download_datasets.py" in text, "make datasets should call the downloader"
