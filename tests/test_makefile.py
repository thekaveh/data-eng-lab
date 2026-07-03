import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TARGETS = ["setup", "up", "down", "verify", "test", "preflight", "lint", "fmt"]


def test_all_targets_defined():
    out = subprocess.run(["make", "-npq"], cwd=ROOT, capture_output=True, text=True)
    text = out.stdout
    missing = [t for t in TARGETS if f"\n{t}:" not in text]
    assert not missing, f"Makefile missing targets: {missing}"
