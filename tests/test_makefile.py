import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TARGETS = [
    "setup", "up", "down", "datasets", "verify", "test",
    "preflight", "lint", "fmt", "new-scenario", "build-apps",
    "docs-build", "docs-check", "docs-serve", "docs-wiki",
    "notebooks-reproducibility",
]


def test_all_targets_defined():
    out = subprocess.run(["make", "-npq"], cwd=ROOT, capture_output=True, text=True)
    text = out.stdout
    missing = [t for t in TARGETS if f"\n{t}:" not in text]
    assert not missing, f"Makefile missing targets: {missing}"


def test_datasets_target_runs_downloader():
    text = subprocess.run(["make", "-npq"], cwd=ROOT, capture_output=True, text=True).stdout
    assert "download_datasets.py" in text, "make datasets should call the downloader"


def test_new_scenario_target_runs_scaffolder():
    text = subprocess.run(["make", "-npq"], cwd=ROOT, capture_output=True, text=True).stdout
    assert "new_scenario.py" in text


def test_build_apps_target_runs_maven():
    text = subprocess.run(["make", "-npq"], cwd=ROOT, capture_output=True, text=True).stdout
    assert "mvn" in text and "spark-apps" in text


def test_docs_targets_use_supported_module_entrypoints():
    text = subprocess.run(["make", "-npq"], cwd=ROOT, capture_output=True, text=True).stdout
    assert "python -m scripts.docs.render_diagrams --root ." in text
    assert "python -m scripts.docs.build_docs --site --root ." in text
    assert "python -m scripts.docs.check_docs --root ." in text
    assert "python -m scripts.docs.push_wiki --check --root ." in text


def test_notebook_reproducibility_target_runs_only_the_exhaustive_live_suite():
    text = subprocess.run(["make", "-npq"], cwd=ROOT, capture_output=True, text=True).stdout
    assert "RUN_INFRA=1" in text
    assert "tests/scenarios/test_notebook_reproducibility_live.py" in text
    assert "--group live" in text
