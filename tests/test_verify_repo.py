import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("verify_repo", ROOT / "scripts" / "verify_repo.py")
verify_repo = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(verify_repo)


def _make_valid_scenario(root: Path, name: str = "batch_ingest-nyc_taxi-spark-iceberg"):
    d = root / "scenarios" / name
    (d / "zeppelin").mkdir(parents=True)
    (d / "jupyter").mkdir(parents=True)
    sections = ["1. Scenario summary", "2. Why this exists", "3. What's in the notebooks",
                "4. How to run", "5. Data & dependencies", "6. Known issues & caveats"]
    (d / "README.md").write_text("# t\n\n" + "\n".join(f"## {s}\n\ntext\n" for s in sections))
    (d / "zeppelin" / "notebook.zpln").write_text('{"paragraphs": []}')
    (d / "jupyter" / "notebook.ipynb").write_text('{"cells": [], "nbformat": 4, "nbformat_minor": 5}')
    return d


CFG = {
    "scenario_name_regex": r"^[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+$",
    "scenario_required_files": ["README.md", "zeppelin/notebook.zpln", "jupyter/notebook.ipynb"],
    "scenario_readme_sections": ["1. Scenario summary", "2. Why this exists", "3. What's in the notebooks",
                                 "4. How to run", "5. Data & dependencies", "6. Known issues & caveats"],
}


def test_valid_scenario_passes(tmp_path: Path):
    _make_valid_scenario(tmp_path)
    errors = [f for f in verify_repo.run_checks(tmp_path, CFG) if f.severity == "error"]
    assert errors == [], errors


def test_bad_scenario_name_flags_error(tmp_path: Path):
    _make_valid_scenario(tmp_path, "BadName")
    findings = verify_repo.run_checks(tmp_path, CFG)
    assert any(f.check == "scenario.naming" and f.severity == "error" for f in findings), findings


def test_missing_notebook_flags_error(tmp_path: Path):
    d = _make_valid_scenario(tmp_path)
    (d / "jupyter" / "notebook.ipynb").unlink()
    findings = verify_repo.run_checks(tmp_path, CFG)
    assert any(f.check == "scenario.files" and f.severity == "error" for f in findings), findings


def test_missing_readme_section_flags_error(tmp_path: Path):
    d = _make_valid_scenario(tmp_path)
    (d / "README.md").write_text("# t\n\n## 1. Scenario summary\n\ntext\n")  # only 1 of 6
    findings = verify_repo.run_checks(tmp_path, CFG)
    assert any(f.check == "scenario.readme" and f.severity == "error" for f in findings), findings


def test_invalid_notebook_json_flags_error(tmp_path: Path):
    d = _make_valid_scenario(tmp_path)
    (d / "zeppelin" / "notebook.zpln").write_text("{not json")
    findings = verify_repo.run_checks(tmp_path, CFG)
    assert any(f.check == "scenario.notebook_json" and f.severity == "error" for f in findings), findings


def test_no_scenarios_dir_is_ok(tmp_path: Path):
    errors = [f for f in verify_repo.run_checks(tmp_path, CFG) if f.severity == "error"]
    assert errors == []


def test_registry_check_flags_invalid_registry(tmp_path: Path):
    # a repo root with a broken registry produces a dataset.registry error.
    # (schema.py is loaded from THIS repo relative to verify_repo.py, so no monkeypatch needed.)
    (tmp_path / "datasets").mkdir()
    (tmp_path / "datasets" / "registry.yaml").write_text("version: 2\ndatasets: {}\n")
    cfg = {"scenario_name_regex": r"^x$"}
    findings = verify_repo.run_checks(tmp_path, cfg)
    assert any(f.check == "dataset.registry" and f.severity == "error" for f in findings), findings
