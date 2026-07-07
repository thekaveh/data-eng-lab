import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("verify_repo", ROOT / "scripts" / "verify_repo.py")
verify_repo = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(verify_repo)


NB_SECS = ["1. Overview", "2. Setup", "3. Read", "4. Transform", "5. Write", "6. Verify"]


def _make_valid_scenario(root: Path, name: str = "batch_ingest-nyc_taxi-spark-iceberg"):
    d = root / "scenarios" / name
    (d / "zeppelin").mkdir(parents=True)
    (d / "jupyter").mkdir(parents=True)
    sections = ["1. Purpose", "2. Data Model", "3. Architecture", "4. Notebooks",
                "5. Orchestration", "6. Usage", "7. Dependencies", "8. Known Issues & Caveats"]
    (d / "README.md").write_text("# t\n\n" + "\n".join(f"## {s}\n\ntext\n" for s in sections))
    nb_marker = " ".join(f"## {s}" for s in NB_SECS)
    (d / "zeppelin" / "notebook.zpln").write_text(json.dumps({"paragraphs": [], "_sections": nb_marker}))
    (d / "jupyter" / "notebook.ipynb").write_text(
        json.dumps({"cells": [], "nbformat": 4, "nbformat_minor": 5, "_sections": nb_marker})
    )
    return d


def _make_valid_spark_app(root: Path, name: str = "nyc-taxi-etl"):
    d = root / "spark-apps" / name
    (d / "src" / "main" / "scala").mkdir(parents=True)
    (d / "pom.xml").write_text("<project/>")
    (d / "Jenkinsfile").write_text("pipeline {}")
    (d / "dag.py").write_text("# dag\n")
    return d


CFG = {
    "scenario_name_regex": r"^[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+$",
    "scenario_required_files": ["README.md", "zeppelin/notebook.zpln", "jupyter/notebook.ipynb"],
    "scenario_readme_sections": ["1. Purpose", "2. Data Model", "3. Architecture", "4. Notebooks",
                                 "5. Orchestration", "6. Usage", "7. Dependencies", "8. Known Issues & Caveats"],
    "notebook_sections": ["1. Overview", "2. Setup", "3. Read", "4. Transform", "5. Write", "6. Verify"],
}

SPARK_CFG = dict(CFG, spark_app_required_files=["pom.xml", "src/main/scala", "Jenkinsfile", "dag.py"])


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
    (d / "README.md").write_text("# t\n\n## 1. Purpose\n\ntext\n")  # only 1 of 8
    findings = verify_repo.run_checks(tmp_path, CFG)
    assert any(f.check == "scenario.readme" and f.severity == "error" for f in findings), findings


def test_invalid_notebook_json_flags_error(tmp_path: Path):
    d = _make_valid_scenario(tmp_path)
    (d / "zeppelin" / "notebook.zpln").write_text("{not json")
    findings = verify_repo.run_checks(tmp_path, CFG)
    assert any(f.check == "scenario.notebook_json" and f.severity == "error" for f in findings), findings


def test_missing_notebook_section_flags_error(tmp_path: Path):
    d = _make_valid_scenario(tmp_path)
    (d / "zeppelin" / "notebook.zpln").write_text('{"paragraphs": []}')  # no section markers
    findings = verify_repo.run_checks(tmp_path, CFG)
    assert any(f.check == "scenario.notebook_sections" and f.severity == "error" for f in findings), findings


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


def test_valid_spark_app_passes(tmp_path: Path):
    _make_valid_spark_app(tmp_path)
    errors = [f for f in verify_repo.run_checks(tmp_path, SPARK_CFG) if f.severity == "error"]
    assert errors == [], errors


def test_spark_app_missing_pom_flags_error(tmp_path: Path):
    d = _make_valid_spark_app(tmp_path)
    (d / "pom.xml").unlink()
    findings = verify_repo.run_checks(tmp_path, SPARK_CFG)
    assert any(f.check == "spark_app.files" and f.severity == "error" for f in findings), findings


def test_no_spark_apps_dir_is_ok(tmp_path: Path):
    errors = [f for f in verify_repo.run_checks(tmp_path, SPARK_CFG) if f.severity == "error"]
    assert errors == []
