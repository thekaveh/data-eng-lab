import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("verify_repo", ROOT / "scripts" / "verify_repo.py")
verify_repo = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(verify_repo)


def test_valid_scenario_name_passes(tmp_path: Path):
    (tmp_path / "scenarios" / "medallion-nyc_taxi-spark-iceberg").mkdir(parents=True)
    cfg = {"active_scenario_dirs": ["medallion-nyc_taxi-spark-iceberg"],
           "scenario_name_regex": r"^[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+$"}
    findings = verify_repo.run_checks(tmp_path, cfg)
    errors = [f for f in findings if f.severity == "error"]
    assert errors == [], errors


def test_bad_scenario_name_flags_error(tmp_path: Path):
    (tmp_path / "scenarios" / "BadName").mkdir(parents=True)
    cfg = {"active_scenario_dirs": ["BadName"],
           "scenario_name_regex": r"^[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+$"}
    findings = verify_repo.run_checks(tmp_path, cfg)
    assert any(f.severity == "error" and "naming" in f.check for f in findings), findings


def test_missing_declared_dir_flags_error(tmp_path: Path):
    cfg = {"active_scenario_dirs": ["ghost-scenario-spark-iceberg"],
           "scenario_name_regex": r"^[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+$"}
    findings = verify_repo.run_checks(tmp_path, cfg)
    assert any(f.severity == "error" and "exists" in f.check for f in findings), findings


def test_registry_check_flags_invalid_registry(tmp_path: Path):
    # a repo root with a broken registry produces a dataset.registry error.
    # (schema.py is loaded from THIS repo relative to verify_repo.py, so no monkeypatch needed.)
    (tmp_path / "datasets").mkdir()
    (tmp_path / "datasets" / "registry.yaml").write_text("version: 2\ndatasets: {}\n")
    cfg = {"active_scenario_dirs": [], "scenario_name_regex": r"^x$"}
    findings = verify_repo.run_checks(tmp_path, cfg)
    assert any(f.check == "dataset.registry" and f.severity == "error" for f in findings), findings
