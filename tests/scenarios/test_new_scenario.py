import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("new_scenario", ROOT / "scripts" / "new_scenario.py")
ns = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ns)

NAME = "batch_ingest-nyc_taxi-spark-iceberg"


def test_scaffold_creates_valid_structure(tmp_path: Path):
    d = ns.scaffold(tmp_path, NAME, with_dag=True)
    assert (d / "README.md").exists()
    assert (d / "zeppelin" / "notebook.zpln").exists()
    assert (d / "jupyter" / "notebook.ipynb").exists()
    assert (d / "dag.py").exists()
    # both notebooks are valid JSON
    json.loads((d / "zeppelin" / "notebook.zpln").read_text())
    json.loads((d / "jupyter" / "notebook.ipynb").read_text())


def test_scaffold_output_passes_the_verifier(tmp_path: Path):
    ns.scaffold(tmp_path, NAME)
    vspec = importlib.util.spec_from_file_location("verify_repo", ROOT / "scripts" / "verify_repo.py")
    verify = importlib.util.module_from_spec(vspec)
    vspec.loader.exec_module(verify)
    import yaml
    cfg = yaml.safe_load((ROOT / "scripts" / "verify_repo_config.yaml").read_text())
    errors = [f for f in verify.run_checks(tmp_path, cfg) if f.severity == "error"]
    assert errors == [], errors


def test_scaffold_rejects_bad_name(tmp_path: Path):
    with pytest.raises(ValueError):
        ns.scaffold(tmp_path, "BadName")


def test_scaffold_refuses_overwrite(tmp_path: Path):
    ns.scaffold(tmp_path, NAME)
    with pytest.raises(ValueError):
        ns.scaffold(tmp_path, NAME)


def test_no_dag_flag(tmp_path: Path):
    d = ns.scaffold(tmp_path, NAME, with_dag=False)
    assert not (d / "dag.py").exists()
