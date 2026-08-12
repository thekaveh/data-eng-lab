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


def test_scaffolded_notebooks_resolve_one_expected_scale_immutable_generation(tmp_path: Path):
    d = ns.scaffold(tmp_path, NAME)
    for path in (d / "zeppelin/notebook.zpln", d / "jupyter/notebook.ipynb"):
        text = path.read_text(encoding="utf-8")
        assert text.count("/v1/resolve") == 1
        assert "DATASET_RESOLVER_URI" in text
        assert "DATASET_SCALE" in text
        assert "expected_scale" in text
        assert "_generations/" in text
        assert "size_bytes" in text and "sha256" in text and "schema_id" in text
        assert "s3a://landing/" not in text
        assert "dataset_spark_uris" in text or "datasetSparkUris" in text
        assert "readNBytes" in text or "read(_MAX_RESOLUTION_BYTES + 1)" in text
        assert "STRICT_DUPLICATE_DETECTION" in text or "object_pairs_hook" in text
        normalized = text.replace("{{", "{").replace("}}", "}")
        assert r"[0-9a-f]{12}4[0-9a-f]{3}[89ab][0-9a-f]{15}" in normalized
        assert r"[0-9a-f]{32}" not in normalized


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
