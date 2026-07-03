import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("build_notebooks", ROOT / "tests" / "scenarios" / "build_notebooks.py")
bn = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bn)

SCALA = {s: f"// {s}" for s in bn.NB_SECTIONS}
PY = {s: f"# {s}" for s in bn.NB_SECTIONS}


def test_zeppelin_has_scala_sections():
    z = bn.build_zeppelin("x", SCALA)
    text = json.dumps(z)
    for s in bn.NB_SECTIONS:
        assert f"## {s}" in text
    assert "%spark" in text and "%md" in text


def test_written_scenario_passes_verifier(tmp_path: Path):
    bn.write_scenario(tmp_path, "batch_ingest-nyc_taxi-spark-iceberg", SCALA, PY, "# dag\n", "# readme")
    vspec = importlib.util.spec_from_file_location("verify_repo", ROOT / "scripts" / "verify_repo.py")
    verify = importlib.util.module_from_spec(vspec)
    vspec.loader.exec_module(verify)
    import yaml
    cfg = yaml.safe_load((ROOT / "scripts" / "verify_repo_config.yaml").read_text())
    # supply a valid README so the readme-section check passes
    readme = "# t\n\n" + "\n".join(f"## {s}\n\ntext\n" for s in [
        "1. Scenario summary", "2. Why this exists", "3. What's in the notebooks",
        "4. How to run", "5. Data & dependencies", "6. Known issues & caveats"])
    (tmp_path / "scenarios" / "batch_ingest-nyc_taxi-spark-iceberg" / "README.md").write_text(readme)
    errors = [f for f in verify.run_checks(tmp_path, cfg) if f.severity == "error"]
    assert errors == [], errors
