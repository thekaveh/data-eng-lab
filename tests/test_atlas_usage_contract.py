from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _artifacts() -> list[Path]:
    return (
        sorted((ROOT / "scenarios").rglob("dag.py"))
        + sorted((ROOT / "spark-apps").rglob("dag.py"))
        + sorted((ROOT / "scenarios").rglob("notebook.zpln"))
        + sorted((ROOT / "scenarios").rglob("notebook.ipynb"))
    )


def test_catalog_has_expected_atlas_artifacts():
    assert len(sorted((ROOT / "scenarios").rglob("dag.py"))) == 19
    assert len(sorted((ROOT / "spark-apps").rglob("dag.py"))) == 2
    assert len(sorted((ROOT / "scenarios").rglob("notebook.zpln"))) == 19
    assert len(sorted((ROOT / "scenarios").rglob("notebook.ipynb"))) == 19


def test_executable_artifacts_do_not_hardcode_host_ports():
    offenders = [
        path.relative_to(ROOT).as_posix()
        for path in _artifacts()
        if "localhost:" in path.read_text(encoding="utf-8")
        or "127.0.0.1:" in path.read_text(encoding="utf-8")
    ]
    assert not offenders, f"host endpoint literals in executable artifacts: {offenders}"
