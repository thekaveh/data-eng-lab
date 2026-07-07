import shutil
import subprocess
from pathlib import Path

FIX = Path(__file__).resolve().parent / "_fixtures" / "repo"


def _run(repo: Path, *extra: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["uv", "run", "--group", "dev", "python", "scripts/build_docs.py",
         "--root", str(repo), "--wiki-dir", str(repo / "wiki"), *extra],
        cwd=".", capture_output=True, text=True,
    )


def test_build_all_writes_readme_and_wiki(tmp_path):
    repo = tmp_path / "repo"
    shutil.copytree(FIX, repo)
    (repo / "docs" / "architectures").mkdir(parents=True, exist_ok=True)
    (repo / "docs" / "architectures" / "batch_ingest-nyc_taxi-spark-iceberg.svg").write_text("<svg/>")
    wd = repo / "wiki"
    r = _run(repo)
    assert r.returncode == 0, r.stderr
    assert (repo / "README.md").exists()
    assert (repo / "scenarios" / "batch_ingest-nyc_taxi-spark-iceberg" / "README.md").exists()
    assert (repo / "scenarios" / "batch_ingest-nyc_taxi-spark-iceberg" / "notebooks.md").exists()
    assert (wd / "Home.md").exists()
    # asset copied into scenario dir
    assert (repo / "scenarios" / "batch_ingest-nyc_taxi-spark-iceberg" /
            "architectures" / "batch_ingest-nyc_taxi-spark-iceberg.svg").exists()
    # asset copied into wiki
    assert (wd / "batch_ingest-nyc_taxi-spark-iceberg.svg").exists()


def test_check_mode_clean_after_build(tmp_path):
    repo = tmp_path / "repo"
    shutil.copytree(FIX, repo)
    (repo / "docs" / "architectures").mkdir(parents=True, exist_ok=True)
    (repo / "docs" / "architectures" / "batch_ingest-nyc_taxi-spark-iceberg.svg").write_text("<svg/>")
    assert _run(repo).returncode == 0
    chk = _run(repo, "--check")
    assert chk.returncode == 0, chk.stdout + chk.stderr


def test_concept_diagrams_copied_to_both_surfaces(tmp_path):
    """Concept-page diagrams (referenced by docs/*.md) must land in both
    <repo>/architectures/ (root README surface) and wiki/ (wiki surface)."""
    repo = tmp_path / "repo"
    shutil.copytree(FIX, repo)
    arch = repo / "docs" / "architectures"
    arch.mkdir(parents=True, exist_ok=True)
    (arch / "overview.svg").write_text("<svg/>")
    (arch / "batch_ingest-nyc_taxi-spark-iceberg.svg").write_text("<svg/>")
    # add a concept-page diagram ref to docs/index.md (the root-README source)
    idx = repo / "docs" / "index.md"
    idx.write_text(idx.read_text(encoding="utf-8")
                   + "\n![Full-stack Lakehouse](architectures/overview.svg)\n")
    wd = repo / "wiki"
    r = _run(repo)
    assert r.returncode == 0, r.stderr
    # in-repo README surface
    assert (repo / "architectures" / "overview.svg").exists()
    # wiki surface
    assert (wd / "overview.svg").exists()
