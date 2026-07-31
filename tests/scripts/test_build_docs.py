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
    png_dir = repo / "docs" / "diagrams" / "img"
    png_dir.mkdir(parents=True, exist_ok=True)
    (png_dir / "batch_ingest-nyc_taxi-spark-iceberg.png").write_bytes(b"\x89PNG fixture")
    wd = repo / "wiki"
    r = _run(repo)
    assert r.returncode == 0, r.stderr
    assert (repo / "README.md").exists()
    assert (repo / "scenarios" / "batch_ingest-nyc_taxi-spark-iceberg" / "README.md").exists()
    assert (repo / "scenarios" / "batch_ingest-nyc_taxi-spark-iceberg" / "notebooks.md").exists()
    assert (wd / "Home.md").exists()
    assert "../../docs/diagrams/img/batch_ingest-nyc_taxi-spark-iceberg.png" in (
        repo / "scenarios" / "batch_ingest-nyc_taxi-spark-iceberg" / "README.md"
    ).read_text(encoding="utf-8")
    assert (wd / "batch_ingest-nyc_taxi-spark-iceberg.png").read_bytes() == b"\x89PNG fixture"


def test_check_mode_clean_after_build(tmp_path):
    repo = tmp_path / "repo"
    shutil.copytree(FIX, repo)
    png_dir = repo / "docs" / "diagrams" / "img"
    png_dir.mkdir(parents=True, exist_ok=True)
    (png_dir / "batch_ingest-nyc_taxi-spark-iceberg.png").write_bytes(b"\x89PNG fixture")
    assert _run(repo).returncode == 0
    chk = _run(repo, "--check")
    assert chk.returncode == 0, chk.stdout + chk.stderr


def test_concept_diagrams_use_committed_png_and_copy_to_wiki(tmp_path):
    repo = tmp_path / "repo"
    shutil.copytree(FIX, repo)
    png_dir = repo / "docs" / "diagrams" / "img"
    png_dir.mkdir(parents=True, exist_ok=True)
    (png_dir / "overview.png").write_bytes(b"\x89PNG overview")
    (png_dir / "batch_ingest-nyc_taxi-spark-iceberg.png").write_bytes(b"\x89PNG fixture")
    # add a concept-page diagram ref to docs/index.md (the root-README source)
    idx = repo / "docs" / "index.md"
    idx.write_text(idx.read_text(encoding="utf-8")
                   + "\n![Full-stack Lakehouse](diagrams/img/overview.png)\n")
    wd = repo / "wiki"
    r = _run(repo)
    assert r.returncode == 0, r.stderr
    assert "(docs/diagrams/img/overview.png)" in (repo / "README.md").read_text(encoding="utf-8")
    assert (wd / "overview.png").read_bytes() == b"\x89PNG overview"
