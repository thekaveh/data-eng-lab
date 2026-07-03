import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("dataset_tpch", ROOT / "datasets" / "sources" / "tpch.py")
tpch = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tpch)


def test_generate_tiny_tpch(tmp_path: Path):
    files = tpch.generate_tpch(0.01, tmp_path)
    names = {f.stem for f in files}
    # TPC-H has 8 tables
    assert {"lineitem", "orders", "customer", "nation", "region", "part", "supplier", "partsupp"} <= names
    assert all(f.suffix == ".parquet" and f.stat().st_size > 0 for f in files)
