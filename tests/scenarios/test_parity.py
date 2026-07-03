import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("parity", ROOT / "tests" / "scenarios" / "parity.py")
parity = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(parity)


def _snap(schema, n, checksum):
    return {"schema": schema, "row_count": n, "checksum": checksum}


def test_identical_snapshots_are_equivalent():
    a = _snap(["id:int", "v:string"], 10, "abc")
    ok, detail = parity.tables_equivalent(a, dict(a))
    assert ok, detail


def test_schema_mismatch_detected():
    ok, detail = parity.tables_equivalent(_snap(["id:int"], 10, "abc"), _snap(["id:bigint"], 10, "abc"))
    assert not ok and "schema" in detail


def test_row_count_mismatch_detected():
    ok, detail = parity.tables_equivalent(_snap(["id:int"], 10, "abc"), _snap(["id:int"], 11, "abc"))
    assert not ok and "row_count" in detail


def test_checksum_mismatch_detected():
    ok, detail = parity.tables_equivalent(_snap(["id:int"], 10, "abc"), _snap(["id:int"], 10, "xyz"))
    assert not ok and "checksum" in detail
