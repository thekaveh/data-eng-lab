"""Compare Scala vs PySpark scenario outputs by lightweight table snapshots.

A snapshot is {"schema": list[str], "row_count": int, "checksum": str}. Phase 2b captures
these from the live-executed notebooks; this comparator is the pure, unit-tested core.
"""
from __future__ import annotations


def tables_equivalent(a: dict, b: dict) -> tuple[bool, str]:
    if a.get("schema") != b.get("schema"):
        return False, f"schema differs: {a.get('schema')} != {b.get('schema')}"
    if a.get("row_count") != b.get("row_count"):
        return False, f"row_count differs: {a.get('row_count')} != {b.get('row_count')}"
    if a.get("checksum") != b.get("checksum"):
        return False, f"checksum differs: {a.get('checksum')} != {b.get('checksum')}"
    return True, "equivalent"
