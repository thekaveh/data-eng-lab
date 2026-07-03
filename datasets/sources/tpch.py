"""Generate TPC-H data locally via DuckDB's tpch extension and export to Parquet."""
from __future__ import annotations

from pathlib import Path

import duckdb

TPCH_TABLES = [
    "customer", "lineitem", "nation", "orders",
    "part", "partsupp", "region", "supplier",
]


def generate_tpch(sf: float, dest: Path) -> list[Path]:
    dest.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute("INSTALL tpch; LOAD tpch;")
        con.execute(f"CALL dbgen(sf={sf})")
        out: list[Path] = []
        for table in TPCH_TABLES:
            target = dest / f"{table}.parquet"
            con.execute(f"COPY {table} TO '{target.as_posix()}' (FORMAT PARQUET)")
            out.append(target)
        return out
    finally:
        con.close()
