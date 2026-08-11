"""Generate byte-locked TPC-H Parquet reference artifacts."""

from __future__ import annotations

import argparse
import os
import tempfile
from collections.abc import Sequence
from pathlib import Path

import duckdb
import yaml

from datasets.locking import file_metadata

DUCKDB_VERSION = "1.5.4"
DUCKDB_WHEEL_SHA256 = "ccc7f2694d02b4763fee61021d45e12f7bc5743993686563957df0cef799fbae"
BASE_IMAGE_DIGEST = "sha256:cec9aa7aa96eea4fa036e9b82be1e6b325f2e3707f462d885868df51ec0a4b47"
UV_LOCK_SHA256 = "a376ce1b5bd5621290aaded68c22572690395419876da41814e28469bb4186b1"
TPCH_EXTENSION_SHA256 = "a6516e487106b4f95bd6d85da4364debdcb2db536d015784bc43209af6ed0125"
UV_LOCK_PATH = Path("/workspace/uv.lock")
TPCH_EXTENSION_PATH = Path("/root/.duckdb/extensions/v1.5.4/linux_amd64/tpch.duckdb_extension")

TABLE_ORDER_BY = {
    "customer": "c_custkey",
    "lineitem": "l_orderkey, l_linenumber",
    "nation": "n_nationkey",
    "orders": "o_orderkey",
    "part": "p_partkey",
    "partsupp": "ps_partkey, ps_suppkey",
    "region": "r_regionkey",
    "supplier": "s_suppkey",
}
_SCALES = {
    "0.01": (0.01, "CALL dbgen(sf=0.01)"),
    "1": (1.0, "CALL dbgen(sf=1)"),
    "10": (10.0, "CALL dbgen(sf=10)"),
}


def copy_query(table: str, target: Path) -> str:
    """Return the deterministic COPY query for a known TPC-H table."""
    try:
        order_by = TABLE_ORDER_BY[table]
    except KeyError as error:
        raise ValueError(f"unknown TPC-H table: {table}") from error
    quoted_target = str(target).replace("'", "''")
    return (
        f"COPY (SELECT * FROM {table} ORDER BY {order_by}) "
        f"TO '{quoted_target}' "
        "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)"
    )


def session_statements() -> tuple[str, str]:
    """Return the session settings that make the export deterministic."""
    return ("SET threads=1", "SET preserve_insertion_order=true")


def environment_metadata() -> dict[str, object]:
    """Return the complete immutable canonical-environment identity."""
    return {
        "platform": "linux/amd64",
        "base_image_digest": BASE_IMAGE_DIGEST,
        "duckdb_version": DUCKDB_VERSION,
        "duckdb_wheel_sha256": DUCKDB_WHEEL_SHA256,
        "uv_lock_sha256": UV_LOCK_SHA256,
        "tpch_extension_sha256": TPCH_EXTENSION_SHA256,
        "locale": "C.UTF-8",
        "timezone": "UTC",
        "threads": 1,
        "preserve_insertion_order": True,
        "format": "parquet",
        "compression": "zstd",
        "row_group_size": 100000,
    }


def _verify_sha256(path: Path, expected: str, label: str) -> None:
    _, actual = file_metadata(path)
    if actual != expected:
        raise ValueError(f"{label} SHA-256 mismatch: expected {expected}, got {actual}")


def verify_runtime_inputs(uv_lock: Path, extension: Path) -> None:
    """Fail unless both offline runtime inputs have their reviewed digests."""
    _verify_sha256(uv_lock, UV_LOCK_SHA256, "uv.lock")
    _verify_sha256(extension, TPCH_EXTENSION_SHA256, "TPC-H extension")


def _verify_runtime_environment() -> None:
    if duckdb.__version__ != DUCKDB_VERSION:
        raise ValueError(f"DuckDB version mismatch: expected {DUCKDB_VERSION}, got {duckdb.__version__}")
    if os.environ.get("LANG") != "C.UTF-8" or os.environ.get("LC_ALL") != "C.UTF-8":
        raise ValueError("locale must be C.UTF-8 through LANG and LC_ALL")
    if os.environ.get("TZ") != "UTC":
        raise ValueError("timezone must be UTC")


def _path_lexists(path: Path) -> bool:
    return os.path.lexists(path)


def _validate_metadata_destination(output_dir: Path, metadata_path: Path) -> None:
    if _path_lexists(metadata_path):
        raise ValueError(f"metadata path must not exist: {metadata_path}")
    resolved_metadata = metadata_path.resolve(strict=False)
    resolved_output_dir = output_dir.resolve(strict=False)
    if resolved_metadata == resolved_output_dir or resolved_metadata in resolved_output_dir.parents:
        raise ValueError(f"metadata path collides with output directory: {output_dir}")
    for table in TABLE_ORDER_BY:
        target = output_dir / f"{table}.parquet"
        if resolved_metadata == target.resolve(strict=False):
            raise ValueError(f"metadata path collides with TPC-H output: {target}")


def _prepare_destinations(output_dir: Path, metadata_path: Path) -> None:
    if _path_lexists(output_dir):
        raise ValueError(f"output directory must not exist: {output_dir}")
    _validate_metadata_destination(output_dir, metadata_path)
    output_dir.mkdir(parents=True, exist_ok=False)


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_write_metadata(path: Path, document: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = yaml.safe_dump(document, sort_keys=False, allow_unicode=True)
    descriptor, raw_temp_path = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    temp_path = Path(raw_temp_path)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(serialized)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_path, path)
        _fsync_directory(path.parent)
    finally:
        temp_path.unlink(missing_ok=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scale", required=True, choices=tuple(_SCALES))
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--metadata", required=True, type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Generate one canonical TPC-H scale and its lock-candidate metadata."""
    args = _parser().parse_args(argv)
    scale_factor, dbgen_statement = _SCALES[args.scale]
    output_dir: Path = args.output_dir
    metadata_path: Path = args.metadata

    _verify_runtime_environment()
    verify_runtime_inputs(UV_LOCK_PATH, TPCH_EXTENSION_PATH)
    _prepare_destinations(output_dir, metadata_path)

    connection = duckdb.connect()
    try:
        for statement in session_statements():
            connection.execute(statement)
        connection.execute("LOAD tpch")
        connection.execute(dbgen_statement)

        outputs: dict[str, dict[str, object]] = {}
        for table in TABLE_ORDER_BY:
            target = output_dir / f"{table}.parquet"
            connection.execute(copy_query(table, target))
            size_bytes, sha256 = file_metadata(target)
            outputs[table] = {
                "object_name": target.name,
                "size_bytes": size_bytes,
                "sha256": sha256,
            }
    finally:
        connection.close()

    _validate_metadata_destination(output_dir, metadata_path)
    _atomic_write_metadata(
        metadata_path,
        {
            "scale_factor": scale_factor,
            "environment": environment_metadata(),
            "outputs": outputs,
        },
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
