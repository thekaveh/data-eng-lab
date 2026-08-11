from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
import yaml

from datasets import tpch_lock_export as exporter

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(
    ("table", "order_by"),
    [
        ("customer", "c_custkey"),
        ("lineitem", "l_orderkey, l_linenumber"),
        ("nation", "n_nationkey"),
        ("orders", "o_orderkey"),
        ("part", "p_partkey"),
        ("partsupp", "ps_partkey, ps_suppkey"),
        ("region", "r_regionkey"),
        ("supplier", "s_suppkey"),
    ],
)
def test_copy_query_is_fully_deterministic(table: str, order_by: str):
    query = exporter.copy_query(table, Path(f"/{table}.parquet"))
    assert f"SELECT * FROM {table} ORDER BY {order_by}" in query
    assert "COMPRESSION ZSTD" in query
    assert "ROW_GROUP_SIZE 100000" in query


def test_copy_query_quotes_target_and_rejects_unknown_table():
    assert "TO '/tmp/it''s.parquet'" in exporter.copy_query("customer", Path("/tmp/it's.parquet"))
    with pytest.raises(ValueError, match="unknown TPC-H table"):
        exporter.copy_query("not_a_table", Path("/tmp/output.parquet"))


def test_metadata_contains_all_locked_environment_inputs(monkeypatch):
    monkeypatch.setattr(exporter, "DUCKDB_VERSION", "1.5.4")
    metadata = exporter.environment_metadata()
    assert metadata == {
        "platform": "linux/amd64",
        "base_image_digest": ("sha256:cec9aa7aa96eea4fa036e9b82be1e6b325f2e3707f462d885868df51ec0a4b47"),
        "duckdb_version": "1.5.4",
        "duckdb_wheel_sha256": ("ccc7f2694d02b4763fee61021d45e12f7bc5743993686563957df0cef799fbae"),
        "uv_lock_sha256": ("a376ce1b5bd5621290aaded68c22572690395419876da41814e28469bb4186b1"),
        "tpch_extension_sha256": ("a6516e487106b4f95bd6d85da4364debdcb2db536d015784bc43209af6ed0125"),
        "locale": "C.UTF-8",
        "timezone": "UTC",
        "threads": 1,
        "preserve_insertion_order": True,
        "format": "parquet",
        "compression": "zstd",
        "row_group_size": 100000,
    }


def test_session_settings_force_single_threaded_ordered_export():
    assert exporter.session_statements() == (
        "SET threads=1",
        "SET preserve_insertion_order=true",
    )


def test_runtime_inputs_fail_closed_on_lock_or_extension_drift(tmp_path: Path):
    uv_lock = tmp_path / "uv.lock"
    extension = tmp_path / "tpch.duckdb_extension"
    uv_lock.write_bytes(b"drifted-lock")
    extension.write_bytes(b"drifted-extension")
    with pytest.raises(ValueError, match="uv.lock SHA-256 mismatch"):
        exporter.verify_runtime_inputs(uv_lock, extension)


def test_runtime_inputs_check_extension_after_valid_lock(tmp_path: Path, monkeypatch):
    uv_lock = tmp_path / "uv.lock"
    extension = tmp_path / "tpch.duckdb_extension"
    uv_lock.write_bytes(b"reviewed lock")
    extension.write_bytes(b"drifted extension")
    monkeypatch.setattr(exporter, "UV_LOCK_SHA256", hashlib.sha256(b"reviewed lock").hexdigest())
    with pytest.raises(ValueError, match="TPC-H extension SHA-256 mismatch"):
        exporter.verify_runtime_inputs(uv_lock, extension)


def test_runtime_inputs_accept_exact_hashes(tmp_path: Path, monkeypatch):
    uv_lock = tmp_path / "uv.lock"
    extension = tmp_path / "tpch.duckdb_extension"
    uv_lock.write_bytes(b"reviewed lock")
    extension.write_bytes(b"reviewed extension")
    monkeypatch.setattr(exporter, "UV_LOCK_SHA256", hashlib.sha256(b"reviewed lock").hexdigest())
    monkeypatch.setattr(
        exporter,
        "TPCH_EXTENSION_SHA256",
        hashlib.sha256(b"reviewed extension").hexdigest(),
    )
    exporter.verify_runtime_inputs(uv_lock, extension)


def test_dockerfile_pins_and_verifies_offline_runtime_inputs():
    text = (ROOT / "datasets" / "tpch-lock.Dockerfile").read_text(encoding="utf-8")
    assert text.startswith(
        "FROM --platform=linux/amd64 python@sha256:cec9aa7aa96eea4fa036e9b82be1e6b325f2e3707f462d885868df51ec0a4b47"
    )
    assert "pip install --no-cache-dir --require-hashes" in text
    assert "a376ce1b5bd5621290aaded68c22572690395419876da41814e28469bb4186b1" in text
    assert "a6516e487106b4f95bd6d85da4364debdcb2db536d015784bc43209af6ed0125" in text
    assert "INSTALL tpch" in text
    assert 'ENTRYPOINT ["python", "-m", "datasets.tpch_lock_export"]' in text


def test_requirements_match_reviewed_linux_wheel_hashes():
    assert (ROOT / "datasets" / "tpch-lock-requirements.txt").read_text(encoding="utf-8") == (
        "duckdb==1.5.4 "
        "--hash=sha256:ccc7f2694d02b4763fee61021d45e12f7bc5743993686563957df0cef799fbae\n"
        "PyYAML==6.0.3 "
        "--hash=sha256:b8bb0864c5a28024fac8a632c443c87c5aa6f215c0b126c449ae1a150412f31d\n"
    )


class _FakeConnection:
    def __init__(self, output_dir: Path, *, fail_table: str | None = None):
        self.output_dir = output_dir
        self.fail_table = fail_table
        self.statements: list[str] = []

    def execute(self, statement: str):
        self.statements.append(statement)
        if self.fail_table and f"FROM {self.fail_table} " in statement:
            raise RuntimeError("simulated COPY failure")
        if statement.startswith("COPY "):
            target = statement.split(" TO '", 1)[1].split("' (FORMAT", 1)[0]
            Path(target.replace("''", "'")).write_bytes(statement.encode())
        return self

    def close(self):
        self.statements.append("CLOSE")


def _configure_runtime(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    fail_table: str | None = None,
) -> _FakeConnection:
    uv_lock = tmp_path / "uv.lock"
    extension = tmp_path / "tpch.duckdb_extension"
    uv_lock.write_bytes(b"reviewed lock")
    extension.write_bytes(b"reviewed extension")
    monkeypatch.setattr(exporter, "UV_LOCK_PATH", uv_lock)
    monkeypatch.setattr(exporter, "TPCH_EXTENSION_PATH", extension)
    monkeypatch.setattr(exporter, "UV_LOCK_SHA256", hashlib.sha256(b"reviewed lock").hexdigest())
    monkeypatch.setattr(
        exporter,
        "TPCH_EXTENSION_SHA256",
        hashlib.sha256(b"reviewed extension").hexdigest(),
    )
    monkeypatch.setattr(exporter.duckdb, "__version__", exporter.DUCKDB_VERSION)
    monkeypatch.setenv("LANG", "C.UTF-8")
    monkeypatch.setenv("LC_ALL", "C.UTF-8")
    monkeypatch.setenv("TZ", "UTC")
    connection = _FakeConnection(tmp_path, fail_table=fail_table)
    monkeypatch.setattr(exporter.duckdb, "connect", lambda: connection)
    return connection


def _cli_args(output_dir: Path, metadata: Path, *, scale: str = "1") -> list[str]:
    return [
        "--scale",
        scale,
        "--output-dir",
        str(output_dir),
        "--metadata",
        str(metadata),
    ]


@pytest.mark.parametrize(
    ("scale", "dbgen"),
    [("0.01", "CALL dbgen(sf=0.01)"), ("1", "CALL dbgen(sf=1)"), ("10", "CALL dbgen(sf=10)")],
)
def test_cli_generates_all_tables_and_deterministic_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    scale: str,
    dbgen: str,
):
    connection = _configure_runtime(monkeypatch, tmp_path)
    output_dir = tmp_path / "output"
    metadata = tmp_path / "metadata.yaml"

    assert exporter.main(["--scale", scale, "--output-dir", str(output_dir), "--metadata", str(metadata)]) == 0

    assert connection.statements[:4] == [
        "SET threads=1",
        "SET preserve_insertion_order=true",
        "LOAD tpch",
        dbgen,
    ]
    assert all("INSTALL" not in statement for statement in connection.statements)
    assert connection.statements[-1] == "CLOSE"
    assert sorted(path.name for path in output_dir.iterdir()) == [
        f"{table}.parquet" for table in sorted(exporter.TABLE_ORDER_BY)
    ]
    document = yaml.safe_load(metadata.read_text(encoding="utf-8"))
    assert document["scale_factor"] == float(scale)
    assert document["environment"] == exporter.environment_metadata()
    assert list(document["outputs"]) == list(exporter.TABLE_ORDER_BY)
    for table, output in document["outputs"].items():
        path = output_dir / f"{table}.parquet"
        assert output == {
            "object_name": f"{table}.parquet",
            "size_bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }


@pytest.mark.parametrize("scale", ["0", "0.1", "1.0", "nan", "inf"])
def test_cli_rejects_noncanonical_scale(scale: str, tmp_path: Path):
    with pytest.raises(SystemExit) as error:
        exporter.main(
            [
                "--scale",
                scale,
                "--output-dir",
                str(tmp_path / "output"),
                "--metadata",
                str(tmp_path / "metadata.yaml"),
            ]
        )
    assert error.value.code == 2


def test_cli_fails_closed_on_version_or_environment_drift(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _configure_runtime(monkeypatch, tmp_path)
    monkeypatch.setattr(exporter.duckdb, "__version__", "1.5.5")
    with pytest.raises(ValueError, match="DuckDB version mismatch"):
        exporter.main(
            [
                "--scale",
                "1",
                "--output-dir",
                str(tmp_path / "output"),
                "--metadata",
                str(tmp_path / "metadata.yaml"),
            ]
        )

    monkeypatch.setattr(exporter.duckdb, "__version__", exporter.DUCKDB_VERSION)
    monkeypatch.setenv("TZ", "America/New_York")
    with pytest.raises(ValueError, match="timezone must be UTC"):
        exporter.main(
            [
                "--scale",
                "1",
                "--output-dir",
                str(tmp_path / "output-2"),
                "--metadata",
                str(tmp_path / "metadata-2.yaml"),
            ]
        )


def test_metadata_is_not_written_when_an_output_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _configure_runtime(monkeypatch, tmp_path, fail_table="part")
    metadata = tmp_path / "metadata.yaml"
    with pytest.raises(RuntimeError, match="simulated COPY failure"):
        exporter.main(
            [
                "--scale",
                "1",
                "--output-dir",
                str(tmp_path / "output"),
                "--metadata",
                str(metadata),
            ]
        )
    assert not metadata.exists()


@pytest.mark.parametrize("with_stale_file", [False, True])
def test_cli_rejects_preexisting_output_directory_before_connect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    with_stale_file: bool,
):
    _configure_runtime(monkeypatch, tmp_path)
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    if with_stale_file:
        (output_dir / "stale.parquet").write_bytes(b"stale")
    metadata = tmp_path / "metadata.yaml"
    connect_calls: list[None] = []
    monkeypatch.setattr(exporter.duckdb, "connect", lambda: connect_calls.append(None))

    with pytest.raises(ValueError, match="output directory must not exist"):
        exporter.main(_cli_args(output_dir, metadata))

    assert connect_calls == []
    assert not metadata.exists()
    assert sorted(path.name for path in output_dir.iterdir()) == (["stale.parquet"] if with_stale_file else [])


@pytest.mark.parametrize("kind", ["file", "dangling-symlink"])
def test_cli_rejects_preexisting_metadata_before_connect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
):
    _configure_runtime(monkeypatch, tmp_path)
    output_dir = tmp_path / "output"
    metadata = tmp_path / "metadata.yaml"
    if kind == "file":
        metadata.write_text("stale: evidence\n", encoding="utf-8")
    else:
        metadata.symlink_to(tmp_path / "missing-target.yaml")
    connect_calls: list[None] = []
    monkeypatch.setattr(exporter.duckdb, "connect", lambda: connect_calls.append(None))

    with pytest.raises(ValueError, match="metadata path must not exist"):
        exporter.main(_cli_args(output_dir, metadata))

    assert connect_calls == []
    assert not output_dir.exists()


def test_cli_rejects_direct_metadata_output_collision_before_connect(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _configure_runtime(monkeypatch, tmp_path)
    output_dir = tmp_path / "output"
    metadata = output_dir / "customer.parquet"
    connect_calls: list[None] = []
    monkeypatch.setattr(exporter.duckdb, "connect", lambda: connect_calls.append(None))

    with pytest.raises(ValueError, match="metadata path collides with TPC-H output"):
        exporter.main(_cli_args(output_dir, metadata))

    assert connect_calls == []
    assert not output_dir.exists()


def test_cli_rejects_aliased_parent_metadata_output_collision_before_connect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    _configure_runtime(monkeypatch, tmp_path)
    real_parent = tmp_path / "real"
    real_parent.mkdir()
    alias_parent = tmp_path / "alias"
    alias_parent.symlink_to(real_parent, target_is_directory=True)
    output_dir = real_parent / "output"
    metadata = alias_parent / "output" / "lineitem.parquet"
    connect_calls: list[None] = []
    monkeypatch.setattr(exporter.duckdb, "connect", lambda: connect_calls.append(None))

    with pytest.raises(ValueError, match="metadata path collides with TPC-H output"):
        exporter.main(_cli_args(output_dir, metadata))

    assert connect_calls == []
    assert not output_dir.exists()


@pytest.mark.parametrize("metadata_position", ["same-as-output", "parent-of-output"])
def test_cli_rejects_metadata_directory_collision_before_connect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    metadata_position: str,
):
    _configure_runtime(monkeypatch, tmp_path)
    metadata = tmp_path / "fresh"
    output_dir = metadata if metadata_position == "same-as-output" else metadata / "output"
    connect_calls: list[None] = []
    monkeypatch.setattr(exporter.duckdb, "connect", lambda: connect_calls.append(None))

    with pytest.raises(ValueError, match="metadata path collides with output directory"):
        exporter.main(_cli_args(output_dir, metadata))

    assert connect_calls == []
    assert not metadata.exists()


def test_failed_export_leaves_no_metadata_and_rerun_refuses_partial_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    first_connection = _configure_runtime(monkeypatch, tmp_path, fail_table="part")
    output_dir = tmp_path / "output"
    metadata = tmp_path / "metadata.yaml"
    with pytest.raises(RuntimeError, match="simulated COPY failure"):
        exporter.main(_cli_args(output_dir, metadata))
    assert not metadata.exists()
    assert list(output_dir.glob("*.parquet"))
    assert first_connection.statements[-1] == "CLOSE"

    connect_calls: list[None] = []
    monkeypatch.setattr(exporter.duckdb, "connect", lambda: connect_calls.append(None))
    with pytest.raises(ValueError, match="output directory must not exist"):
        exporter.main(_cli_args(output_dir, metadata))
    assert connect_calls == []
    assert not metadata.exists()


def test_successful_rerun_refuses_prior_outputs_and_metadata(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _configure_runtime(monkeypatch, tmp_path)
    output_dir = tmp_path / "output"
    metadata = tmp_path / "metadata.yaml"
    assert exporter.main(_cli_args(output_dir, metadata)) == 0

    connect_calls: list[None] = []
    monkeypatch.setattr(exporter.duckdb, "connect", lambda: connect_calls.append(None))
    with pytest.raises(ValueError, match="output directory must not exist"):
        exporter.main(_cli_args(output_dir, metadata))
    assert connect_calls == []
    assert metadata.exists()


def test_metadata_write_is_atomic_for_fresh_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _configure_runtime(monkeypatch, tmp_path)
    metadata = tmp_path / "metadata.yaml"

    def fail_replace(source: Path, destination: Path):
        assert source.parent == metadata.parent
        assert destination == metadata
        raise OSError("simulated replace failure")

    monkeypatch.setattr(exporter.os, "replace", fail_replace)
    with pytest.raises(OSError, match="simulated replace failure"):
        exporter.main(
            [
                "--scale",
                "1",
                "--output-dir",
                str(tmp_path / "output"),
                "--metadata",
                str(metadata),
            ]
        )
    assert not metadata.exists()
    assert [path for path in tmp_path.iterdir() if path.name.startswith(".metadata.yaml.")] == []


def test_metadata_parent_is_fsynced_after_atomic_replace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _configure_runtime(monkeypatch, tmp_path)
    output_dir = tmp_path / "output"
    metadata = tmp_path / "metadata.yaml"
    events: list[tuple[str, Path]] = []
    real_replace = exporter.os.replace

    def recording_replace(source: Path, destination: Path):
        real_replace(source, destination)
        events.append(("replace", destination))

    def recording_fsync_directory(path: Path):
        assert metadata.exists()
        events.append(("fsync-directory", path))

    monkeypatch.setattr(exporter.os, "replace", recording_replace)
    monkeypatch.setattr(exporter, "_fsync_directory", recording_fsync_directory)

    assert exporter.main(_cli_args(output_dir, metadata)) == 0
    assert events == [("replace", metadata), ("fsync-directory", metadata.parent)]


def test_fsync_directory_uses_portable_readonly_directory_descriptor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    opened: list[tuple[Path, int]] = []
    synced: list[int] = []
    closed: list[int] = []
    monkeypatch.setattr(exporter.os, "open", lambda path, flags: opened.append((path, flags)) or 731)
    monkeypatch.setattr(exporter.os, "fsync", synced.append)
    monkeypatch.setattr(exporter.os, "close", closed.append)

    exporter._fsync_directory(tmp_path)

    expected_flags = exporter.os.O_RDONLY | getattr(exporter.os, "O_DIRECTORY", 0)
    assert opened == [(tmp_path, expected_flags)]
    assert synced == [731]
    assert closed == [731]
