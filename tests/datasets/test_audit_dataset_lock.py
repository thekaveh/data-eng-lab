from __future__ import annotations

import hashlib
import os
import stat
import struct
import tempfile
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest
import responses
import yaml

from scripts import audit_dataset_lock as audit

ROOT = Path(__file__).resolve().parents[2]


def zip_bytes(members: dict[str, bytes]) -> bytes:
    with tempfile.SpooledTemporaryFile() as stream:
        with zipfile.ZipFile(stream, "w") as archive:
            for name, payload in members.items():
                archive.writestr(name, payload)
        stream.seek(0)
        return stream.read()


def compressed_zip_bytes(members: dict[str, bytes]) -> bytes:
    with tempfile.SpooledTemporaryFile() as stream:
        with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for name, payload in members.items():
                archive.writestr(name, payload)
        stream.seek(0)
        return stream.read()


def bounded_zip_metadata_bytes(*, zip64: bool, entries: int, central_directory_size: int) -> bytes:
    if not zip64:
        return struct.pack(
            "<4s4H2LH",
            b"PK\x05\x06",
            0,
            0,
            entries,
            entries,
            central_directory_size,
            0,
            0,
        )
    zip64_eocd = struct.pack(
        "<4sQ2H2L4Q",
        b"PK\x06\x06",
        44,
        45,
        45,
        0,
        0,
        entries,
        entries,
        central_directory_size,
        0,
    )
    locator = struct.pack("<4sLQL", b"PK\x06\x07", 0, 0, 1)
    eocd = struct.pack(
        "<4s4H2LH",
        b"PK\x05\x06",
        0,
        0,
        0xFFFF,
        0xFFFF,
        0xFFFFFFFF,
        0xFFFFFFFF,
        0,
    )
    return zip64_eocd + locator + eocd


def zip64_record_bytes(*, entries: int, central_directory_size: int, central_directory_offset: int) -> bytes:
    return struct.pack(
        "<4sQ2H2L4Q",
        b"PK\x06\x06",
        44,
        45,
        45,
        0,
        0,
        entries,
        entries,
        central_directory_size,
        central_directory_offset,
    )


def sentinel_eocd_bytes() -> bytes:
    return struct.pack(
        "<4s4H2LH",
        b"PK\x05\x06",
        0,
        0,
        0xFFFF,
        0xFFFF,
        0xFFFFFFFF,
        0xFFFFFFFF,
        0,
    )


def valid_zip64_bytes(members: dict[str, bytes]) -> bytes:
    payload = bytearray(zip_bytes(members))
    eocd_offset = payload.rfind(b"PK\x05\x06")
    eocd = struct.unpack_from("<4s4H2LH", payload, eocd_offset)
    entries = eocd[4]
    central_directory_size = eocd[5]
    central_directory_offset = eocd[6]
    record = zip64_record_bytes(
        entries=entries,
        central_directory_size=central_directory_size,
        central_directory_offset=central_directory_offset,
    )
    locator = struct.pack("<4sLQL", b"PK\x06\x07", 0, eocd_offset, 1)
    return bytes(payload[:eocd_offset] + record + locator + sentinel_eocd_bytes())


def zip_with_false_eocd_in_comment() -> bytes:
    with tempfile.SpooledTemporaryFile() as stream:
        with zipfile.ZipFile(stream, "w") as archive:
            archive.writestr("data.csv", b"locked")
            archive.comment = b"comment-prefix" + struct.pack(
                "<4s4H2LH",
                b"PK\x05\x06",
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            )
        stream.seek(0)
        return stream.read()


def divergent_zip64_layout_bytes(kind: str) -> bytes:
    safe_record = zip64_record_bytes(
        entries=0,
        central_directory_size=0,
        central_directory_offset=0,
    )
    locator = struct.pack("<4sLQL", b"PK\x06\x07", 0, 0, 1)
    if kind == "non-adjacent":
        return safe_record + b"GAP!" + locator + sentinel_eocd_bytes()
    if kind == "divergent-offset":
        consumed_record = zip64_record_bytes(
            entries=1,
            central_directory_size=1,
            central_directory_offset=0,
        )
        return safe_record + consumed_record + locator + sentinel_eocd_bytes()
    raise AssertionError(f"unknown ZIP64 layout {kind}")


def zip_with_central_directory_violation(kind: str) -> bytes:
    payload = bytearray(zip_bytes({"data.csv": b"locked"}))
    eocd_offset = payload.rfind(b"PK\x05\x06")
    central_header = payload.index(b"PK\x01\x02")
    if kind == "count-mismatch":
        struct.pack_into("<H", payload, eocd_offset + 8, 2)
        struct.pack_into("<H", payload, eocd_offset + 10, 2)
    elif kind == "truncated-fixed-header":
        struct.pack_into("<L", payload, eocd_offset + 12, 10)
    elif kind == "variable-record-overrun":
        filename_size = struct.unpack_from("<H", payload, central_header + 28)[0]
        struct.pack_into("<H", payload, central_header + 28, filename_size + 100)
    else:
        raise AssertionError(f"unknown central-directory violation {kind}")
    return bytes(payload)


def zip_bytes_with_member_policy_violation(kind: str) -> bytes:
    payload = bytearray(zip_bytes({"data.csv": b"locked"}))
    local_header = payload.index(b"PK\x03\x04")
    central_header = payload.index(b"PK\x01\x02")
    if kind == "encrypted":
        local_flags = struct.unpack_from("<H", payload, local_header + 6)[0]
        central_flags = struct.unpack_from("<H", payload, central_header + 8)[0]
        struct.pack_into("<H", payload, local_header + 6, local_flags | 1)
        struct.pack_into("<H", payload, central_header + 8, central_flags | 1)
    elif kind == "unsupported-compression":
        struct.pack_into("<H", payload, local_header + 8, 99)
        struct.pack_into("<H", payload, central_header + 10, 99)
    else:
        raise AssertionError(f"unknown ZIP mutation {kind}")
    return bytes(payload)


def record_audit_temp_directories(monkeypatch: pytest.MonkeyPatch) -> list[Path]:
    real_temporary_directory = tempfile.TemporaryDirectory
    created: list[Path] = []

    class RecordingTemporaryDirectory:
        def __init__(self) -> None:
            self._temporary_directory = real_temporary_directory()

        def __enter__(self) -> str:
            path = self._temporary_directory.__enter__()
            created.append(Path(path))
            return path

        def __exit__(self, *args: object) -> object:
            return self._temporary_directory.__exit__(*args)

    monkeypatch.setattr(audit.tempfile, "TemporaryDirectory", RecordingTemporaryDirectory)
    return created


@responses.activate
def test_audit_http_emits_raw_direct_output_and_response_evidence():
    payload = b"id,name\n1,Ada\n"
    responses.add(
        responses.GET,
        "https://source.invalid/data.csv",
        body=payload,
        status=200,
        headers={"ETag": '"locked"', "Last-Modified": "Mon, 01 Jan 2024 00:00:00 GMT"},
    )

    result = audit.audit_http("https://source.invalid/data.csv", archive=False)

    expected_sha256 = hashlib.sha256(payload).hexdigest()
    assert result == {
        "url": "https://source.invalid/data.csv",
        "evidence": {
            "etag": '"locked"',
            "last_modified": "Mon, 01 Jan 2024 00:00:00 GMT",
        },
        "raw": {"name": "data.csv", "size_bytes": 14, "sha256": expected_sha256},
        "outputs": [
            {
                "object_name": "data.csv",
                "size_bytes": 14,
                "sha256": expected_sha256,
                "raw_identity": True,
            }
        ],
    }


@responses.activate
def test_audit_http_streams_with_120_second_timeout(monkeypatch: pytest.MonkeyPatch):
    responses.add(responses.GET, "https://source.invalid/data.csv", body=b"locked", status=200)
    request_get = audit.requests.get
    arguments: dict[str, object] = {}

    def recording_get(url: str, **kwargs: object):
        arguments.update(kwargs)
        return request_get(url, **kwargs)

    monkeypatch.setattr(audit.requests, "get", recording_get)

    audit.audit_http("https://source.invalid/data.csv", archive=False)

    assert arguments["stream"] is True
    assert arguments["allow_redirects"] is False
    assert 119 < arguments["timeout"] <= 120


@responses.activate
def test_audit_http_owns_and_cleans_temporary_directory(monkeypatch: pytest.MonkeyPatch):
    responses.add(responses.GET, "https://source.invalid/data.csv", body=b"locked", status=200)
    created = record_audit_temp_directories(monkeypatch)

    result = audit.audit_http("https://source.invalid/data.csv", archive=False)

    assert result["raw"]["size_bytes"] == 6
    assert len(created) == 1
    assert not created[0].exists()


@responses.activate
def test_audit_http_rejects_unsafe_redirect_before_requesting_target():
    source = "https://source.invalid/data.csv"
    responses.add(responses.GET, source, status=302, headers={"Location": "http://127.0.0.1/private"})

    with pytest.raises(ValueError, match="must be an authoritative HTTPS URL"):
        audit.audit_http(source, archive=False)

    assert [call.request.url for call in responses.calls] == [source]


@responses.activate
def test_audit_http_follows_valid_relative_https_redirect():
    source = "https://source.invalid/data.csv"
    target = "https://source.invalid/releases/data.csv"
    responses.add(responses.GET, source, status=302, headers={"Location": "/releases/data.csv"})
    responses.add(responses.GET, target, body=b"locked", status=200)

    result = audit.audit_http(source, archive=False)

    assert result["raw"]["size_bytes"] == 6
    assert [call.request.url for call in responses.calls] == [source, target]


@responses.activate
def test_audit_http_rejects_redirect_without_location():
    source = "https://source.invalid/data.csv"
    responses.add(responses.GET, source, status=302)

    with pytest.raises(ValueError, match="redirect response is missing Location"):
        audit.audit_http(source, archive=False)


@responses.activate
def test_audit_http_rejects_redirect_overflow(monkeypatch: pytest.MonkeyPatch):
    source = "https://source.invalid/data.csv"
    monkeypatch.setattr(audit, "MAX_REDIRECTS", 1)
    responses.add(responses.GET, source, status=302, headers={"Location": "/next.csv"})
    responses.add(
        responses.GET,
        "https://source.invalid/next.csv",
        status=302,
        headers={"Location": "/overflow.csv"},
    )

    with pytest.raises(ValueError, match="too many redirects"):
        audit.audit_http(source, archive=False)

    assert len(responses.calls) == 2


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com/data.csv",
        "https://localhost/data.csv",
        "https://127.0.0.1/data.csv",
        "https://user:secret@example.com/data.csv",
        "https://example.com:443/data.csv",
        "https://example.com/data.csv#fragment",
    ],
)
def test_audit_http_reuses_authoritative_https_policy(url: str):
    with pytest.raises(ValueError, match="must be an authoritative HTTPS URL"):
        audit.audit_http(url, archive=False)


@responses.activate
def test_audit_http_rejects_empty_artifact():
    responses.add(responses.GET, "https://source.invalid/empty.csv", body=b"", status=200)

    with pytest.raises(ValueError, match="artifact must not be empty"):
        audit.audit_http("https://source.invalid/empty.csv", archive=False)


@responses.activate
def test_audit_http_rejects_download_over_limit_and_cleans_temp(
    monkeypatch: pytest.MonkeyPatch,
):
    source = "https://source.invalid/data.csv"
    responses.add(responses.GET, source, body=b"123456", status=200)
    monkeypatch.setattr(audit, "MAX_DOWNLOAD_BYTES", 5)
    created = record_audit_temp_directories(monkeypatch)

    with pytest.raises(ValueError, match="download exceeds 5 bytes"):
        audit.audit_http(source, archive=False)

    assert created and all(not path.exists() for path in created)


@responses.activate
def test_audit_http_enforces_overall_streaming_deadline_and_cleans_temp(
    monkeypatch: pytest.MonkeyPatch,
):
    source = "https://source.invalid/data.csv"
    responses.add(responses.GET, source, body=b"locked", status=200)
    ticks = iter([0.0, 0.0, 121.0])
    last_tick = 121.0

    def monotonic() -> float:
        nonlocal last_tick
        last_tick = next(ticks, last_tick)
        return last_tick

    monkeypatch.setattr(audit.time, "monotonic", monotonic)
    created = record_audit_temp_directories(monkeypatch)

    with pytest.raises(ValueError, match="download deadline exceeded"):
        audit.audit_http(source, archive=False)

    assert created and all(not path.exists() for path in created)


def test_audit_http_rebounds_every_transport_read_to_cumulative_deadline(
    monkeypatch: pytest.MonkeyPatch,
):
    clock = {"now": 0.0}

    class FakeSocket:
        def __init__(self) -> None:
            self.timeouts: list[float] = []

        def settimeout(self, value: float) -> None:
            self.timeouts.append(value)

    fake_socket = FakeSocket()

    class FakeRaw:
        def __init__(self) -> None:
            self._connection = SimpleNamespace(sock=fake_socket)
            self.calls = 0

        def read1(self, amount: int, *, decode_content: bool) -> bytes:
            assert amount == 1 << 20
            assert decode_content is False
            self.calls += 1
            if self.calls == 1:
                clock["now"] = 100.0
                return b"a"
            if self.calls == 2:
                assert fake_socket.timeouts[-1] <= 20
                clock["now"] = 105.0
                return b"b"
            return b""

    class FakeResponse:
        status_code = 200
        headers: dict[str, str] = {}

        def __init__(self) -> None:
            self.raw = FakeRaw()

        def __enter__(self):
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def raise_for_status(self) -> None:
            return None

        def iter_content(self, *, chunk_size: int):
            assert chunk_size == 1 << 20
            clock["now"] = 100.0
            yield b"a"
            clock["now"] = 220.0
            yield b"b"

    response = FakeResponse()
    monkeypatch.setattr(audit.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(audit.requests, "get", lambda *args, **kwargs: response)

    result = audit.audit_http("https://source.invalid/data.csv", archive=False)

    assert result["raw"]["size_bytes"] == 2
    assert fake_socket.timeouts == pytest.approx([120, 20, 15])


def test_audit_http_rejects_blocking_transport_without_bounded_socket(
    monkeypatch: pytest.MonkeyPatch,
):
    class UnboundedRaw:
        _connection = None
        _fp = object()

        def __init__(self) -> None:
            self.read = False

        def read1(self, amount: int, *, decode_content: bool) -> bytes:
            if self.read:
                return b""
            self.read = True
            return b"unbounded"

    class FakeResponse:
        status_code = 200
        headers: dict[str, str] = {}
        raw = UnboundedRaw()

        def __enter__(self):
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr(audit.requests, "get", lambda *args, **kwargs: FakeResponse())

    with pytest.raises(ValueError, match="transport does not expose a bounded socket"):
        audit.audit_http("https://source.invalid/data.csv", archive=False)


@responses.activate
def test_audit_zip_preserves_exact_member_paths_and_hashes_saved_bytes():
    members = {"a/data.csv": b"a", "b/other.csv": b"other"}
    payload = zip_bytes(members)
    responses.add(responses.GET, "https://source.invalid/data.zip", body=payload, status=200)

    result = audit.audit_http("https://source.invalid/data.zip", archive=True)

    assert result["raw"] == {
        "name": "data.zip",
        "size_bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }
    assert result["outputs"] == [
        {
            "object_name": "data.csv",
            "member_path": "a/data.csv",
            "size_bytes": 1,
            "sha256": hashlib.sha256(b"a").hexdigest(),
        },
        {
            "object_name": "other.csv",
            "member_path": "b/other.csv",
            "size_bytes": 5,
            "sha256": hashlib.sha256(b"other").hexdigest(),
        },
    ]


@responses.activate
def test_audit_zip_rejects_flattened_basename_collision():
    payload = zip_bytes({"a/data.csv": b"a", "b/data.csv": b"b"})
    responses.add(responses.GET, "https://source.invalid/data.zip", body=payload, status=200)

    with pytest.raises(ValueError, match="flatten to duplicate object name data.csv"):
        audit.audit_http("https://source.invalid/data.zip", archive=True)


@pytest.mark.parametrize("member", ["/absolute.csv", "../escape.csv", "a/../escape.csv", "a\\data.csv", "a/."])
@responses.activate
def test_audit_zip_rejects_unsafe_member_paths(member: str):
    responses.add(
        responses.GET,
        "https://source.invalid/data.zip",
        body=zip_bytes({member: b"unsafe"}),
        status=200,
    )

    with pytest.raises(ValueError, match="must be a safe relative POSIX path"):
        audit.audit_http("https://source.invalid/data.zip", archive=True)


@responses.activate
def test_audit_zip_rejects_symlinks():
    with tempfile.SpooledTemporaryFile() as stream:
        with zipfile.ZipFile(stream, "w") as archive:
            member = zipfile.ZipInfo("link.csv")
            member.create_system = 3
            member.external_attr = (stat.S_IFLNK | 0o777) << 16
            archive.writestr(member, "target.csv")
        stream.seek(0)
        responses.add(responses.GET, "https://source.invalid/data.zip", body=stream.read(), status=200)

    with pytest.raises(ValueError, match="symlink"):
        audit.audit_http("https://source.invalid/data.zip", archive=True)


@responses.activate
def test_audit_zip_rejects_non_file_members():
    responses.add(
        responses.GET,
        "https://source.invalid/data.zip",
        body=zip_bytes({"directory/": b""}),
        status=200,
    )

    with pytest.raises(ValueError, match="regular file"):
        audit.audit_http("https://source.invalid/data.zip", archive=True)


@responses.activate
def test_audit_zip_rejects_member_count_limit_and_cleans_temp(monkeypatch: pytest.MonkeyPatch):
    source = "https://source.invalid/data.zip"
    responses.add(responses.GET, source, body=zip_bytes({"a.csv": b"a", "b.csv": b"b"}), status=200)
    monkeypatch.setattr(audit, "MAX_ARCHIVE_MEMBERS", 1)
    created = record_audit_temp_directories(monkeypatch)

    with pytest.raises(ValueError, match="archive contains more than 1 members"):
        audit.audit_http(source, archive=True)

    assert created and all(not path.exists() for path in created)


@responses.activate
def test_audit_zip_rejects_declared_member_size_limit_and_cleans_temp(monkeypatch: pytest.MonkeyPatch):
    source = "https://source.invalid/data.zip"
    responses.add(responses.GET, source, body=zip_bytes({"large.csv": b"ab"}), status=200)
    monkeypatch.setattr(audit, "MAX_MEMBER_BYTES", 1)
    created = record_audit_temp_directories(monkeypatch)

    with pytest.raises(ValueError, match="member large.csv exceeds 1 bytes"):
        audit.audit_http(source, archive=True)

    assert created and all(not path.exists() for path in created)


@responses.activate
def test_audit_zip_rejects_total_uncompressed_limit_and_cleans_temp(monkeypatch: pytest.MonkeyPatch):
    source = "https://source.invalid/data.zip"
    responses.add(responses.GET, source, body=zip_bytes({"a.csv": b"aa", "b.csv": b"bb"}), status=200)
    monkeypatch.setattr(audit, "MAX_TOTAL_UNCOMPRESSED_BYTES", 3)
    created = record_audit_temp_directories(monkeypatch)

    with pytest.raises(ValueError, match="archive exceeds 3 uncompressed bytes"):
        audit.audit_http(source, archive=True)

    assert created and all(not path.exists() for path in created)


@responses.activate
def test_audit_zip_rejects_compression_ratio_limit_and_cleans_temp(monkeypatch: pytest.MonkeyPatch):
    source = "https://source.invalid/data.zip"
    payload = compressed_zip_bytes({"dense.csv": b"0" * 1_000})
    responses.add(responses.GET, source, body=payload, status=200)
    monkeypatch.setattr(audit, "MAX_COMPRESSION_RATIO", 2)
    created = record_audit_temp_directories(monkeypatch)

    with pytest.raises(ValueError, match="member dense.csv exceeds compression ratio 2"):
        audit.audit_http(source, archive=True)

    assert created and all(not path.exists() for path in created)


@pytest.mark.parametrize("zip64", [False, True], ids=["eocd", "zip64"])
@pytest.mark.parametrize(
    ("bound", "entries", "central_directory_size", "message"),
    [
        ("members", 2, 0, "archive contains more than 1 members"),
        ("central-directory", 1, 2, "central directory exceeds 1 bytes"),
    ],
)
@responses.activate
def test_audit_zip_preflights_metadata_bounds_before_zipfile_construction(
    monkeypatch: pytest.MonkeyPatch,
    zip64: bool,
    bound: str,
    entries: int,
    central_directory_size: int,
    message: str,
):
    source = "https://source.invalid/data.zip"
    payload = bounded_zip_metadata_bytes(
        zip64=zip64,
        entries=entries,
        central_directory_size=central_directory_size,
    )
    responses.add(responses.GET, source, body=payload, status=200)
    monkeypatch.setattr(audit, "MAX_ARCHIVE_MEMBERS", 1)
    monkeypatch.setattr(audit, "MAX_CENTRAL_DIRECTORY_BYTES", 1, raising=False)
    zipfile_constructed = False

    def forbidden_zipfile(*args: object, **kwargs: object):
        nonlocal zipfile_constructed
        zipfile_constructed = True
        raise AssertionError("ZipFile metadata parsing was reached before preflight")

    monkeypatch.setattr(audit.zipfile, "ZipFile", forbidden_zipfile)

    with pytest.raises(ValueError, match=message):
        audit.audit_http(source, archive=True)

    assert not zipfile_constructed


@responses.activate
def test_audit_zip_accepts_valid_fixed_layout_zip64_archive():
    source = "https://source.invalid/data.zip"
    responses.add(
        responses.GET,
        source,
        body=valid_zip64_bytes({"data.csv": b"locked"}),
        status=200,
    )

    result = audit.audit_http(source, archive=True)

    assert result["outputs"][0]["object_name"] == "data.csv"
    assert result["outputs"][0]["size_bytes"] == 6


@responses.activate
def test_audit_zip_rejects_false_eocd_signature_in_comment_before_zipfile(
    monkeypatch: pytest.MonkeyPatch,
):
    source = "https://source.invalid/data.zip"
    responses.add(responses.GET, source, body=zip_with_false_eocd_in_comment(), status=200)
    zipfile_constructed = False

    def forbidden_zipfile(*args: object, **kwargs: object):
        nonlocal zipfile_constructed
        zipfile_constructed = True
        raise AssertionError("ZipFile was constructed for ambiguous EOCD")

    monkeypatch.setattr(audit.zipfile, "ZipFile", forbidden_zipfile)

    with pytest.raises(ValueError, match="ambiguous end-of-central-directory record"):
        audit.audit_http(source, archive=True)

    assert not zipfile_constructed


@pytest.mark.parametrize("kind", ["non-adjacent", "divergent-offset"])
@responses.activate
def test_audit_zip_rejects_divergent_zip64_layout_before_zipfile(
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
):
    source = "https://source.invalid/data.zip"
    responses.add(responses.GET, source, body=divergent_zip64_layout_bytes(kind), status=200)
    zipfile_constructed = False

    def forbidden_zipfile(*args: object, **kwargs: object):
        nonlocal zipfile_constructed
        zipfile_constructed = True
        raise AssertionError("ZipFile was constructed for divergent ZIP64 metadata")

    monkeypatch.setattr(audit.zipfile, "ZipFile", forbidden_zipfile)

    with pytest.raises(ValueError, match="ZIP64 record layout is inconsistent"):
        audit.audit_http(source, archive=True)

    assert not zipfile_constructed


@pytest.mark.parametrize(
    ("kind", "message"),
    [
        ("count-mismatch", "central directory contains 1 records but declares 2"),
        ("truncated-fixed-header", "central directory has a truncated fixed header"),
        ("variable-record-overrun", "central directory record exceeds declared region"),
    ],
)
@responses.activate
def test_audit_zip_stream_validates_central_directory_before_zipfile(
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    message: str,
):
    source = "https://source.invalid/data.zip"
    responses.add(
        responses.GET,
        source,
        body=zip_with_central_directory_violation(kind),
        status=200,
    )
    zipfile_constructed = False

    def forbidden_zipfile(*args: object, **kwargs: object):
        nonlocal zipfile_constructed
        zipfile_constructed = True
        raise AssertionError("ZipFile was constructed before central-directory validation")

    monkeypatch.setattr(audit.zipfile, "ZipFile", forbidden_zipfile)

    with pytest.raises(ValueError, match=message):
        audit.audit_http(source, archive=True)

    assert not zipfile_constructed


@pytest.mark.parametrize(
    ("kind", "message"),
    [
        ("encrypted", "encrypted archive member data.csv is not supported"),
        ("unsupported-compression", "archive member data.csv uses unsupported compression method 99"),
    ],
)
@responses.activate
def test_audit_zip_rejects_encryption_and_unsupported_compression(kind: str, message: str):
    source = "https://source.invalid/data.zip"
    responses.add(
        responses.GET,
        source,
        body=zip_bytes_with_member_policy_violation(kind),
        status=200,
    )

    with pytest.raises(ValueError, match=message):
        audit.audit_http(source, archive=True)


@responses.activate
def test_audit_zip_converts_corrupt_content_to_value_error():
    source = "https://source.invalid/data.zip"
    responses.add(responses.GET, source, body=b"not a zip", status=200)

    with pytest.raises(ValueError, match="artifact is not a valid ZIP archive"):
        audit.audit_http(source, archive=True)


@responses.activate
def test_audit_http_omits_blank_response_evidence():
    source = "https://source.invalid/data.csv"
    responses.add(
        responses.GET,
        source,
        body=b"locked",
        status=200,
        headers={"ETag": ' " " ', "Last-Modified": "   "},
    )

    result = audit.audit_http(source, archive=False)

    assert result["evidence"] == {}


@responses.activate
def test_audit_http_strips_response_evidence_whitespace():
    source = "https://source.invalid/data.csv"
    responses.add(
        responses.GET,
        source,
        body=b"locked",
        status=200,
        headers={"ETag": '  "locked"  ', "Last-Modified": "  Mon, 01 Jan 2024 00:00:00 GMT  "},
    )

    result = audit.audit_http(source, archive=False)

    assert result["evidence"] == {
        "etag": '"locked"',
        "last_modified": "Mon, 01 Jan 2024 00:00:00 GMT",
    }


def test_cli_requires_explicit_output_and_never_changes_registry(tmp_path: Path):
    registry = ROOT / "datasets" / "registry.yaml"
    before = registry.read_bytes()

    assert audit.main(["http", "--url", "https://source.invalid/data.csv"]) == 2

    assert registry.read_bytes() == before
    assert list(tmp_path.iterdir()) == []


def test_cli_rejects_registry_as_output_without_fetching():
    registry = ROOT / "datasets" / "registry.yaml"
    before = registry.read_bytes()

    assert audit.main(
        ["http", "--url", "https://source.invalid/data.csv", "--output", str(registry)]
    ) == 2

    assert registry.read_bytes() == before


@responses.activate
def test_cli_rejects_hard_link_to_registry_without_fetching(tmp_path: Path):
    registry = ROOT / "datasets" / "registry.yaml"
    before = registry.read_bytes()
    output = tmp_path / "registry-alias.yaml"
    output.hardlink_to(registry)

    assert audit.main(
        ["http", "--url", "https://source.invalid/data.csv", "--output", str(output)]
    ) == 2

    assert registry.read_bytes() == before


@pytest.mark.parametrize("alias_kind", ["symlink", "hardlink"])
@responses.activate
def test_cli_rechecks_registry_alias_immediately_before_atomic_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    alias_kind: str,
):
    source = "https://source.invalid/data.csv"
    responses.add(responses.GET, source, body=b"locked", status=200)
    registry = ROOT / "datasets" / "registry.yaml"
    before = registry.read_bytes()
    output = tmp_path / "candidate.yaml"
    real_fsync = os.fsync

    def swap_output(file_descriptor: int) -> None:
        real_fsync(file_descriptor)
        if alias_kind == "symlink":
            output.symlink_to(registry)
        else:
            output.hardlink_to(registry)

    monkeypatch.setattr(audit.os, "fsync", swap_output)

    assert audit.main(["http", "--url", source, "--output", str(output)]) == 2

    assert registry.read_bytes() == before
    assert sorted(path.name for path in tmp_path.iterdir()) == ["candidate.yaml"]


@responses.activate
def test_cli_leaves_existing_output_unchanged_when_serialization_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    source = "https://source.invalid/data.csv"
    responses.add(responses.GET, source, body=b"locked", status=200)
    output = tmp_path / "candidate.yaml"
    output.write_text("previous\n")

    def fail_serialization(*args: object, **kwargs: object) -> str:
        raise RuntimeError("serialization failed")

    monkeypatch.setattr(audit.yaml, "safe_dump", fail_serialization)

    with pytest.raises(RuntimeError, match="serialization failed"):
        audit.main(["http", "--url", source, "--output", str(output)])

    assert output.read_text() == "previous\n"
    assert sorted(path.name for path in tmp_path.iterdir()) == ["candidate.yaml"]


@responses.activate
def test_cli_leaves_existing_output_unchanged_and_cleans_temp_when_replace_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    source = "https://source.invalid/data.csv"
    responses.add(responses.GET, source, body=b"locked", status=200)
    output = tmp_path / "candidate.yaml"
    output.write_text("previous\n")

    def fail_replace(source_path: object, destination_path: object) -> None:
        raise OSError("replace failed")

    monkeypatch.setattr(audit.os, "replace", fail_replace)

    with pytest.raises(OSError, match="replace failed"):
        audit.main(["http", "--url", source, "--output", str(output)])

    assert output.read_text() == "previous\n"
    assert sorted(path.name for path in tmp_path.iterdir()) == ["candidate.yaml"]


@responses.activate
def test_cli_returns_2_for_corrupt_archive_without_output(tmp_path: Path):
    source = "https://source.invalid/data.zip"
    responses.add(responses.GET, source, body=b"not a zip", status=200)
    output = tmp_path / "candidate.yaml"

    assert audit.main(["http", "--url", source, "--archive", "--output", str(output)]) == 2

    assert not output.exists()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    ("kind", "message"),
    [
        ("encrypted", "encrypted archive member data.csv is not supported"),
        ("unsupported-compression", "archive member data.csv uses unsupported compression method 99"),
    ],
)
@responses.activate
def test_cli_returns_2_for_unsupported_member_policy(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    kind: str,
    message: str,
):
    source = "https://source.invalid/data.zip"
    responses.add(
        responses.GET,
        source,
        body=zip_bytes_with_member_policy_violation(kind),
        status=200,
    )
    output = tmp_path / "candidate.yaml"

    assert audit.main(["http", "--url", source, "--archive", "--output", str(output)]) == 2

    assert message in capsys.readouterr().err
    assert not output.exists()


@responses.activate
def test_cli_keeps_only_requested_deterministic_metadata_output(tmp_path: Path):
    url = "https://source.invalid/data.csv"
    responses.add(responses.GET, url, body=b"locked", status=200)
    responses.add(responses.GET, url, body=b"locked", status=200)
    first = tmp_path / "candidate.yaml"
    second = tmp_path / "candidate-again.yaml"

    assert audit.main(["http", "--url", url, "--output", str(first)]) == 0
    assert audit.main(["http", "--url", url, "--output", str(second)]) == 0

    assert first.read_bytes() == second.read_bytes()
    assert yaml.safe_load(first.read_text()) == {
        "url": url,
        "evidence": {},
        "raw": {
            "name": "data.csv",
            "size_bytes": 6,
            "sha256": hashlib.sha256(b"locked").hexdigest(),
        },
        "outputs": [
            {
                "object_name": "data.csv",
                "size_bytes": 6,
                "sha256": hashlib.sha256(b"locked").hexdigest(),
                "raw_identity": True,
            }
        ],
    }
    assert sorted(path.name for path in tmp_path.iterdir()) == ["candidate-again.yaml", "candidate.yaml"]
