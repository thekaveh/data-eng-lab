from __future__ import annotations

import hashlib
import io
import os
import stat
import struct
import tempfile
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests
import responses
import yaml

from datasets import acquisition
from scripts import audit_dataset_lock as audit

ROOT = Path(__file__).resolve().parents[2]


def test_bound_metadata_helpers_are_internal_only():
    assert not hasattr(acquisition, "bound_download_metadata")
    assert not hasattr(acquisition, "bound_extracted_metadata")
    assert callable(acquisition._bound_download_metadata)
    assert callable(acquisition._bound_extracted_metadata)


@pytest.fixture(autouse=True)
def shared_download_transport(monkeypatch: pytest.MonkeyPatch):
    def process_factory(context: object, send: object, host: str):
        class InlineProcess:
            def start(self) -> None:
                acquisition._resolve_worker(send, host)

            def is_alive(self) -> bool:
                return False

            def join(self, timeout: float = 0) -> None:
                pass

            def close(self) -> None:
                pass

        return InlineProcess()

    class ResponseAdapter:
        def __init__(self, response: requests.Response, peer: str) -> None:
            self.status = response.status_code
            self.headers = response.headers
            self.peer_address = peer
            self._response = response
            self._socket = audit_response_socket(response)

        def read1(self, amount: int, *, decode_content: bool) -> bytes:
            return self._response.raw.read1(amount, decode_content=decode_content)

        def settimeout(self, timeout: float) -> None:
            if self._socket is not None:
                self._socket.settimeout(timeout)
            elif not isinstance(getattr(self._response.raw, "_fp", None), io.BytesIO):
                raise ValueError("HTTP transport does not expose a bounded socket")

        def close(self) -> None:
            if close := getattr(self._response, "close", None):
                close()

    class RequestsTransport:
        trust_env = False

        def request(self, *, url: str, address: str, headers: dict[str, str], timeout: float, **kwargs: object):
            response = requests.get(
                url,
                stream=True,
                timeout=timeout,
                allow_redirects=False,
                headers=headers,
            )
            response.raise_for_status()
            return ResponseAdapter(response, address)

    monkeypatch.setattr(
        acquisition.socket,
        "getaddrinfo",
        lambda *args, **kwargs: [(2, 1, 6, "", ("192.0.0.9", 443))],
    )
    monkeypatch.setattr(acquisition, "_make_resolver_process", process_factory)
    monkeypatch.setattr(audit, "DOWNLOAD_TRANSPORT", RequestsTransport())


def audit_response_socket(response: requests.Response) -> object | None:
    connection = getattr(response.raw, "_connection", None)
    if response_socket := getattr(connection, "sock", None):
        return response_socket
    http_response = getattr(response.raw, "_fp", None)
    buffered_reader = getattr(http_response, "fp", None)
    socket_io = getattr(buffered_reader, "raw", None)
    return getattr(socket_io, "_sock", None)


def test_audit_and_production_share_the_same_zip_policy(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    archive = tmp_path / "data.zip"
    archive.write_bytes(zip_bytes({"data.csv": b"locked"}))
    called: list[tuple[Path, acquisition.ZipLimits]] = []

    def sentinel_policy(path: Path, limits: acquisition.ZipLimits):
        called.append((path, limits))
        return [acquisition.ArchiveEntry("data.csv", "data.csv", 6)]

    monkeypatch.setattr(audit, "validated_zip_members", sentinel_policy)
    monkeypatch.setattr(audit, "_canonical_archive_entries", tuple)
    monkeypatch.setattr(audit, "extract_members", lambda *args: [tmp_path / "extracted"])
    monkeypatch.setattr(audit, "_bound_extracted_metadata", lambda *args: [(6, hashlib.sha256(b"locked").hexdigest())])
    (tmp_path / "extracted").write_bytes(b"locked")

    audit._archive_outputs(archive, tmp_path)

    assert called == [(archive, audit._zip_limits())]


@pytest.mark.parametrize(
    "url",
    [
        "https://example.com/%2E%2E",
        "https://example.com/%2Fetc%2Fpasswd",
        "https://example.com/bad%00name.zip",
        "https://example.com/nested%2Fartifact.zip",
    ],
)
def test_artifact_name_rejects_unsafe_percent_decoded_basename(url: str):
    with pytest.raises(ValueError, match="url path must end with a safe artifact name"):
        audit._artifact_name(url)


def test_artifact_name_accepts_safe_percent_decoded_basename():
    assert audit._artifact_name("https://example.com/release%20data.zip") == "release data.zip"


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
    request_get = requests.get
    arguments: dict[str, object] = {}

    def recording_get(url: str, **kwargs: object):
        arguments.update(kwargs)
        return request_get(url, **kwargs)

    monkeypatch.setattr(requests, "get", recording_get)

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
    monkeypatch.setattr(acquisition, "_MAX_REDIRECTS", 1)
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

    monkeypatch.setattr(acquisition.time, "monotonic", monotonic)
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
    monkeypatch.setattr(acquisition.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: response)

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

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: FakeResponse())

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
def test_audit_zip_accepts_grouplens_structural_directory_and_excludes_it_from_metadata():
    source = "https://source.invalid/ml-latest-small.zip"
    responses.add(
        responses.GET,
        source,
        body=zip_bytes(
            {
                "ml-latest-small/": b"",
                "ml-latest-small/README.txt": b"GroupLens README",
                "ml-latest-small/ratings.csv": b"userId,movieId,rating\n1,1,4.0\n",
            }
        ),
        status=200,
    )

    result = audit.audit_http(source, archive=True)

    assert [output["member_path"] for output in result["outputs"]] == [
        "ml-latest-small/README.txt",
        "ml-latest-small/ratings.csv",
    ]
    assert [output["object_name"] for output in result["outputs"]] == ["README.txt", "ratings.csv"]


@pytest.mark.parametrize(
    "directory",
    [
        "/",
        "//",
        "/absolute/",
        "../escape/",
        "a/../escape/",
        "a\\directory/",
        "double//",
        "a//nested/",
        "a/./",
    ],
)
@responses.activate
def test_audit_zip_rejects_unsafe_directory_paths(directory: str):
    source = "https://source.invalid/data.zip"
    responses.add(
        responses.GET,
        source,
        body=zip_bytes({directory: b"", "data.csv": b"locked"}),
        status=200,
    )

    with pytest.raises(ValueError, match="archive directory: must be a safe relative POSIX path"):
        audit.audit_http(source, archive=True)


@responses.activate
def test_audit_zip_rejects_directory_with_payload():
    source = "https://source.invalid/data.zip"
    responses.add(
        responses.GET,
        source,
        body=zip_bytes({"directory/": b"payload", "data.csv": b"locked"}),
        status=200,
    )

    with pytest.raises(ValueError, match="archive directory 'directory/' must be empty"):
        audit.audit_http(source, archive=True)


def directory_policy_zip(kind: str) -> bytes:
    with tempfile.SpooledTemporaryFile() as stream:
        with zipfile.ZipFile(stream, "w") as archive:
            if kind == "regular-attributes":
                member = zipfile.ZipInfo("ambiguous/")
                member.create_system = 3
                member.external_attr = (stat.S_IFREG | 0o644) << 16
            elif kind == "special-attributes":
                member = zipfile.ZipInfo("special/")
                member.create_system = 3
                member.external_attr = (stat.S_IFIFO | 0o600) << 16
            elif kind == "symlink":
                member = zipfile.ZipInfo("link/")
                member.create_system = 3
                member.external_attr = (stat.S_IFLNK | 0o777) << 16
            elif kind == "directory-attributes-on-file":
                member = zipfile.ZipInfo("ambiguous")
                member.create_system = 3
                member.external_attr = ((stat.S_IFDIR | 0o755) << 16) | 0x10
            elif kind == "missing-directory-attributes":
                member = zipfile.ZipInfo("unmarked/")
                member.create_system = 3
                member.external_attr = 0o755 << 16
            else:
                raise AssertionError(f"unknown directory policy kind {kind}")
            archive.writestr(member, b"")
            archive.writestr("data.csv", b"locked")
        stream.seek(0)
        payload = bytearray(stream.read())

    if kind == "symlink":
        return bytes(payload)
    return bytes(payload)


@pytest.mark.parametrize(
    ("kind", "message"),
    [
        ("regular-attributes", "directory/file attribute ambiguity for archive member 'ambiguous/'"),
        ("special-attributes", "archive directory 'special/' has unsafe attributes"),
        ("symlink", "archive member 'link/' must not be a symlink"),
        ("directory-attributes-on-file", "directory/file attribute ambiguity for archive member 'ambiguous'"),
        (
            "missing-directory-attributes",
            "directory/file attribute ambiguity for archive member 'unmarked/'",
        ),
    ],
)
@responses.activate
def test_audit_zip_rejects_unsafe_or_ambiguous_directory_attributes(kind: str, message: str):
    source = "https://source.invalid/data.zip"
    responses.add(responses.GET, source, body=directory_policy_zip(kind), status=200)

    with pytest.raises(ValueError, match=message):
        audit.audit_http(source, archive=True)


@responses.activate
def test_audit_zip_rejects_encrypted_directory():
    source = "https://source.invalid/data.zip"
    payload = bytearray(zip_bytes({"secret/": b"", "data.csv": b"locked"}))
    local_header = payload.index(b"PK\x03\x04")
    central_header = payload.index(b"PK\x01\x02")
    local_flags = struct.unpack_from("<H", payload, local_header + 6)[0]
    central_flags = struct.unpack_from("<H", payload, central_header + 8)[0]
    struct.pack_into("<H", payload, local_header + 6, local_flags | 1)
    struct.pack_into("<H", payload, central_header + 8, central_flags | 1)
    responses.add(responses.GET, source, body=bytes(payload), status=200)

    with pytest.raises(ValueError, match="encrypted archive member secret/ is not supported"):
        audit.audit_http(source, archive=True)


@responses.activate
def test_audit_zip_rejects_directory_file_namespace_ambiguity():
    source = "https://source.invalid/data.zip"
    responses.add(
        responses.GET,
        source,
        body=zip_bytes({"node/": b"", "node": b"locked"}),
        status=200,
    )

    with pytest.raises(ValueError, match="archive path 'node' is both a directory and a file"):
        audit.audit_http(source, archive=True)


@responses.activate
def test_audit_zip_rejects_raw_directory_separators_before_namespace_normalization():
    source = "https://source.invalid/data.zip"
    responses.add(
        responses.GET,
        source,
        body=zip_bytes({"a//node/": b"", "a/node": b"locked"}),
        status=200,
    )

    with pytest.raises(ValueError, match="archive directory: must be a safe relative POSIX path"):
        audit.audit_http(source, archive=True)


@pytest.mark.parametrize(
    ("members", "file_path", "directory_path"),
    [
        ({"a": b"locked", "a/b/": b""}, "a", "a/b"),
        ({"a/b": b"locked", "a/b/c/d/": b""}, "a/b", "a/b/c/d"),
        (
            {"z": b"locked", "z/x/y/": b"", "a": b"locked", "a/b/": b""},
            "a",
            "a/b",
        ),
    ],
)
@responses.activate
def test_audit_zip_rejects_file_that_is_ancestor_of_structural_directory(
    members: dict[str, bytes],
    file_path: str,
    directory_path: str,
):
    source = "https://source.invalid/data.zip"
    responses.add(responses.GET, source, body=zip_bytes(members), status=200)

    with pytest.raises(
        ValueError,
        match=(f"archive file path {file_path!r} is an ancestor of structural directory {directory_path!r}"),
    ):
        audit.audit_http(source, archive=True)


@responses.activate
def test_audit_zip_accepts_structural_directory_that_is_ancestor_of_file():
    source = "https://source.invalid/data.zip"
    responses.add(
        responses.GET,
        source,
        body=zip_bytes({"a/": b"", "a/b.csv": b"locked"}),
        status=200,
    )

    result = audit.audit_http(source, archive=True)

    assert [output["member_path"] for output in result["outputs"]] == ["a/b.csv"]


@responses.activate
def test_audit_zip_ignores_directory_in_flattened_name_collisions():
    source = "https://source.invalid/data.zip"
    responses.add(
        responses.GET,
        source,
        body=zip_bytes({"a/data.csv/": b"", "b/data.csv": b"locked"}),
        status=200,
    )

    result = audit.audit_http(source, archive=True)

    assert [output["member_path"] for output in result["outputs"]] == ["b/data.csv"]


@responses.activate
def test_audit_zip_rejects_archive_containing_only_directories():
    source = "https://source.invalid/data.zip"
    responses.add(
        responses.GET,
        source,
        body=zip_bytes({"root/": b"", "root/nested/": b""}),
        status=200,
    )

    with pytest.raises(ValueError, match="archive must contain at least one regular file"):
        audit.audit_http(source, archive=True)


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
    monkeypatch.setattr(acquisition, "_MAX_ZIP_MEMBER_BYTES", 1)
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

    monkeypatch.setattr(acquisition.zipfile, "ZipFile", forbidden_zipfile)

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

    monkeypatch.setattr(acquisition.zipfile, "ZipFile", forbidden_zipfile)

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

    monkeypatch.setattr(acquisition.zipfile, "ZipFile", forbidden_zipfile)

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

    monkeypatch.setattr(acquisition.zipfile, "ZipFile", forbidden_zipfile)

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
def test_audit_zip_converts_crc_corruption_to_value_error():
    source = "https://source.invalid/data.zip"
    payload = bytearray(zip_bytes({"data.csv": b"locked"}))
    local_header = payload.index(b"PK\x03\x04")
    filename_size, extra_size = struct.unpack_from("<2H", payload, local_header + 26)
    payload[local_header + 30 + filename_size + extra_size] ^= 0xFF
    responses.add(responses.GET, source, body=bytes(payload), status=200)

    with pytest.raises(ValueError, match="artifact is not a valid ZIP archive"):
        audit.audit_http(source, archive=True)


@responses.activate
def test_audit_rejects_raw_archive_replacement_before_publishing_evidence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    original = zip_bytes({"data.csv": b"first!"})
    replacement = zip_bytes({"data.csv": b"second"})
    source = "https://source.invalid/data.zip"
    responses.add(responses.GET, source, body=original, status=200)

    real_archive_outputs = audit._archive_outputs

    def replacing_archive_outputs(raw_path: Path, temporary_root: Path):
        outputs = real_archive_outputs(raw_path, temporary_root)
        raw_path.write_bytes(replacement)
        return outputs

    monkeypatch.setattr(audit, "_archive_outputs", replacing_archive_outputs)

    with pytest.raises(ValueError, match="archive changed during audit"):
        audit.audit_http(source, archive=True)


@responses.activate
def test_audit_rejects_early_extracted_output_replacement_before_success(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    source = "https://source.invalid/data.zip"
    responses.add(
        responses.GET,
        source,
        body=zip_bytes({"a.csv": b"a", "b.csv": b"b"}),
        status=200,
    )
    real_bound_metadata = audit._bound_extracted_metadata

    def replacing_metadata(paths: list[Path]):
        first = paths[0]
        first.unlink()
        first.write_bytes(b"foreign")
        return real_bound_metadata(paths)

    monkeypatch.setattr(audit, "_bound_extracted_metadata", replacing_metadata)

    with pytest.raises(ValueError, match="extracted output changed"):
        audit.audit_http(source, archive=True)


def test_audit_pairs_bound_metadata_with_canonical_entries_during_public_list_mutation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    archive = tmp_path / "data.zip"
    archive.write_bytes(zip_bytes({"a.csv": b"a", "b.csv": b"bb"}))
    real_validated = audit.validated_zip_members
    real_bound = audit._bound_extracted_metadata
    observed: dict[str, list[acquisition.ArchiveEntry]] = {}

    def capture_entries(path: Path, limits: acquisition.ZipLimits):
        entries = real_validated(path, limits)
        observed["entries"] = entries
        return entries

    def mutate_during_hash(paths: list[Path]):
        metadata = real_bound(paths)
        list.__setitem__(
            observed["entries"],
            0,
            acquisition.ArchiveEntry("b.csv", "renamed.csv", 999),
        )
        return metadata

    monkeypatch.setattr(audit, "validated_zip_members", capture_entries)
    monkeypatch.setattr(audit, "_bound_extracted_metadata", mutate_during_hash)

    outputs = audit._archive_outputs(archive, tmp_path)

    assert outputs == [
        {
            "object_name": "a.csv",
            "member_path": "a.csv",
            "size_bytes": 1,
            "sha256": hashlib.sha256(b"a").hexdigest(),
        },
        {
            "object_name": "b.csv",
            "member_path": "b.csv",
            "size_bytes": 2,
            "sha256": hashlib.sha256(b"bb").hexdigest(),
        },
    ]


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

    assert audit.main(["http", "--url", "https://source.invalid/data.csv", "--output", str(registry)]) == 2

    assert registry.read_bytes() == before


@responses.activate
def test_cli_rejects_hard_link_to_registry_without_fetching(tmp_path: Path):
    registry = ROOT / "datasets" / "registry.yaml"
    before = registry.read_bytes()
    output = tmp_path / "registry-alias.yaml"
    output.hardlink_to(registry)

    assert audit.main(["http", "--url", "https://source.invalid/data.csv", "--output", str(output)]) == 2

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
