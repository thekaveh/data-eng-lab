from __future__ import annotations

import copy
import dataclasses
import os
import pickle
import socket
import stat
import struct
import time
import zipfile
from pathlib import Path

import pytest

from datasets import acquisition
from datasets.acquisition import ZipLimits, download_bounded, extract_members, validated_zip_members


def _stalled_dns_worker(connection: object) -> None:
    time.sleep(2)
    connection.close()  # type: ignore[attr-defined]


def _static_dns_worker(connection: object) -> None:
    connection.send((True, [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.0.0.9", 443))]))  # type: ignore[attr-defined]
    connection.close()  # type: ignore[attr-defined]


@pytest.fixture(autouse=True)
def inline_dns_resolver(monkeypatch: pytest.MonkeyPatch):
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

    monkeypatch.setattr(acquisition, "_make_resolver_process", process_factory)


def _zip(path: Path, members: dict[str, bytes]) -> Path:
    with zipfile.ZipFile(path, "w") as archive:
        for name, payload in members.items():
            archive.writestr(name, payload)
    return path


class FakeResponse:
    status = 200

    def __init__(
        self,
        body: bytes = b"locked",
        *,
        peer: str = "192.0.0.9",
        headers: dict[str, str] | None = None,
    ) -> None:
        self.headers = headers or {}
        self.peer_address = peer
        self._chunks = iter((body, b""))
        self.timeouts: list[float] = []

    def read1(self, amount: int, *, decode_content: bool) -> bytes:
        assert amount == 1 << 20
        assert decode_content is False
        return next(self._chunks)

    def settimeout(self, timeout: float) -> None:
        self.timeouts.append(timeout)

    def close(self) -> None:
        pass


class FakeTransport:
    def __init__(self, responses: list[FakeResponse] | None = None) -> None:
        self.responses = iter(responses or [FakeResponse()])
        self.requests: list[dict[str, object]] = []
        self.trust_env = False

    def request(self, **kwargs: object) -> FakeResponse:
        self.requests.append(kwargs)
        return next(self.responses)


def _public_dns(monkeypatch: pytest.MonkeyPatch, *addresses: str) -> None:
    monkeypatch.setattr(
        acquisition.socket,
        "getaddrinfo",
        lambda *args, **kwargs: [
            (socket.AF_INET6 if ":" in address else socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, 443))
            for address in addresses
        ],
    )


def test_download_pins_public_dns_and_rejects_private_peer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _public_dns(monkeypatch, "192.0.0.9")
    transport = FakeTransport([FakeResponse(peer="127.0.0.1")])

    with pytest.raises(ValueError, match="connected peer"):
        download_bounded("https://example.test/file", tmp_path / "target", 10, transport=transport)

    assert not (tmp_path / "target").exists()


def test_download_requires_every_a_and_aaaa_answer_to_be_public(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _public_dns(monkeypatch, "192.0.0.9", "127.0.0.1")
    transport = FakeTransport()

    with pytest.raises(ValueError, match="non-public address"):
        download_bounded("https://example.test/file", tmp_path / "target", 10, transport=transport)

    assert transport.requests == []


def test_download_pins_address_while_preserving_tls_and_http_host(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _public_dns(monkeypatch, "192.0.0.9")
    transport = FakeTransport()

    result = download_bounded("https://example.test/file?token=secret", tmp_path / "target", 10, transport=transport)

    assert result.path.read_bytes() == b"locked"
    assert transport.trust_env is False
    assert transport.requests == [
        {
            "url": "https://example.test/file?token=secret",
            "address": "192.0.0.9",
            "server_hostname": "example.test",
            "host_header": "example.test",
            "headers": {"Accept-Encoding": "identity"},
            "timeout": pytest.approx(120, abs=1),
        }
    ]


def test_download_rejects_transport_that_inherits_proxy_configuration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _public_dns(monkeypatch, "192.0.0.9")
    transport = FakeTransport()
    transport.trust_env = True

    with pytest.raises(ValueError, match="must not inherit proxy"):
        download_bounded("https://example.test/file", tmp_path / "target", 10, transport=transport)

    assert not (tmp_path / "target").exists()


def test_download_redacts_query_when_transport_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _public_dns(monkeypatch, "192.0.0.9")

    class FailingTransport(FakeTransport):
        def request(self, **kwargs: object) -> FakeResponse:
            raise OSError("failure exposed token=secret")

    with pytest.raises(ValueError, match=r"file\?<redacted>") as error:
        download_bounded(
            "https://example.test/file?token=secret",
            tmp_path / "target",
            10,
            transport=FailingTransport(),
        )

    assert "secret" not in str(error.value)
    assert not (tmp_path / "target").exists()


def test_download_cleans_only_its_partial_destination_on_size_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _public_dns(monkeypatch, "192.0.0.9")
    sibling = tmp_path / "keep"
    sibling.write_bytes(b"caller owned")

    with pytest.raises(ValueError, match="download exceeds 5 bytes"):
        download_bounded(
            "https://example.test/file",
            tmp_path / "target",
            5,
            transport=FakeTransport([FakeResponse(body=b"123456")]),
        )

    assert not (tmp_path / "target").exists()
    assert sibling.read_bytes() == b"caller owned"


def test_download_does_not_unlink_replacement_after_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _public_dns(monkeypatch, "192.0.0.9")
    destination = tmp_path / "target"
    replacement = tmp_path / "replacement"
    replacement.write_bytes(b"replacement")

    class ReplacingResponse(FakeResponse):
        def read1(self, amount: int, *, decode_content: bool) -> bytes:
            replacement.replace(destination)
            return b"too large"

    with pytest.raises(ValueError, match="download exceeds 1 bytes"):
        download_bounded(
            "https://example.test/file",
            destination,
            1,
            transport=FakeTransport([ReplacingResponse()]),
        )

    assert destination.read_bytes() == b"replacement"


def test_download_deadline_bounds_dns_resolution(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    def process_factory(context: object, send: object, host: str):
        return context.Process(target=_stalled_dns_worker, args=(send,), daemon=True)  # type: ignore[attr-defined]

    monkeypatch.setattr(acquisition, "_make_resolver_process", process_factory)
    started = time.monotonic()

    with pytest.raises(ValueError, match="download deadline exceeded"):
        download_bounded(
            "https://example.test/file",
            tmp_path / "target",
            10,
            deadline_seconds=0.05,
            transport=FakeTransport(),
        )

    assert time.monotonic() - started < 1
    assert not (tmp_path / "target").exists()


def test_dns_resolver_uses_spawn_context_and_closes_process_handle(monkeypatch: pytest.MonkeyPatch):
    observed: dict[str, object] = {}

    class FinishedProcess:
        def start(self) -> None:
            observed["started"] = True

        def is_alive(self) -> bool:
            return False

        def join(self, timeout: float = 0) -> None:
            observed.setdefault("joins", []).append(timeout)  # type: ignore[union-attr]

        def close(self) -> None:
            observed["closed"] = True

    def process_factory(context: object, send: object, host: str):
        observed["method"] = context.get_start_method()  # type: ignore[attr-defined]
        send.send((True, [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.0.0.9", 443))]))
        return FinishedProcess()

    monkeypatch.setattr(acquisition, "_make_resolver_process", process_factory)

    answers = acquisition._bounded_dns_answers("example.test", time.monotonic() + 1, "https://example.test")

    assert answers[0][-1] == ("192.0.0.9", 443)
    assert observed == {"method": "spawn", "started": True, "joins": [0], "closed": True}


def test_dns_resolver_start_failure_does_not_join_unstarted_process(monkeypatch: pytest.MonkeyPatch):
    calls: list[str] = []

    class StartFailureProcess:
        def start(self) -> None:
            calls.append("start")
            raise RuntimeError("spawn failed")

        def is_alive(self) -> bool:
            calls.append("is_alive")
            raise AssertionError("unstarted process inspected")

        def join(self, timeout: float = 0) -> None:
            calls.append("join")
            raise AssertionError("unstarted process joined")

        def close(self) -> None:
            calls.append("close")

    monkeypatch.setattr(acquisition, "_make_resolver_process", lambda *args: StartFailureProcess())

    with pytest.raises(ValueError, match="could not start DNS resolver") as error:
        acquisition._bounded_dns_answers("example.test", time.monotonic() + 1, "https://example.test")

    assert isinstance(error.value.__cause__, RuntimeError)
    assert calls == ["start", "close"]


def test_dns_resolver_spawn_process_completes_without_handle_leak(monkeypatch: pytest.MonkeyPatch):
    before = {process.pid for process in acquisition.multiprocessing.active_children()}

    def process_factory(context: object, send: object, host: str):
        return context.Process(target=_static_dns_worker, args=(send,), daemon=True)  # type: ignore[attr-defined]

    monkeypatch.setattr(acquisition, "_make_resolver_process", process_factory)

    answers = acquisition._bounded_dns_answers("example.test", time.monotonic() + 3, "https://example.test")

    assert answers[0][-1] == ("192.0.0.9", 443)
    assert {process.pid for process in acquisition.multiprocessing.active_children()} == before


def test_download_rejects_path_replacement_before_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _public_dns(monkeypatch, "192.0.0.9")
    destination = tmp_path / "target"
    replacement = tmp_path / "replacement"
    replacement.write_bytes(b"replacement")

    class ReplacingSuccessResponse(FakeResponse):
        def read1(self, amount: int, *, decode_content: bool) -> bytes:
            chunk = super().read1(amount, decode_content=decode_content)
            if not chunk:
                replacement.replace(destination)
            return chunk

    with pytest.raises(ValueError, match="destination changed during download"):
        download_bounded(
            "https://example.test/file",
            destination,
            10,
            transport=FakeTransport([ReplacingSuccessResponse()]),
        )

    assert destination.read_bytes() == b"replacement"


def test_bound_download_metadata_rejects_replacement_and_same_inode_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _public_dns(monkeypatch, "192.0.0.9")
    destination = tmp_path / "target"
    downloaded = download_bounded(
        "https://example.test/file",
        destination,
        10,
        transport=FakeTransport(),
    )
    destination.write_bytes(b"mutate")

    with pytest.raises(ValueError, match="download destination changed"):
        acquisition.bound_download_metadata(downloaded)

    destination.unlink()
    destination.write_bytes(b"locked")
    with pytest.raises(ValueError, match="download destination changed"):
        acquisition.bound_download_metadata(downloaded)


def test_download_revalidates_redirect_dns_and_redacts_query_from_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    answers = {
        "example.test": "192.0.0.9",
        "redirect.test": "127.0.0.1",
    }
    monkeypatch.setattr(
        acquisition.socket,
        "getaddrinfo",
        lambda host, *args, **kwargs: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (answers[host], 443))],
    )
    transport = FakeTransport(
        [FakeResponse(peer="192.0.0.9", headers={"Location": "https://redirect.test/file?token=secret"})]
    )
    transport.responses = iter(
        [
            type(
                "Redirect",
                (FakeResponse,),
                {"status": 302},
            )(peer="192.0.0.9", headers={"Location": "https://redirect.test/file?token=secret"})
        ]
    )

    with pytest.raises(ValueError, match="non-public address") as error:
        download_bounded("https://example.test/file", tmp_path / "target", 10, transport=transport)

    assert "secret" not in str(error.value)
    assert len(transport.requests) == 1


def test_download_uses_exclusive_destination_and_never_removes_unowned_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _public_dns(monkeypatch, "192.0.0.9")
    destination = tmp_path / "target"
    destination.write_bytes(b"owned by caller")

    with pytest.raises(FileExistsError):
        download_bounded("https://example.test/file", destination, 10, transport=FakeTransport())

    assert destination.read_bytes() == b"owned by caller"


def test_validated_zip_members_preserve_paths_and_exclude_structural_directories(tmp_path: Path):
    archive = _zip(
        tmp_path / "data.zip",
        {"root/": b"", "root/data.csv": b"a", "other.txt": b"other"},
    )

    assert validated_zip_members(archive, ZipLimits()) == [
        acquisition.ArchiveEntry("root/data.csv", "data.csv", 1),
        acquisition.ArchiveEntry("other.txt", "other.txt", 5),
    ]


@pytest.mark.parametrize("name", ["../escape.csv", "/absolute.csv", "a\\data.csv"])
def test_validated_zip_members_reject_unsafe_paths(tmp_path: Path, name: str):
    archive = _zip(tmp_path / "data.zip", {name: b"unsafe"})

    with pytest.raises(ValueError, match="safe relative POSIX path"):
        validated_zip_members(archive, ZipLimits())


def test_validated_zip_members_reject_symlink_and_flattened_collision(tmp_path: Path):
    archive = tmp_path / "data.zip"
    with zipfile.ZipFile(archive, "w") as stream:
        symlink = zipfile.ZipInfo("link.csv")
        symlink.create_system = 3
        symlink.external_attr = (stat.S_IFLNK | 0o777) << 16
        stream.writestr(symlink, "target.csv")

    with pytest.raises(ValueError, match="symlink"):
        validated_zip_members(archive, ZipLimits())

    _zip(archive, {"a/data.csv": b"a", "b/data.csv": b"b"})
    with pytest.raises(ValueError, match="flatten to duplicate object name"):
        validated_zip_members(archive, ZipLimits())


def test_extract_members_uses_exclusive_owned_paths(tmp_path: Path):
    archive = _zip(tmp_path / "data.zip", {"root/data.csv": b"locked"})
    entries = validated_zip_members(archive, ZipLimits())
    destination = tmp_path / "members"
    destination.mkdir()
    (destination / "data.csv").write_bytes(b"owned by caller")

    with pytest.raises(FileExistsError):
        extract_members(archive, entries, destination)

    assert (destination / "data.csv").read_bytes() == b"owned by caller"


def test_extract_members_rejects_archive_replacement_after_validation(tmp_path: Path):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"first!"})
    entries = validated_zip_members(archive, ZipLimits())
    replacement = _zip(tmp_path / "replacement.zip", {"data.csv": b"second"})
    replacement.replace(archive)

    with pytest.raises(ValueError, match="changed after validation"):
        extract_members(archive, entries, tmp_path / "members")

    assert not (tmp_path / "members").exists()


def test_extract_members_rejects_same_inode_archive_mutation_after_validation(tmp_path: Path):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"first!"})
    entries = validated_zip_members(archive, ZipLimits())
    replacement_bytes = _zip(tmp_path / "replacement.zip", {"data.csv": b"second"}).read_bytes()
    archive.write_bytes(replacement_bytes)

    with pytest.raises(ValueError, match="changed after validation"):
        extract_members(archive, entries, tmp_path / "members")

    assert not (tmp_path / "members").exists()


def test_extract_members_rejects_requested_subset_of_validated_namespace(tmp_path: Path):
    archive = _zip(tmp_path / "data.zip", {"a.csv": b"a", "b.csv": b"b"})
    entries = validated_zip_members(archive, ZipLimits())

    with pytest.raises(ValueError, match="members changed after validation"):
        extract_members(archive, entries[:1], tmp_path / "members")

    assert not (tmp_path / "members").exists()


def test_extract_members_translates_crc_error_and_cleans_owned_destination(tmp_path: Path):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    payload = bytearray(archive.read_bytes())
    local_header = payload.index(b"PK\x03\x04")
    filename_size, extra_size = struct.unpack_from("<2H", payload, local_header + 26)
    payload[local_header + 30 + filename_size + extra_size] ^= 0xFF
    archive.write_bytes(payload)
    entries = validated_zip_members(archive, ZipLimits())

    with pytest.raises(ValueError, match="artifact is not a valid ZIP archive"):
        extract_members(archive, entries, tmp_path / "members")

    assert not (tmp_path / "members").exists()


def test_extract_members_does_not_follow_replaced_destination_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    entries = validated_zip_members(archive, ZipLimits())
    destination = tmp_path / "members"
    attacker = tmp_path / "attacker"
    attacker.mkdir()
    real_publish = acquisition._publish_directory_exclusive

    def replacing_publish(source: Path, target: Path):
        destination.symlink_to(attacker, target_is_directory=True)
        return real_publish(source, target)

    monkeypatch.setattr(acquisition, "_publish_directory_exclusive", replacing_publish)

    with pytest.raises(ValueError, match="destination changed during extraction"):
        extract_members(archive, entries, destination)

    assert destination.is_symlink()
    assert list(attacker.iterdir()) == []


def test_extract_members_does_not_delete_replacement_output_during_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    entries = validated_zip_members(archive, ZipLimits())
    destination = tmp_path / "members"
    replacement = tmp_path / "replacement"
    replacement.write_bytes(b"attacker owned")
    real_fdopen = acquisition.os.fdopen
    destination_visible_during_write = False

    class ReplacingTarget:
        def __init__(self, target: object) -> None:
            self.target = target

        def __enter__(self):
            return self

        def __exit__(self, *args: object):
            return self.target.__exit__(*args)

        def write(self, payload: bytes) -> int:
            nonlocal destination_visible_during_write
            written = self.target.write(payload)
            destination_visible_during_write = destination.exists()
            return written

    def replacing_fdopen(descriptor: int, mode: str):
        target = real_fdopen(descriptor, mode)
        if mode == "wb":
            target.__enter__()
            return ReplacingTarget(target)
        return target

    monkeypatch.setattr(acquisition.os, "fdopen", replacing_fdopen)

    paths = extract_members(archive, entries, destination)

    assert not destination_visible_during_write
    assert paths[0].read_bytes() == b"locked"
    assert replacement.read_bytes() == b"attacker owned"


def test_archive_entry_public_constructor_remains_three_fields():
    assert acquisition.ArchiveEntry("a/data.csv", "data.csv", 1) == acquisition.ArchiveEntry(
        member_path="a/data.csv",
        object_name="data.csv",
        size_bytes=1,
    )


@pytest.mark.parametrize(
    "value",
    [
        acquisition.ArchiveEntry("a/data.csv", "data.csv", 1),
        acquisition.DownloadedFile(Path("download"), acquisition.ResponseEvidence(etag='"locked"')),
        acquisition.ResponseEvidence(etag='"locked"', last_modified="Mon, 01 Jan 2024 00:00:00 GMT"),
    ],
)
def test_public_dataclasses_have_exact_serialization_fields(value: object):
    expected_fields = {
        acquisition.ArchiveEntry: ["member_path", "object_name", "size_bytes"],
        acquisition.DownloadedFile: ["path", "evidence"],
        acquisition.ResponseEvidence: ["etag", "last_modified"],
    }[type(value)]

    assert [field.name for field in dataclasses.fields(value)] == expected_fields
    assert list(dataclasses.asdict(value)) == expected_fields
    assert copy.copy(value) == value
    assert copy.deepcopy(value) == value
    assert pickle.loads(pickle.dumps(value)) == value
    assert hash(pickle.loads(pickle.dumps(value))) == hash(value)


def test_archive_snapshot_rejects_raw_file_over_explicit_bound(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    archive = tmp_path / "oversized.zip"
    archive.touch()
    os.truncate(archive, 11)
    monkeypatch.setattr(acquisition, "_MAX_ARCHIVE_SNAPSHOT_BYTES", 10)

    with pytest.raises(ValueError, match="archive exceeds 10 bytes"):
        acquisition.preflight_zip(archive, ZipLimits())


def test_archive_disappearance_is_stable_value_error(tmp_path: Path):
    missing = tmp_path / "missing.zip"

    with pytest.raises(ValueError, match="archive is unavailable"):
        acquisition.preflight_zip(missing, ZipLimits())


def test_secure_extraction_has_explicit_supported_platform_guard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    entries = validated_zip_members(archive, ZipLimits())
    monkeypatch.setattr(acquisition, "_SECURE_EXTRACTION_SUPPORTED", False)

    with pytest.raises(RuntimeError, match="secure extraction is not supported"):
        extract_members(archive, entries, tmp_path / "members")

    assert not (tmp_path / "members").exists()


def test_download_requires_owned_non_writable_trusted_parent(tmp_path: Path):
    untrusted = tmp_path / "untrusted"
    untrusted.mkdir(mode=0o777)
    untrusted.chmod(0o777)

    with pytest.raises(ValueError, match="not group/world writable"):
        download_bounded("https://example.test/file", untrusted / "target", 10, transport=FakeTransport())


def test_shared_zip_policy_enforces_member_and_total_expanded_limits(tmp_path: Path):
    archive = _zip(tmp_path / "data.zip", {"a.csv": b"aa", "b.csv": b"bb"})

    with pytest.raises(ValueError, match="member a.csv exceeds 1 bytes"):
        validated_zip_members(archive, ZipLimits(max_member_bytes=1))
    with pytest.raises(ValueError, match="archive exceeds 3 uncompressed bytes"):
        validated_zip_members(archive, ZipLimits(max_total_expanded_bytes=3))


def test_shared_zip_policy_rejects_duplicate_exact_member_path(tmp_path: Path):
    archive = tmp_path / "data.zip"
    with pytest.warns(UserWarning, match="Duplicate name"):
        with zipfile.ZipFile(archive, "w") as stream:
            stream.writestr("data.csv", b"first")
            stream.writestr("data.csv", b"second")

    with pytest.raises(ValueError, match="duplicate member path"):
        validated_zip_members(archive, ZipLimits())
