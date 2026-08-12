from __future__ import annotations

import socket
import stat
import zipfile
from pathlib import Path

import pytest

from datasets import acquisition
from datasets.acquisition import ZipLimits, download_bounded, extract_members, validated_zip_members


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
            "timeout": pytest.approx(120),
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
